import sys
import os
import time
import json
import random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import gspread
from gspread.exceptions import APIError
from webdriver_manager.chrome import ChromeDriverManager


def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "102"))

START_ROW = SHARD_INDEX * SHARD_SIZE + 1   # 1-based sheet row
END_ROW = START_ROW + SHARD_SIZE - 1

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 20
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 15

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()
CREDENTIALS_FILE = "credentials.json"

# -------- SOURCE / DESTINATION -------- #
# READ FROM HERE
SOURCE_SPREADSHEET_NAME = "Stock List"
SOURCE_WORKSHEET_NAME = "Sheet1"

# WRITE HERE
DEST_SPREADSHEET_ID = "1NYqFa7KEyHCLivd86RJNT9cZN0SIZeARgEH6BgW25yk"
DEST_WORKSHEET_NAME = "Sheet1"

# Writing starts from first column
WRITE_NAME_COL = "A"
WRITE_DATE_COL = "B"
WRITE_VALUE_START_COL = "C"

# Startup jitter
startup_delay = SHARD_INDEX * 8 + random.uniform(3, 8)
log(f"⏳ Startup stagger delay: {startup_delay:.1f}s")
time.sleep(startup_delay)

if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            saved = int(f.read().strip())
        last_row = max(saved, START_ROW)
    except:
        last_row = START_ROW
else:
    last_row = START_ROW


# ---------------- DRIVER ---------------- #
driver = None


def create_driver():
    log(f"🌐 [DAY] [Shard {SHARD_INDEX}] Range {START_ROW}-{END_ROW} | Initializing browser...")
    opts = Options()
    opts.page_load_strategy = "normal"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            time.sleep(2)

            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    drv.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue

            drv.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {repr(e)[:120]}")

    return drv


def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver


def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Closing browser...")
            driver.quit()
    except Exception as e:
        log(f"⚠️ Browser close issue: {repr(e)[:120]}")
    driver = None
    time.sleep(3)


# ---------------- HELPERS ---------------- #
def wait_for_page_ready(drv, timeout=25):
    WebDriverWait(drv, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"]
    )


def get_visible_value_elements(drv):
    elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    values = []

    for el in elems:
        try:
            if not el.is_displayed():
                continue
            txt = el.text.strip()
            if txt:
                values.append(txt)
        except:
            pass

    return values


def stable_read_values(drv, pause=1.2):
    first = get_visible_value_elements(drv)
    time.sleep(pause)
    second = get_visible_value_elements(drv)

    if first == second and first:
        return first

    return second if len(second) >= len(first) else first


def bs4_fallback_values(drv):
    try:
        soup = BeautifulSoup(drv.page_source, "html.parser")
        raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
        out = []
        for el in raw_values:
            txt = el.get_text(strip=True)
            if txt:
                out.append(txt)
        return out
    except:
        return []


def looks_like_polluted_day(values):
    if not values:
        return True
    joined = " | ".join(values[:8])
    suspicious_tokens = ["%", "K", "M", "B", "∅"]
    suspicious_score = sum(1 for tok in suspicious_tokens if tok in joined)
    return suspicious_score >= 2


def validate_day(values):
    if not values:
        return False
    if len(values) != EXPECTED_COUNT:
        return False
    if looks_like_polluted_day(values):
        return False
    return True


def sleep_with_jitter(base, extra=3):
    time.sleep(base + random.uniform(1, extra))


def is_quota_error(exc):
    text = repr(exc)
    return "429" in text or "Quota exceeded" in text or "Read requests per minute per user" in text


# ---------------- SCRAPER ---------------- #
def scrape_day(url):
    if not url:
        return []

    log(f"   📡 Navigating DAY: {url}")

    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)

            wait_for_page_ready(drv, timeout=25)

            try:
                WebDriverWait(drv, 20).until(
                    lambda d: len(get_visible_value_elements(d)) >= EXPECTED_COUNT
                )
            except TimeoutException:
                log("   ⚠️ DAY initial wait timeout, trying anyway...")

            drv.execute_script("window.scrollTo(0, 300);")
            time.sleep(1)
            drv.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            values = stable_read_values(drv, pause=1.2)

            if not validate_day(values):
                log(f"   ⚠️ DAY invalid data on attempt {attempt+1}: count={len(values)} preview={values[:8]}")
                try:
                    drv.refresh()
                    time.sleep(4)
                    wait_for_page_ready(drv, timeout=20)
                    values = stable_read_values(drv, pause=1.2)
                except Exception as e:
                    log(f"   ⚠️ DAY refresh issue: {repr(e)[:120]}")

            if not validate_day(values):
                values = bs4_fallback_values(drv)

            if validate_day(values):
                log(f"   📊 Found {len(values)} correct DAY values")
                log(f"   📝 DAY Preview: {values[:8]}...")
                return values

            log(f"   ⚠️ DAY invalid data on attempt {attempt+1}. Expected {EXPECTED_COUNT}, got {len(values)}")

        except Exception as e:
            log(f"   ❌ DAY ERROR: {repr(e)[:150]}")
            restart_driver()
            time.sleep(3)

    return []


# ---------------- SHEETS ---------------- #
def get_gc():
    return gspread.service_account(filename=CREDENTIALS_FILE)


def connect_source_sheet(max_retries=8):
    """
    Read source only once.
    """
    for attempt in range(max_retries):
        try:
            log("📊 Connecting to SOURCE Google Sheet...")
            gc = get_gc()
            ws = gc.open(SOURCE_SPREADSHEET_NAME).worksheet(SOURCE_WORKSHEET_NAME)
            return ws

        except Exception as e:
            wait = min(90, (2 ** attempt) + random.uniform(4, 10))
            log(f"⚠️ SOURCE connect retry {attempt+1}: {repr(e)[:180]} | sleeping {wait:.1f}s")
            time.sleep(wait)

    raise Exception("Failed to connect to SOURCE sheet after retries")


def connect_dest_sheet(max_retries=8):
    """
    Writer connection only.
    """
    for attempt in range(max_retries):
        try:
            log("📊 Connecting to DEST Google Sheet...")
            gc = get_gc()
            ws = gc.open_by_key(DEST_SPREADSHEET_ID).worksheet(DEST_WORKSHEET_NAME)
            return ws

        except Exception as e:
            wait = min(90, (2 ** attempt) + random.uniform(4, 10))
            log(f"⚠️ DEST connect retry {attempt+1}: {repr(e)[:180]} | sleeping {wait:.1f}s")
            time.sleep(wait)

    raise Exception("Failed to connect to DEST sheet after retries")


def load_source_rows():
    """
    Read only this shard's rows.
    A = company name
    D = day URL
    """
    ws = connect_source_sheet()

    range_a = f"A{START_ROW}:A{END_ROW}"
    range_d = f"D{START_ROW}:D{END_ROW}"

    for attempt in range(8):
        try:
            log(f"📥 Reading shard source ranges: {range_a} and {range_d}")
            result = ws.batch_get([range_a, range_d])

            col_a = result[0] if len(result) > 0 else []
            col_d = result[1] if len(result) > 1 else []

            names = [row[0].strip() if row else "" for row in col_a]
            urls = [row[0].strip() if row else "" for row in col_d]

            max_len = max(len(names), len(urls), SHARD_SIZE)

            if len(names) < max_len:
                names.extend([""] * (max_len - len(names)))
            if len(urls) < max_len:
                urls.extend([""] * (max_len - len(urls)))

            return names[:SHARD_SIZE], urls[:SHARD_SIZE]

        except Exception as e:
            wait = min(90, (2 ** attempt) + random.uniform(4, 10))
            log(f"⚠️ SOURCE read retry {attempt+1}: {repr(e)[:180]} | sleeping {wait:.1f}s")
            time.sleep(wait)

            if is_quota_error(e):
                continue

            try:
                ws = connect_source_sheet()
            except Exception as inner_e:
                log(f"⚠️ SOURCE reconnect issue: {repr(inner_e)[:150]}")

    raise Exception("Failed to read source rows after retries")


try:
    company_list, url_day_list = load_source_rows()
    sheet_data = connect_dest_sheet()
    log(f"✅ DAY Data Ready. Starting from Row {last_row}")
except Exception as e:
    log(f"❌ Connection Error: {repr(e)}")
    sys.exit(1)


# ---------------- BATCH ---------------- #
batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")
prev_day = None
same_count = 0


def flush_batch():
    global batch_list, buffered_rows, sheet_data

    if not batch_list:
        return True

    log(f"🚀 UPLOADING DAY BATCH: Sending {buffered_rows} rows...")

    for attempt in range(8):
        try:
            sheet_data.batch_update(batch_list, value_input_option="RAW")
            log("✅ DAY batch written successfully.")
            batch_list = []
            buffered_rows = 0
            return True

        except Exception as e:
            wait = min(90, (2 ** attempt) + random.uniform(4, 10))
            log(f"⚠️ DAY write retry {attempt+1}: {repr(e)[:180]} | sleeping {wait:.1f}s")
            time.sleep(wait)

            try:
                sheet_data = connect_dest_sheet()
            except Exception as inner_e:
                log(f"⚠️ DEST reconnect failed: {repr(inner_e)[:150]}")

    return False


# ---------------- MAIN LOOP ---------------- #
try:
    # Convert sheet row to local index inside current shard
    local_start_index = max(0, last_row - START_ROW)
    loop_end = min(SHARD_SIZE, len(company_list))

    for local_i in range(local_start_index, loop_end):
        row_idx = START_ROW + local_i
        name = company_list[local_i].strip() if local_i < len(company_list) else ""

        log(f"--- [ROW {row_idx}] DAY Processing: {name} ---")

        u_day = url_day_list[local_i].strip() if local_i < len(url_day_list) and url_day_list[local_i].startswith("http") else None
        vals_day = scrape_day(u_day) if u_day else []

        if vals_day == prev_day and vals_day:
            same_count += 1
            log(f"   ⚠️ Same DAY data repeated. Count={same_count}")
            if same_count >= 2:
                log("   ♻️ Repeated DAY values detected. Restarting browser and retrying...")
                restart_driver()
                vals_day = scrape_day(u_day) if u_day else []
                same_count = 0
        else:
            same_count = 0

        prev_day = vals_day.copy() if vals_day else []

        batch_list.append({"range": f"{WRITE_NAME_COL}{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"{WRITE_DATE_COL}{row_idx}", "values": [[current_date]]})
        batch_list.append({
            "range": f"{WRITE_VALUE_START_COL}{row_idx}",
            "values": [vals_day] if vals_day else [[]]
        })

        buffered_rows += 1

        if len(vals_day) == EXPECTED_COUNT:
            log(f"   ✅ Correct DAY count: {EXPECTED_COUNT}")
        else:
            log(f"   ⚠️ DAY count mismatch: {len(vals_day)}")

        log(f"   📥 Buffered {len(vals_day)} DAY values starting from {WRITE_VALUE_START_COL}.")
        log(f"📈 DAY Progress: {local_i + 1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(row_idx + 1))

        processed = (local_i - local_start_index + 1)
        if processed % RESTART_EVERY_ROWS == 0:
            log(f"♻️ Periodic browser restart after {processed} rows.")
            restart_driver()

        if buffered_rows >= BATCH_SIZE:
            ok = flush_batch()
            restart_driver()

            if not ok:
                log("❌ DAY batch upload failed.")
                break

        time.sleep(0.5)

finally:
    if batch_list:
        flush_batch()
    restart_driver()
    log("🏁 DAY Shard Completed.")
