import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ✅ Speed: resolve chromedriver path ONCE (restart will reuse it)
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()

    # ✅ Faster: don't wait for full page assets
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    # ✅ small extra speed/consistency flags (no extractor change)
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )
    driver.set_page_load_timeout(40)

    # ---- COOKIE LOGIC ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue

            driver.refresh()
            time.sleep(1)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:120]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
# ❌ NOT CHANGED (as you asked)
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

def scrape_with_retry(driver, url, label=""):
    """Retry once if empty. Returns list, [] or 'RESTART'."""
    if label:
        log(f"   🌐 {label} visiting...")
    else:
        log("   🌐 visiting...")

    values = scrape_tradingview(driver, url)
    if values == []:
        log(f"   ⚠️ {label} got empty values, refreshing once...")
        try:
            driver.refresh()
            time.sleep(0.7)
        except:
            pass
        values = scrape_tradingview(driver, url)
    return values

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")

    # ✅ TARGET: MV2 for SQL -> Sheet16
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet16")

    name_list = sheet_main.col_values(1)      # Names
    url_list_c = sheet_main.col_values(3)     # Column C links
    url_list_d = sheet_main.col_values(4)     # Column D links

    total_rows = max(len(name_list), len(url_list_c), len(url_list_d))
    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {total_rows}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()

# Buffer of cell updates
batch_list = []

# ✅ Bigger batch = fewer API calls
BATCH_SIZE = 300

# ✅ Date once
current_date = date.today().strftime("%m/%d/%Y")

ROW_SLEEP = 0.05

# ✅ Accurate counters for “show everything”
rows_buffered = 0
total_rows_processed = 0
total_flushes = 0

def flush_batch(reason=""):
    """Flush buffered cell updates to Google Sheets (batch_update)."""
    global batch_list, rows_buffered, total_flushes
    if not batch_list:
        log("📭 Flush skipped (buffer empty)")
        return

    log(f"🚚 FLUSH START {('('+reason+')') if reason else ''} | "
        f"Updates={len(batch_list)} | RowsBuffered={rows_buffered}")

    for attempt in range(1, 4):
        try:
            sheet_data.batch_update(batch_list)
            total_flushes += 1
            log(f"🚀 FLUSH OK | Saved {len(batch_list)} updates | "
                f"RowsBuffered={rows_buffered} | FlushCount={total_flushes}")
            batch_list = []
            rows_buffered = 0
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ FLUSH ERROR (attempt {attempt}/3): {msg[:200]}")
            if "429" in msg:
                log("⏳ Quota hit, sleeping 60s...")
                time.sleep(60)
            else:
                time.sleep(3)

    log("🛑 FLUSH FAILED after 3 attempts (buffer retained, will retry later)")

def safe_get(lst, idx):
    return (lst[idx] if idx < len(lst) else "").strip()

def log_buffer_state(extra=""):
    updates = len(batch_list)
    remaining = max(BATCH_SIZE - updates, 0)
    msg = (f"📦 BUFFER STATE | Updates={updates}/{BATCH_SIZE} | "
           f"RowsBuffered={rows_buffered} | RemainingToFlush={remaining}")
    if extra:
        msg += f" | {extra}"
    log(msg)

try:
    for i in range(last_i, total_rows):

        # ✅ Sharding
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        total_rows_processed += 1

        name = safe_get(name_list, i) or f"Row {i+1}"
        url_c = safe_get(url_list_c, i)   # Column C
        url_d = safe_get(url_list_d, i)   # Column D
        target_row = i + 1

        log("")
        log("====================================================")
        log(f"🔍 ROW START | Index={target_row}/{total_rows} | Name={name} | "
            f"Shard={SHARD_INDEX}/{SHARD_STEP} | CheckpointFrom={last_i}")
        log(f"🔗 Links | C='{url_c[:90]}' | D='{url_d[:90]}'")

        # ---------- SCRAPE COLUMN C ----------
        values_c = []
        if url_c.startswith("http"):
            values_c = scrape_with_retry(driver, url_c, label="C link")
            if values_c == "RESTART":
                log("🧯 RESTART needed (during C). Rebuilding browser...")
                try:
                    driver.quit()
                except:
                    pass
                driver = create_driver()
                values_c = scrape_with_retry(driver, url_c, label="C link (after restart)")
                if values_c == "RESTART":
                    log("🛑 C still failing after restart, treating as empty.")
                    values_c = []
        else:
            log("   ⏭️ C link invalid/blank -> skipped")

        # ---------- SCRAPE COLUMN D ----------
        values_d = []
        if url_d.startswith("http"):
            values_d = scrape_with_retry(driver, url_d, label="D link")
            if values_d == "RESTART":
                log("🧯 RESTART needed (during D). Rebuilding browser...")
                try:
                    driver.quit()
                except:
                    pass
                driver = create_driver()
                values_d = scrape_with_retry(driver, url_d, label="D link (after restart)")
                if values_d == "RESTART":
                    log("🛑 D still failing after restart, treating as empty.")
                    values_d = []
        else:
            log("   ⏭️ D link invalid/blank -> skipped")

        # ---------- COMBINE ----------
        combined_values = []
        if isinstance(values_c, list) and values_c:
            combined_values.extend(values_c)
        if isinstance(values_d, list) and values_d:
            combined_values.extend(values_d)

        log(f"📌 SCRAPE RESULT | C={len(values_c) if isinstance(values_c, list) else 0} "
            f"| D={len(values_d) if isinstance(values_d, list) else 0} "
            f"| Combined={len(combined_values)}")

        # ---------- WRITE BUFFER ----------
        # Always write Name + Date
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})

        if combined_values:
            batch_list.append({"range": f"K{target_row}", "values": [combined_values]})
            log(f"📝 BUFFER APPEND | A{target_row}, J{target_row}, K{target_row}.. "
                f"| AddedUpdates=3")
        else:
            log(f"📝 BUFFER APPEND | A{target_row}, J{target_row} "
                f"| AddedUpdates=2 (no combined values)")

        rows_buffered += 1

        # show buffer state every row
        log_buffer_state(extra=f"After row {target_row}")

        # ---------- FLUSH WHEN FULL ----------
        if len(batch_list) >= BATCH_SIZE:
            flush_batch(reason="BATCH_SIZE reached")
            log_buffer_state(extra="After flush")

        # ---------- CHECKPOINT ----------
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        log(f"💾 CHECKPOINT saved -> {i+1} (file: {checkpoint_file})")

        log(f"✅ ROW END | ProcessedInThisRun={total_rows_processed} | FlushCount={total_flushes}")
        log("====================================================")

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    # Final flush
    flush_batch(reason="finalize")
    log_buffer_state(extra="After final flush")

    try:
        driver.quit()
    except:
        pass

    log(f"🏁 DONE | TotalProcessed={total_rows_processed} | TotalFlushes={total_flushes}")
