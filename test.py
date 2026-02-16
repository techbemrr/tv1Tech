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

# =========================
# CONFIG & SHARDING
# =========================
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ✅ Resolve chromedriver path ONCE (fast restarts)
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ✅ Batch size (buffer) = 50 updates (as you asked)
BATCH_SIZE_UPDATES = 50

# ✅ Small optimizations (no main logic change)
CHECKPOINT_EVERY = 10   # write checkpoint every N processed rows
ROW_SLEEP = 0.05

# =========================
# HELPERS: CLEAN + LAST 3
# =========================
def clean_cell_text(s: str) -> str:
    # removes ALL whitespace (spaces, tabs, newlines)
    return "".join(str(s).split())

def clean_list(values):
    return [clean_cell_text(v) for v in values if str(v).strip() != ""]

def last_three(values):
    vals = clean_list(values)
    return vals[-3:] if len(vals) >= 3 else vals

def safe_get(lst, idx):
    return (lst[idx] if idx < len(lst) else "").strip()

# =========================
# BROWSER FACTORY
# =========================
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()

    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

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

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
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

# =========================
# SCRAPER LOGIC (UNCHANGED MAIN XPATH)
# =========================
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

# =========================
# SHEETS SETUP
# =========================
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet16")

    name_list = sheet_main.col_values(1)
    url_list_c = sheet_main.col_values(3)
    url_list_d = sheet_main.col_values(4)

    total_rows = max(len(name_list), len(url_list_c), len(url_list_d))
    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {total_rows}")

    # prevent grid limit crashes
    needed_rows = total_rows + 10
    if sheet_data.row_count < needed_rows:
        log(f"🧱 Resizing Sheet16 rows: {sheet_data.row_count} -> {needed_rows}")
        sheet_data.resize(rows=needed_rows)

except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# =========================
# BUFFER + FLUSH
# =========================
driver = create_driver()
batch_list = []

current_date = date.today().strftime("%m/%d/%Y")

rows_buffered = 0
total_rows_processed = 0
total_flushes = 0
_last_checkpoint_written = last_i

def _clean_ranges(updates):
    # prevent range corruption like: Sheet16!Sheet16!A16
    cleaned = []
    for u in updates:
        r = u.get("range", "")
        if "!" in r:
            r = r.split("!")[-1]  # keep only A1
        cleaned.append({"range": r, "values": u.get("values", [[]])})
    return cleaned

def flush_batch(reason=""):
    global batch_list, rows_buffered, total_flushes
    if not batch_list:
        log("📭 Flush skipped (buffer empty)")
        return

    log(f"🚚 FLUSH START {('('+reason+')') if reason else ''} | Updates={len(batch_list)} | RowsBuffered={rows_buffered}")

    backoffs = [2, 5, 15]
    for attempt in range(1, 4):
        try:
            payload = _clean_ranges(batch_list)
            sheet_data.batch_update(payload)

            total_flushes += 1
            log(f"🚀 FLUSH OK | Saved {len(payload)} updates | RowsBuffered={rows_buffered} | FlushCount={total_flushes}")

            batch_list = []
            rows_buffered = 0
            return

        except Exception as e:
            msg = str(e)
            log(f"⚠️ FLUSH ERROR (attempt {attempt}/3): {msg[:220]}")

            if "exceeds grid limits" in msg.lower():
                try:
                    new_rows = max(sheet_data.row_count + 800, total_rows + 10)
                    log(f"🧱 Auto-resize on grid limit: {sheet_data.row_count} -> {new_rows}")
                    sheet_data.resize(rows=new_rows)
                except Exception as ee:
                    log(f"⚠️ Resize failed: {str(ee)[:150]}")

            if "429" in msg:
                log("⏳ Quota hit, sleeping 60s...")
                time.sleep(60)
            else:
                time.sleep(backoffs[attempt - 1])

    log("🛑 FLUSH FAILED after 3 attempts (buffer retained, will retry later)")

def maybe_checkpoint(i_plus_1, force=False):
    global _last_checkpoint_written
    if force or (i_plus_1 - _last_checkpoint_written) >= CHECKPOINT_EVERY:
        try:
            with open(checkpoint_file, "w") as f:
                f.write(str(i_plus_1))
            _last_checkpoint_written = i_plus_1
            log(f"💾 CHECKPOINT saved -> {i_plus_1} (file: {checkpoint_file})")
        except Exception as e:
            log(f"⚠️ CHECKPOINT write failed: {str(e)[:120]}")

def log_buffer_state(extra=""):
    updates = len(batch_list)
    remaining = max(BATCH_SIZE_UPDATES - updates, 0)
    msg = (f"📦 BUFFER STATE | Updates={updates}/{BATCH_SIZE_UPDATES} | "
           f"RowsBuffered={rows_buffered} | RemainingToFlush={remaining}")
    if extra:
        msg += f" | {extra}"
    log(msg)

try:
    for i in range(last_i, total_rows):

        # sharding
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        total_rows_processed += 1

        name = safe_get(name_list, i) or f"Row {i+1}"
        url_c = safe_get(url_list_c, i)
        url_d = safe_get(url_list_d, i)
        target_row = i + 1

        # safety: ensure row exists
        if target_row > sheet_data.row_count:
            grow_to = target_row + 300
            log(f"🧱 Growing Sheet16 for row {target_row}: {sheet_data.row_count} -> {grow_to}")
            sheet_data.resize(rows=grow_to)

        log("")
        log("====================================================")
        log(f"🔍 ROW START | Index={target_row}/{total_rows} | Name={name} | Shard={SHARD_INDEX}/{SHARD_STEP} | CheckpointFrom={last_i}")
        log(f"🔗 Links | C='{url_c[:90]}' | D='{url_d[:90]}'")

        # ---- Scrape C ----
        values_c = []
        if url_c.startswith("http"):
            values_c = scrape_with_retry(driver, url_c, label="C link")
            if values_c == "RESTART":
                log("🧯 RESTART needed (during C). Rebuilding browser...")
                try: driver.quit()
                except: pass
                driver = create_driver()
                values_c = scrape_with_retry(driver, url_c, label="C link (after restart)")
                if values_c == "RESTART":
                    log("🛑 C still failing after restart, treating as empty.")
                    values_c = []
        else:
            log("   ⏭️ C link invalid/blank -> skipped")

        # ---- Scrape D ----
        values_d = []
        if url_d.startswith("http"):
            values_d = scrape_with_retry(driver, url_d, label="D link")
            if values_d == "RESTART":
                log("🧯 RESTART needed (during D). Rebuilding browser...")
                try: driver.quit()
                except: pass
                driver = create_driver()
                values_d = scrape_with_retry(driver, url_d, label="D link (after restart)")
                if values_d == "RESTART":
                    log("🛑 D still failing after restart, treating as empty.")
                    values_d = []
        else:
            log("   ⏭️ D link invalid/blank -> skipped")

        # ---- Combine: ONLY last 3 of C + last 3 of D, and remove whitespace ----
        c_last3 = last_three(values_c if isinstance(values_c, list) else [])
        d_last3 = last_three(values_d if isinstance(values_d, list) else [])
        combined_values = c_last3 + d_last3

        log(f"📌 SCRAPE RESULT | C={len(values_c) if isinstance(values_c, list) else 0} | D={len(values_d) if isinstance(values_d, list) else 0} | Combined={len(combined_values)}")

        # ---- Buffer updates ----
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})

        if combined_values:
            batch_list.append({"range": f"K{target_row}", "values": [combined_values]})
            log(f"📝 BUFFER APPEND | A{target_row}, J{target_row}, K{target_row}.. | AddedUpdates=3")
        else:
            log(f"📝 BUFFER APPEND | A{target_row}, J{target_row} | AddedUpdates=2 (no combined values)")

        rows_buffered += 1
        log_buffer_state(extra=f"After row {target_row}")

        # flush when buffer full
        if len(batch_list) >= BATCH_SIZE_UPDATES:
            flush_batch(reason="BATCH_SIZE reached")
            log_buffer_state(extra="After flush")

        # checkpoint
        maybe_checkpoint(i + 1, force=False)

        log(f"✅ ROW END | ProcessedInThisRun={total_rows_processed} | FlushCount={total_flushes}")
        log("====================================================")

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush_batch(reason="finalize")
    log_buffer_state(extra="After final flush")
    maybe_checkpoint(_last_checkpoint_written, force=True)

    try:
        driver.quit()
    except:
        pass

    log(f"🏁 DONE | TotalProcessed={total_rows_processed} | TotalFlushes={total_flushes}")
