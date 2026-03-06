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
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG & RANGE CALCULATION ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500"))

START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW   = START_ROW + SHARD_SIZE

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing...")
    opts = Options()

    # Same paths/URLs; just safer and more stable for TradingView
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(60)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)

            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    payload = {
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    }
                    driver.add_cookie(payload)
                except:
                    continue

            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")
    return driver

# ---------------- HELPERS ---------------- #
def normalize_text(x):
    return x.replace("\u202f", " ").replace("\xa0", " ").strip()

def get_visible_values(driver):
    values = []
    elems = driver.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    for el in elems:
        try:
            if el.is_displayed():
                txt = normalize_text(el.text)
                if txt:
                    values.append(txt)
        except:
            pass

    if values:
        return values

    # fallback
    soup = BeautifulSoup(driver.page_source, "html.parser")
    raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
    out = []
    for el in raw_values:
        txt = normalize_text(el.get_text(strip=True))
        if txt:
            out.append(txt)
    return out

def looks_suspicious(values):
    if not values or len(values) < 8:
        return True

    first = values[:8]

    bad_prefixes = [
        ["1", "3", "5"],
        ["1", "89", "89"],
        ["1", "70", "70"],
        ["1", "75", "75"],
        ["1", "50", "50"],
        ["1", "46", "46"],
        ["1", "28", "28"],
        ["1", "19", "19"],
        ["1", "14", "14"],
        ["1", "4", "4"],
    ]
    for bp in bad_prefixes:
        if first[:len(bp)] == bp:
            return True

    # Agar first few values me bohot chhote UI-like numbers hon
    small_count = 0
    for v in first[:6]:
        t = v.replace(",", "").replace("−", "-").strip()
        try:
            num = float(t)
            if abs(num) <= 10:
                small_count += 1
        except:
            pass

    return small_count >= 4

def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Restarting browser...")
            driver.quit()
    except:
        pass
    driver = None

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    if not url:
        return []

    log(f"   📡 Navigating {url_type}: {url}")

    for attempt in range(3):
        try:
            log(f"   ⏳ {url_type} Attempt {attempt + 1}")
            driver.get(url)

            # Wait only for actual values, not just document.readyState
            try:
                WebDriverWait(driver, 20).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")) >= 8
                )
            except:
                pass

            driver.execute_script("window.scrollTo(0, 250);")
            time.sleep(0.7)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2.5)

            final_values = get_visible_values(driver)

            if final_values and not looks_suspicious(final_values):
                log(f"   📊 Found {len(final_values)} values for {url_type}")
                log(f"   📝 Data Preview: {final_values[:8]}...")
                return final_values

            log(f"   ⚠️ {url_type} suspicious/incorrect values detected.")
            if final_values:
                log(f"   📝 Suspicious Preview: {final_values[:8]}...")

            # Retry with refresh once, then fresh browser on last retry
            if attempt == 0:
                try:
                    driver.refresh()
                except:
                    pass
                time.sleep(3)
            elif attempt == 1:
                restart_driver()
                fresh_driver = ensure_driver()
                driver = fresh_driver
                time.sleep(2)

        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:100]}")
            time.sleep(2)

    return []

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    # SAME PATH / SAME COLUMNS
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)

    log(f"✅ Data Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
buffered_rows = 0
BATCH_SIZE = 100
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows
    if not batch_list:
        return True

    log(f"🚀 UPLOADING BATCH: Sending {buffered_rows} rows to Google Sheets...")

    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log("✅ SUCCESS: Batch successfully written to Sheet2.")
            batch_list = []
            buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ API Retry {attempt+1}: {str(e)[:120]}")
            time.sleep(8 + (attempt * 5))

    return False

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

try:
    loop_end = min(END_ROW, len(company_list))

    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] Processing: {name} ---")

        active_driver = ensure_driver()

        u_d = url_d_list[i].strip() if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i].strip() if i < len(url_h_list) and url_h_list[i].startswith("http") else None

        vals_d = scrape_tradingview(active_driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(active_driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [combined] if combined else [[]]})

        buffered_rows += 1

        if combined:
            log(f"   📥 Buffered {len(combined)} values into batch.")
        else:
            log("   ⚠️ No valid values found. Writing blank K cell.")

        log(f"📈 Shard Progress: {i+1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if buffered_rows >= BATCH_SIZE:
            ok = flush_batch()

            # Most important fix: after batch complete, restart browser
            restart_driver()

            if not ok:
                log("❌ Batch upload failed after retries.")
                break

        time.sleep(0.4)

finally:
    if batch_list:
        flush_batch()
    if driver:
        try:
            driver.quit()
        except:
            pass
    log("🏁 Shard Completed.")
