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
        saved = int(open(checkpoint_file).read().strip())
        last_i = max(saved, START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing...")
    opts = Options()

    # same site path, just safer loading
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

def looks_wrong(values):
    if not values or len(values) < 8:
        return True

    first = [normalize_text(v) for v in values[:8]]

    # known bad patterns from your logs
    bad_prefixes = [
        ["1", "3", "5"],
        ["1", "70", "70"],
        ["1", "89", "89"],
        ["1", "4", "4"],
        ["1", "19", "19"],
        ["1", "28", "28"],
        ["1", "46", "46"],
        ["1", "50", "50"],
        ["1", "75", "75"],
    ]

    for bp in bad_prefixes:
        if first[:len(bp)] == bp:
            return True

    # too many tiny integers in first few values => suspicious UI data
    small_numeric = 0
    for v in first[:6]:
        t = v.replace(",", "").replace("−", "-").strip()
        try:
            num = float(t)
            if abs(num) <= 10:
                small_numeric += 1
        except:
            pass

    if small_numeric >= 4:
        return True

    return False

def extract_values_fast(driver):
    elems = driver.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    vals = []
    for el in elems:
        try:
            txt = el.text.strip()
            if txt:
                vals.append(normalize_text(txt))
        except:
            pass

    if vals:
        return vals

    soup = BeautifulSoup(driver.page_source, "html.parser")
    raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
    vals = []
    for el in raw_values:
        txt = el.get_text(strip=True)
        if txt:
            vals.append(normalize_text(txt))
    return vals

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    if not url:
        return []

    log(f"   📡 Navigating {url_type}: {url}")

    for attempt in range(3):
        try:
            log(f"   ⏳ {url_type} attempt {attempt + 1}")
            driver.get(url)

            # keep same path, just stronger wait
            try:
                WebDriverWait(driver, 20).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")) >= 8
                )
            except:
                pass

            driver.execute_script("window.scrollTo(0, 250);")
            time.sleep(0.6)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2.5)

            final_values = extract_values_fast(driver)

            if final_values and not looks_wrong(final_values):
                log(f"   📊 Found {len(final_values)} values for {url_type}")
                log(f"   📝 Data Preview: {final_values[:8]}...")
                return final_values

            log(f"   ⚠️ {url_type} wrong/empty values detected on attempt {attempt + 1}")
            if final_values:
                log(f"   📝 Suspicious Preview: {final_values[:8]}...")

            if attempt < 2:
                try:
                    driver.refresh()
                except:
                    pass
                time.sleep(3)

        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:80]}")
            time.sleep(2)

    return []

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)   # SAME
    url_h_list = sheet_main.col_values(8)   # SAME

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
        return False

    log(f"🚀 UPLOADING BATCH: Sending {buffered_rows} rows to Google Sheets...")

    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option="RAW")
            log("✅ SUCCESS: Batch successfully written to Sheet2.")
            batch_list = []
            buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ API Retry {attempt+1}: {str(e)[:120]}")
            time.sleep(8 + attempt * 5)

    return False

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Restarting browser for stability...")
            driver.quit()
    except:
        pass
    driver = None

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
            log("   ⚠️ No valid values found, buffering blank K cell.")

        log(f"📈 Shard Progress: {i+1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if buffered_rows >= BATCH_SIZE:
            ok = flush_batch()
            restart_driver()   # most important fix
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
