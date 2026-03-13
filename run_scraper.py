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
from google.oauth2.service_account import Credentials
from webdriver_manager.chrome import ChromeDriverManager


def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")
CREDENTIALS_FILE = "credentials.json"
COOKIE_FILE = "cookies.json"

EXPECTED_COUNT = 20
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 15

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# Load checkpoint
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW


# ---------------- AUTH & API HELPERS ---------------- #
def get_gc():
    """Returns a stateless gspread client to avoid PermissionError."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    # This method avoids writing local cache files
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def safe_call(func, *args, **kwargs):
    """Retries Google Sheet calls with exponential backoff on Quota or Permission errors."""
    for attempt in range(6):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err = str(e).lower()
            # Catch Quota (429) and local PermissionError
            if "429" in err or "quota" in err or "permission" in err:
                wait_time = (2 ** attempt) * 10 + random.uniform(1, 5)
                log(f"⚠️ API/Permission Issue. Retrying in {wait_time:.1f}s... ({attempt+1}/6)")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("Max retries reached for Google Sheets API.")


def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = get_gc()
    sheet_main = safe_call(gc.open, "Stock List")
    sheet_data = safe_call(gc.open, "MV2 for SQL")
    return sheet_main.worksheet("Sheet1"), sheet_data.worksheet("Sheet2")


# ---------------- BROWSER ---------------- #
driver = None


def create_driver():
    log(f"🌐 [DAY] [Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

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
                    drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            drv.refresh()
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:100]}")
    return drv


def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver


def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Closing browser...")
            driver.quit()
    except: pass
    driver = None
    time.sleep(2)


# ---------------- SCRAPER HELPERS ---------------- #
def get_visible_value_elements(drv):
    elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    return [el.text.strip() for el in elems if el.is_displayed() and el.text.strip()]


def scrape_day(url):
    if not url: return []
    log(f"    📡 Navigating DAY: {url}")
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            WebDriverWait(drv, 20).until(lambda d: len(get_visible_value_elements(d)) >= EXPECTED_COUNT)
            time.sleep(2)
            drv.execute_script("window.scrollTo(0, 300);")
            time.sleep(1.5)
            values = get_visible_value_elements(drv)
            if len(values) >= EXPECTED_COUNT:
                return values[:EXPECTED_COUNT]
        except Exception as e:
            log(f"    ❌ Scrape attempt {attempt+1} error: {str(e)[:80]}")
            restart_driver()
    return []


# ---------------- MAIN LOOP ---------------- #
try:
    ws_main, ws_data = connect_sheets()
    # Minimize read requests by pre-fetching columns
    log("📥 Pre-fetching source data...")
    company_list = safe_call(ws_main.col_values, 1)
    url_day_list = safe_call(ws_main.col_values, 4)
    log(f"✅ Setup Ready. Row {last_i + 1}")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}")
    sys.exit(1)

batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows, ws_data
    if not batch_list: return True
    log(f"🚀 UPLOADING BATCH: {buffered_rows} rows...")
    try:
        safe_call(ws_data.batch_update, batch_list, value_input_option="RAW")
        log("✅ Batch written successfully.")
        batch_list = []
        buffered_rows = 0
        return True
    except Exception as e:
        log(f"❌ Batch upload failed: {e}")
        return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        u_day = url_day_list[i].strip() if i < len(url_day_list) and url_day_list[i].startswith("http") else None
        
        log(f"--- [ROW {i+1}] Processing: {name} ---")
        vals = scrape_day(u_day)
        
        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [vals] if vals else [[]]})
        buffered_rows += 1

        with open(checkpoint_file, "w") as f: f.write(str(i + 1))

        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if buffered_rows >= BATCH_SIZE:
            if not flush_batch(): break
            restart_driver()
            # Refresh connections to keep token fresh
            ws_main, ws_data = connect_sheets()
            company_list = safe_call(ws_main.col_values, 1)
            url_day_list = safe_call(ws_main.col_values, 4)

finally:
    if batch_list: flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
