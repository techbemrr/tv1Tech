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
from gspread.exceptions import APIError
from webdriver_manager.chrome import ChromeDriverManager


def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "102"))

START_ROW = SHARD_INDEX * SHARD_SIZE + 1
END_ROW = START_ROW + SHARD_SIZE - 1

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 20
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 15

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()
CREDENTIALS_FILE = "credentials.json"

# SOURCE / DESTINATION
SOURCE_SPREADSHEET_NAME = "Stock List"
SOURCE_WORKSHEET_NAME = "Sheet1"
DEST_SPREADSHEET_ID = "1NYqFa7KEyHCLivd86RJNT9cZN0SIZeARgEH6BgW25yk"
DEST_WORKSHEET_NAME = "Sheet1"

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
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW}-{END_ROW} | Initializing browser...")
    opts = Options()
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
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {repr(e)[:120]}")
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
    except Exception as e:
        log(f"⚠️ Browser close issue: {repr(e)[:120]}")
    driver = None
    time.sleep(3)

# ---------------- SCRAPER HELPERS ---------------- #
def wait_for_page_ready(drv, timeout=25):
    WebDriverWait(drv, timeout).until(lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"])

def get_visible_value_elements(drv):
    elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    values = []
    for el in elems:
        try:
            if el.is_displayed():
                txt = el.text.strip()
                if txt: values.append(txt)
        except: pass
    return values

def stable_read_values(drv, pause=1.2):
    first = get_visible_value_elements(drv)
    time.sleep(pause)
    second = get_visible_value_elements(drv)
    return second if len(second) >= len(first) else first

def bs4_fallback_values(drv):
    try:
        soup = BeautifulSoup(drv.page_source, "html.parser")
        raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
        return [el.get_text(strip=True) for el in raw_values if el.get_text(strip=True)]
    except: return []

def validate_day(values):
    if not values or len(values) != EXPECTED_COUNT: return False
    joined = " | ".join(values[:8])
    suspicious_tokens = ["%", "K", "M", "B", "∅"]
    if sum(1 for tok in suspicious_tokens if tok in joined) >= 2: return False
    return True

def scrape_day(url):
    if not url: return []
    log(f"    📡 Navigating DAY: {url}")
    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)
            wait_for_page_ready(drv, 25)
            try:
                WebDriverWait(drv, 20).until(lambda d: len(get_visible_value_elements(d)) >= EXPECTED_COUNT)
            except: pass
            drv.execute_script("window.scrollTo(0, 300);")
            time.sleep(2)
            values = stable_read_values(drv)
            if not validate_day(values):
                values = bs4_fallback_values(drv)
            if validate_day(values):
                log(f"    📊 Found {len(values)} values. Preview: {values[:5]}")
                return values
            log(f"    ⚠️ Attempt {attempt+1} failed validation.")
        except Exception as e:
            log(f"    ❌ Scrape error: {repr(e)[:100]}")
            restart_driver()
    return []

# ---------------- AUTH & SHEETS ---------------- #
def get_gc():
    # Force credentials to load without creating local cache files
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def connect_source_sheet():
    for attempt in range(5):
        try:
            gc = get_gc()
            return gc.open(SOURCE_SPREADSHEET_NAME).worksheet(SOURCE_WORKSHEET_NAME)
        except Exception as e:
            time.sleep(5)
    raise Exception("Source connection failed")

def connect_dest_sheet():
    for attempt in range(5):
        try:
            gc = get_gc()
            # open_by_key is safer than open_by_name for Destination
            return gc.open_by_key(DEST_SPREADSHEET_ID).worksheet(DEST_WORKSHEET_NAME)
        except Exception as e:
            log(f"⚠️ DEST retry {attempt+1}: {repr(e)[:100]}")
            time.sleep(10)
    raise Exception("Destination connection failed")

# ---------------- MAIN ---------------- #
try:
    ws_source = connect_source_sheet()
    log("📥 Reading source data...")
    res = ws_source.batch_get([f"A{START_ROW}:A{END_ROW}", f"D{START_ROW}:D{END_ROW}"])
    company_list = [r[0] if r else "" for r in res[0]]
    url_day_list = [r[0] if r else "" for r in res[1]]
    
    sheet_dest = connect_dest_sheet()
    log(f"✅ Ready. Starting Row {last_row}")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}")
    sys.exit(1)

batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows, sheet_dest
    if not batch_list: return True
    log(f"🚀 Uploading batch: {buffered_rows} rows...")
    for attempt in range(5):
        try:
            sheet_dest.batch_update(batch_list, value_input_option="RAW")
            batch_list = []
            buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ Write retry {attempt+1}: {e}")
            time.sleep(10)
            sheet_dest = connect_dest_sheet()
    return False

try:
    local_idx = max(0, last_row - START_ROW)
    for i in range(local_idx, len(company_list)):
        curr_row = START_ROW + i
        name = company_list[i]
        u_day = url_day_list[i] if url_day_list[i].startswith("http") else None
        
        log(f"--- [ROW {curr_row}] {name} ---")
        vals = scrape_day(u_day)
        
        batch_list.append({"range": f"{WRITE_NAME_COL}{curr_row}", "values": [[name]]})
        batch_list.append({"range": f"{WRITE_DATE_COL}{curr_row}", "values": [[current_date]]})
        batch_list.append({"range": f"{WRITE_VALUE_START_COL}{curr_row}", "values": [vals] if vals else [[]]})
        
        buffered_rows += 1
        with open(checkpoint_file, "w") as f: f.write(str(curr_row + 1))
        
        if (i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()
        if buffered_rows >= BATCH_SIZE: 
            flush_batch()
            restart_driver()

finally:
    flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
