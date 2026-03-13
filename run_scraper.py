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

# Use the environment variable for checkpoint if provided
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")
CREDENTIALS_FILE = "credentials.json"

EXPECTED_COUNT = 20
BATCH_SIZE = 5
RESTART_EVERY_ROWS = 15

SOURCE_SHEET_NAME = "Stock List"
DEST_SHEET_NAME = "MV2 DAY"  
DEST_WORKSHEET_NAME = "Sheet1" 

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# Initialize last_i from checkpoint
if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            last_i = max(int(f.read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- GOOGLE AUTH ---------------- #
def get_gc():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"Missing {CREDENTIALS_FILE}")
    
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def safe_call(func, *args, **kwargs):
    for attempt in range(7):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err = str(e).lower()
            if "200" in err or "429" in err or "quota" in err or "permission" in err:
                wait_time = (2 ** attempt) * 12 + random.uniform(2, 5)
                log(f"⚠️ API issue, retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("Max retries reached for Google Sheets.")

def connect_sheets():
    log(f"📊 Connecting to {DEST_SHEET_NAME}...")
    gc = get_gc()
    sheet_main = safe_call(gc.open, SOURCE_SHEET_NAME)
    sheet_data = safe_call(gc.open, DEST_SHEET_NAME) 
    return sheet_main.worksheet("Sheet1"), sheet_data.worksheet(DEST_WORKSHEET_NAME)

# ---------------- BROWSER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)
    return drv

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    try:
        if driver: driver.quit()
    except: pass
    driver = None
    time.sleep(2)

def get_visible_value_elements(drv):
    script = """
    return Array.from(document.querySelectorAll("div[class*='valueValue']"))
        .map(el => el.innerText.strip())
        .filter(txt => txt && !txt.includes('%') && !/[KMB]$/i.test(txt));
    """
    try: return drv.execute_script(script)
    except: return []

def scrape_day(url):
    if not url: return []
    log(f"    📡 Navigating: {url}")
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            WebDriverWait(drv, 25).until(lambda d: len(get_visible_value_elements(d)) > 0)
            time.sleep(3)
            values = get_visible_value_elements(drv)
            if len(values) >= EXPECTED_COUNT:
                return values[:EXPECTED_COUNT]
        except Exception as e:
            log(f"    ❌ Scrape error: {str(e)[:50]}")
            restart_driver()
    return []

# ---------------- MAIN ---------------- #
try:
    ws_main, ws_data = connect_sheets()
    log("📥 Pre-fetching source columns...")
    company_list = safe_call(ws_main.col_values, 1)
    url_day_list = safe_call(ws_main.col_values, 4)
    log(f"✅ Ready. Starting Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows, ws_data
    if not batch_list: return True
    log(f"🚀 UPLOADING BATCH to {DEST_SHEET_NAME}...")
    try:
        safe_call(ws_data.batch_update, batch_list, value_input_option="RAW")
        batch_list, buffered_rows = [], 0
        log("✅ Upload complete.")
        return True
    except Exception as e:
        log(f"❌ Flush failed: {e}")
        return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        u_day = url_day_list[i].strip() if i < len(url_day_list) and url_day_list[i].startswith("http") else None
        
        log(f"--- [ROW {i+1}] {name} ---")
        vals = scrape_day(u_day)
        
        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [vals] if vals else [[]]})
        buffered_rows += 1

        with open(checkpoint_file, "w") as f: 
            f.write(str(i + 1))

        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if buffered_rows >= BATCH_SIZE:
            flush_batch()
            restart_driver()
            ws_main, ws_data = connect_sheets()

finally:
    if batch_list: 
        flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
