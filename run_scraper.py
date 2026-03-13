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

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")
CREDENTIALS_FILE = "credentials.json"

EXPECTED_COUNT = 20
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 15

# Source Spreadsheet
SOURCE_SHEET_NAME = "Stock List"

# Destination Spreadsheet (MV2 DAY)
DEST_SPREADSHEET_ID = "1NYqFa7KEyHCLivd86RJNT9cZN0SIZeARgEH6BgW25yk"
DEST_WORKSHEET_NAME = "Sheet1" 

CHROME_DRIVER_PATH = ChromeDriverManager().install()

if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            last_i = max(int(f.read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- GOOGLE AUTH & SAFE CALLS ---------------- #
def get_gc():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if not os.path.exists(CREDENTIALS_FILE):
        log(f"❌ CRITICAL: {CREDENTIALS_FILE} not found.")
        sys.exit(1)
    
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def safe_call(func, *args, **kwargs):
    """Retries with exponential backoff for API stability."""
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str or "200" in err_str:
                wait_time = (attempt + 1) * 15
                log(f"⚠️ API/Connection issue. Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                log(f"⚠️ Error: {err_str[:150]}")
                time.sleep(5)
    raise Exception("Operation failed after multiple retries.")

def connect_sheets():
    log(f"📊 Connecting to Spreadsheet ID: {DEST_SPREADSHEET_ID}")
    gc = get_gc()
    # Open Source by Name
    ws_main = safe_call(gc.open, SOURCE_SHEET_NAME).worksheet("Sheet1")
    # Open Destination by ID to ensure accuracy
    ws_data = safe_call(gc.open_by_key, DEST_SPREADSHEET_ID).worksheet(DEST_WORKSHEET_NAME)
    return ws_main, ws_data

# ---------------- BROWSER & SCRAPER ---------------- #
driver = None

def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def scrape_day(url):
    if not url: return []
    try:
        drv = ensure_driver()
        drv.get(url)
        time.sleep(5)
        vals = drv.execute_script("""
            return Array.from(document.querySelectorAll("div[class*='valueValue']"))
                .map(el => el.innerText.strip())
                .filter(txt => txt && !txt.includes('%') && !/[KMB]$/i.test(txt));
        """)
        return vals[:EXPECTED_COUNT] if vals else []
    except:
        return []

# ---------------- MAIN ---------------- #
try:
    ws_main, ws_data = connect_sheets()
    log("📥 Pre-fetching source columns...")
    company_list = safe_call(ws_main.col_values, 1)
    url_day_list = safe_call(ws_main.col_values, 4)
    log(f"✅ Ready. Row {last_i + 1}")
except Exception as e:
    log(f"❌ Startup Failed: {e}")
    sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        u_day = url_day_list[i].strip() if i < len(url_day_list) and url_day_list[i].startswith("http") else None
        
        log(f"--- [ROW {i+1}] {name} ---")
        vals = scrape_day(u_day)
        
        row_idx = i + 1
        # Mapping to Sheet1 of MV2 DAY
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [vals] if vals else [[]]})

        if len(batch_list) // 3 >= BATCH_SIZE:
            log(f"🚀 Uploading {BATCH_SIZE} rows...")
            safe_call(ws_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []
            with open(checkpoint_file, "w") as f: f.write(str(i + 1))
            
        if (i + 1) % RESTART_EVERY_ROWS == 0:
            if driver: driver.quit()
            driver = None

finally:
    if batch_list:
        safe_call(ws_data.batch_update, batch_list, value_input_option="RAW")
    if driver: driver.quit()
    log("🏁 Shard Completed.")
