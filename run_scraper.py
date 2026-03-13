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
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
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

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- SETTINGS ---------------- #
BATCH_SIZE = 5
RESTART_EVERY_ROWS = 15
EXPECTED_COUNTS = {"DAILY": 20, "HOURLY": 12}
CREDENTIALS_FILE = "credentials.json"

# ---------------- GOOGLE SHEETS AUTH WITH BACKOFF ---------------- #
def get_gc_client():
    """Stateless authentication to prevent PermissionError on token cache."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    # Check if we have a JSON string in environment (cleaner for CI/CD)
    creds_json = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def safe_sheet_call(func, *args, **kwargs):
    """Wrapper to handle Read/Write quota issues with exponential backoff."""
    for attempt in range(7):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "quota" in err_msg.lower() or "PermissionError" in err_msg:
                wait = (2 ** attempt) + (attempt * 5)
                log(f"⚠️ API Quota/Permission hit. Retrying in {wait}s... (Attempt {attempt+1})")
                time.sleep(wait)
            else:
                raise e
    raise Exception("Max retries exceeded for Google Sheets operation.")

def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = get_gc_client()
    # Destination Updated as requested
    sheet_main = safe_sheet_call(gc.open, "Stock List")
    sheet_data = safe_sheet_call(gc.open, "MV2 for SQL")
    return sheet_main.worksheet("Sheet1"), sheet_data.worksheet("Sheet2")

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)
    return drv

driver = None
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
def get_clean_values_js(drv):
    script = """
    return Array.from(document.querySelectorAll("div[data-name='legend-item-value']"))
        .map(el => el.innerText.trim())
        .filter(txt => {
            if (!txt || txt === '∅' || txt.includes('%') || /[KMB]$/i.test(txt)) return false;
            return /\\d/.test(txt);
        });
    """
    try: return drv.execute_script(script)
    except: return []

def scrape_tradingview(url, url_type=""):
    if not url: return []
    expected = EXPECTED_COUNTS.get(url_type, 0)
    log(f"    📡 Navigating {url_type}: {url}")
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            time.sleep(4) # Buffer for initial load
            for _ in range(5):
                vals = get_clean_values_js(drv)
                if len(vals) >= expected:
                    log(f"    📊 Found {len(vals)} values.")
                    return vals
                time.sleep(2)
        except Exception as e:
            log(f"    ❌ Scrape Error: {str(e)[:50]}")
            restart_driver()
    return []

# ---------------- MAIN EXECUTION ---------------- #
try:
    ws_main, ws_data = connect_sheets()
    
    # BUFFERED READ: Get all column data at once to minimize read requests
    log("📥 Reading source columns (Minimized Requests)...")
    company_list = safe_sheet_call(ws_main.col_values, 1)
    url_d_list = safe_sheet_call(ws_main.col_values, 4)
    url_h_list = safe_sheet_call(ws_main.col_values, 8)
    
    if os.path.exists(checkpoint_file):
        try: last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
        except: last_i = START_ROW
    else: last_i = START_ROW
    
    log(f"✅ Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

batch_list, buffered_rows = [], 0
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows, ws_data
    if not batch_list: return True
    log(f"🚀 UPLOADING BATCH: {buffered_rows} rows...")
    try:
        safe_sheet_call(ws_data.batch_update, batch_list, value_input_option='RAW')
        batch_list, buffered_rows = [], 0
        return True
    except Exception as e:
        log(f"❌ Critical Upload Failure: {e}")
        return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        u_d = url_d_list[i].strip() if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i].strip() if i < len(url_h_list) and url_h_list[i].startswith("http") else None

        log(f"--- [ROW {i+1}] {name} ---")
        vals_d = scrape_tradingview(u_d, "DAILY")
        vals_h = scrape_tradingview(u_h, "HOURLY")
        
        combined = vals_d + vals_h
        row_idx = i + 1
        
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [combined] if combined else [[]]})
        buffered_rows += 1

        with open(checkpoint_file, "w") as f: f.write(str(i + 1))
        
        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()
        
        if buffered_rows >= BATCH_SIZE: 
            flush_batch()
            # Refresh sheet connections after a batch to keep session alive
            ws_main, ws_data = connect_sheets()

finally:
    if batch_list: flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
