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
from selenium.webdriver.support import expected_conditions as EC
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

# Always starts from the first row of your list
START_FROM_SCRATCH = True 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")
CREDENTIALS_FILE = "credentials.json"

EXPECTED_COUNT = 20
BATCH_SIZE = 5
RESTART_EVERY_ROWS = 15

# --- UPDATED LOCATIONS ---
SOURCE_SHEET_NAME = "Stock List"
DEST_SPREADSHEET_ID = "1NYqFa7KEyHCLivd86RJNT9cZN0SIZeARgEH6BgW25yk"
DEST_WORKSHEET_NAME = "Sheet1" 

CHROME_DRIVER_PATH = ChromeDriverManager().install()

if not START_FROM_SCRATCH and os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            last_i = max(int(f.read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- GOOGLE AUTH ---------------- #
def get_gc():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if not os.path.exists(CREDENTIALS_FILE):
        log(f"❌ CRITICAL: {CREDENTIALS_FILE} not found.")
        sys.exit(1)
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def safe_call(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "quota" in err or "200" in err:
                wait = (attempt + 1) * 20
                log(f"⚠️ API issue. Waiting {wait}s...")
                time.sleep(wait)
            elif "permission" in err or "403" in err:
                log("❌ PERMISSION DENIED: Share the sheet with the Service Account email as EDITOR.")
                sys.exit(1)
            else:
                log(f"⚠️ Error: {err[:100]}")
                time.sleep(10)
    raise Exception("API failure after multiple retries.")

def connect_sheets():
    gc = get_gc()
    ws_main = safe_call(gc.open, SOURCE_SHEET_NAME).worksheet("Sheet1")
    ws_data = safe_call(gc.open_by_key, DEST_SPREADSHEET_ID).worksheet(DEST_WORKSHEET_NAME)
    return ws_main, ws_data

# ---------------- SCRAPER ---------------- #
driver = None

def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

def scrape_day(url):
    if not url: return []
    global driver
    if not driver: driver = create_driver()
    
    try:
        driver.get(url)
        # Wait specifically for the data values to appear
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']"))
        )
        time.sleep(5) # Stabilization time
        
        # JS extraction ensures we get text hidden or lazy-loaded
        vals = driver.execute_script("""
            return Array.from(document.querySelectorAll("div[class*='valueValue']"))
                .map(el => el.innerText.strip())
                .filter(txt => txt !== "");
        """)
        return vals[:EXPECTED_COUNT] if vals else []
    except Exception as e:
        log(f"    ⚠️ Scrape error: {str(e)[:50]}")
        return []

# ---------------- MAIN ---------------- #
try:
    ws_main, ws_data = connect_sheets()
    log("📥 Pre-fetching source data...")
    company_list = safe_call(ws_main.col_values, 1)
    url_day_list = safe_call(ws_main.col_values, 4)
    log(f"✅ Ready. Starting at Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

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
        batch_list.append({"range": f"K{row_idx}", "values": [vals] if vals else [["N/A"] * EXPECTED_COUNT]})

        # Checkpoint
        with open(checkpoint_file, "w") as f: f.write(str(i + 1))

        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0:
            if driver: driver.quit()
            driver = None

        if len(batch_list) // 3 >= BATCH_SIZE:
            log(f"🚀 UPLOADING BATCH to {DEST_WORKSHEET_NAME}...")
            safe_call(ws_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []
            if driver: driver.quit()
            driver = None
            # Refresh connection for next chunk
            ws_main, ws_data = connect_sheets()

finally:
    if batch_list:
        try: safe_call(ws_data.batch_update, batch_list, value_input_option="RAW")
        except: pass
    if driver: driver.quit()
    log("🏁 DAY Shard Completed.")
