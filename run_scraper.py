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
from selenium.common.exceptions import TimeoutException
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    # Logs with timestamp for better tracking in GitHub Actions
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- BLOCK-BASED SHARDING CONFIG ---------------- #
# Each YAML should provide SHARD_INDEX (0-4) and SHARD_SIZE (500)
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER SETUP ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing Chrome Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(90)
    return driver

# ---------------- SMART SCRAPER (Zero Error Logic) ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 Fetching {url_type}...")
    for attempt in range(1, 4):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 45)
            
            # Selector for TradingView indicator values
            val_css = "[class*='valueValue']"
            
            # 1. Wait for elements to appear in DOM
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, val_css)))
            
            # 2. SMART WAIT: Wait for '∅' or '—' to turn into actual numbers
            # This is the key to 100% accuracy.
            try:
                wait.until(lambda d: d.find_element(By.CSS_SELECTOR, val_css).text not in ["", "∅", "—", "0"])
            except TimeoutException:
                log(f"   ⏳ Data taking too long to calculate... forcing extra 10s wait.")
                time.sleep(10)

            # 3. EXTRACTION: Execute JS to pull all values instantly
            raw_data = driver.execute_script("""
                return Array.from(document.querySelectorAll("[class*='valueValue']")).map(el => el.innerText.strip());
            """)
            
            # Filter out any lingering null symbols
            clean_data = [str(v) for v in raw_data if v and v not in ["∅", "—"]]
            
            if len(clean_data) > 5:
                log(f"   ✅ {url_type} SUCCESS: Captured {len(clean_data)} indicators.")
                log(f"   📊 Sample: {clean_data[:4]}...")
                return clean_data
            else:
                log(f"   ⚠️ Attempt {attempt}: Incomplete data. Retrying...")
                driver.refresh()
                time.sleep(5)
        except Exception as e:
            log(f"   ❌ {url_type} Error: {str(e)[:60]}")
    return []

# ---------------- DATA INITIALIZATION ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)
    total_count = len(company_list)
    log(f"✅ Data Synchronized. Total Symbols: {total_count}")
except Exception as e:
    log(f"❌ Critical Error: {e}"); sys.exit(1)

# ---------------- BLOCK RANGE CALCULATION ---------------- #
# Row 1 is usually header, but col_values(1) gives all. 
# Shard 0 starts at 0, Shard 1 starts at 500, etc.
start_idx = SHARD_INDEX * SHARD_SIZE

if SHARD_INDEX == 4: # Last shard handles the remainder
    end_idx = total_count
else:
    end_idx = min(start_idx + SHARD_SIZE, total_count)

# Resume from checkpoint within assigned block
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        saved = f.read().strip()
        current_idx = int(saved) if saved.isdigit() else start_idx
else:
    current_idx = start_idx

# Protection against checkpoint being outside shard block
current_idx = max(start_idx, current_idx)

log(f"🚀 Shard {SHARD_INDEX} Assigned: Rows {start_idx+1} to {end_idx}")
log(f"📈 Resuming from Row: {current_idx+1}")

# ---------------- MAIN PROCESSING LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_LIMIT = 20 # Save every 20 symbols
current_date = date.today().strftime("%m/%d/%Y")

def save_to_sheets():
    global batch_list
    if not batch_list: return
    log(f"💾 Saving {len(batch_list)//3} items to Google Sheets...")
    for _ in range(3): # Retry logic for API limits
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log(f"✅ Batch Write Successful.")
            batch_list = []
            return
        except Exception as e:
            log(f"⚠️ Sheets API Busy, waiting 30s...")
            time.sleep(30)

try:
    for i in range(current_idx, end_idx):
        symbol_name = company_list[i].strip()
        log(f"--- [Row {i+1}] {symbol_name} ---")
        
        u_d = url_d_list[i] if i < len(url_d_list) and "http" in str(url_d_list[i]) else None
        u_h = url_h_list[i] if i < len(url_h_list) and "http" in str(url_h_list[i]) else None
        
        data_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        data_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        
        combined = data_d + data_h
        row_num = i + 1
        
        # Build the update rows
        batch_list.append({"range": f"A{row_num}", "values": [[symbol_name]]})
        batch_list.append({"range": f"J{row_num}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_num}", "values": [combined]})

        # Flush batch and update checkpoint
        if len(batch_list) >= (BATCH_LIMIT * 3):
            save_to_sheets()
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
        
        time.sleep(1) # Small delay to be polite to servers

finally:
    save_to_sheets()
    driver.quit()
    log("🏁 Shard Completed.")
