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
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500")) 
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

# ---------------- BROWSER ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing Chrome Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(120)
    return driver

# ---------------- SCRAPER (Enhanced Detection) ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 {url_type}: {url}")
    for attempt in range(1, 4):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 45)
            
            # This selector covers almost all technical indicator value types on TradingView
            val_css = "[class*='valueValue'], [class*='counter-'], .js-symbol-last"
            
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, val_css)))
            
            # Scroll to trigger any lazy-loaded indicator tables
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(15) # Essential for TradingView JS math to finish
            
            # FIXED JAVASCRIPT: Comprehensive extraction
            raw_data = driver.execute_script("""
                const elements = document.querySelectorAll("[class*='valueValue'], .js-symbol-last");
                return Array.from(elements).map(el => el.innerText.trim());
            """)
            
            # Filter out UI text, keeping only numbers/signals
            clean_data = [str(v) for v in raw_data if v and v not in ["Detailed", "∅", "—", "Neutral", "Buy", "Sell"]]
            
            # If you want the signals (Buy/Sell) included, remove "Neutral", "Buy", "Sell" from the list above.
            
            if len(clean_data) > 3:
                log(f"   ✅ {url_type} SUCCESS: Found {len(clean_data)} indicators.")
                log(f"   📊 DATA OBTAINED: {', '.join(clean_data[:15])}...") # Shows what it got in logs
                return clean_data
            else:
                log(f"   ⚠️ Attempt {attempt}: Low indicator count ({len(clean_data)}). Retrying...")
                driver.refresh()
                time.sleep(10)
        except Exception as e:
            log(f"   ❌ {url_type} Error: {str(e)[:60]}")
    return []

# ---------------- DATA SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)
    total_count = len(company_list)
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- RANGE LOGIC ---------------- #
start_idx = SHARD_INDEX * SHARD_SIZE
end_idx = total_count if SHARD_INDEX == 4 else min(start_idx + SHARD_SIZE, total_count)

if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        saved = f.read().strip()
        current_idx = int(saved) if saved.isdigit() else start_idx
else:
    current_idx = start_idx

current_idx = max(start_idx, current_idx)
log(f"🚀 Processing Rows {start_idx+1} to {end_idx}")

# ---------------- MAIN ---------------- #
driver = create_driver()
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

def save_to_sheets():
    global batch_list
    if not batch_list: return
    log(f"💾 Updating Google Sheets...")
    try:
        sheet_data.batch_update(batch_list, value_input_option='RAW')
        batch_list = []
        log(f"✅ Sheets Sync Complete.")
    except Exception as e:
        log(f"⚠️ Sheets API Error: {e}")

try:
    for i in range(current_idx, end_idx):
        symbol_name = company_list[i].strip()
        if symbol_name.lower() == "symbol" or not symbol_name: continue

        log(f"--- [Row {i+1}] {symbol_name} ---")
        
        u_d = url_d_list[i] if i < len(url_d_list) and "http" in str(url_d_list[i]) else None
        u_h = url_h_list[i] if i < len(url_h_list) and "http" in str(url_h_list[i]) else None
        
        data_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        data_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        
        combined = data_d + data_h
        row_num = i + 1
        
        batch_list.append({"range": f"A{row_num}", "values": [[symbol_name]]})
        batch_list.append({"range": f"J{row_num}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_num}", "values": [combined]})

        if len(batch_list) >= 30: # Save every 10 stocks
            save_to_sheets()
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
        
        time.sleep(1)

finally:
    save_to_sheets()
    driver.quit()
    log("🏁 All Task Finished.")
