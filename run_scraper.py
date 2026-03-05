import sys
import os
import time
import json
import random
import re
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
checkpoint_file = f"checkpoint_{SHARD_INDEX}.txt"

# ---------------- BROWSER SETUP ---------------- #
def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(120)
    return driver

# ---------------- ACCURACY-FIRST SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type):
    log(f"    🔍 Scanning {url_type}...")
    
    for attempt in range(1, 4):
        try:
            driver.get(url)
            
            # 1. Wait for the element to even exist
            wait = WebDriverWait(driver, 40)
            target_css = "[class*='valueValue']"
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, target_css)))
            
            # 2. ACCURACY TRICK: Wait for the FIRST element to NOT be '∅'
            # This ensures the JS engine has finished calculating before we scrape.
            try:
                wait.until(lambda d: d.find_element(By.CSS_SELECTOR, target_css).text not in ["∅", "—", "", "0"])
            except:
                log(f"    ⏳ Data still calculating (∅)... forcing wait.")
                time.sleep(15)

            # Final settle time
            time.sleep(5)
            
            # 3. Fast extraction using Javascript (Faster than BeautifulSoup)
            raw_data = driver.execute_script("""
                return Array.from(document.querySelectorAll("[class*='valueValue']")).map(el => el.innerText);
            """)
            
            # 4. Filter and Validate
            clean_data = [str(v).strip() for v in raw_data if v and v not in ["∅", "—", ""]]
            
            if len(clean_data) >= 10: # Standard TradingView technical page has 20+ values
                log(f"    ✅ {url_type} SUCCESS: Captured {len(clean_data)} values.")
                log(f"    📊 Values: {clean_data[:5]} ... {clean_data[-3:]}")
                return clean_data
            else:
                log(f"    ⚠️ Attempt {attempt}: Incomplete data ({len(clean_data)} values). Retrying...")
                driver.refresh()
                time.sleep(10)
                
        except Exception as e:
            log(f"    ❌ Attempt {attempt} Error: {str(e)[:50]}")
            
    return []

# ---------------- DATA INITIALIZATION ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    all_names = sheet_main.col_values(1)
    all_url_d = sheet_main.col_values(4)
    all_url_h = sheet_main.col_values(8)
    total_symbols = len(all_names)
except Exception as e:
    log(f"❌ Critical Connection Error: {e}"); sys.exit(1)

# Sequential Sharding Logic
start_row = SHARD_INDEX * SHARD_SIZE
end_row = total_symbols if SHARD_INDEX == 4 else min(start_row + SHARD_SIZE, total_symbols)

# Resume from checkpoint
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        current_start = int(f.read().strip())
else:
    current_start = start_row

current_start = max(start_row, current_start)
log(f"🚀 Shard {SHARD_INDEX} | Processing Rows: {current_start + 1} to {end_row}")

# ---------------- PROCESSING LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_FLUSH_INTERVAL = 10 
current_date = date.today().strftime("%m/%d/%Y")

try:
    for i in range(current_start, end_row):
        symbol_name = all_names[i].strip()
        log(f"--- [{i+1}/{total_symbols}] {symbol_name} ---")
        
        url_d = all_url_d[i] if i < len(all_url_d) and "http" in str(all_url_d[i]) else None
        url_h = all_url_h[i] if i < len(all_url_h) and "http" in str(all_url_h[i]) else None
        
        # Scrape both intervals
        data_d = scrape_tradingview(driver, url_d, "DAILY") if url_d else []
        data_h = scrape_tradingview(driver, url_h, "HOURLY") if url_h else []
        
        combined_data = data_d + data_h
        row_num = i + 1
        
        # Prepare batch update
        batch_list.append({"range": f"A{row_num}", "values": [[symbol_name]]})
        batch_list.append({"range": f"J{row_num}", "values": [[current_date]]})
        if combined_data:
            batch_list.append({"range": f"K{row_num}", "values": [combined_data]})

        # Flush batch and update checkpoint
        if len(batch_list) >= (BATCH_FLUSH_INTERVAL * 3):
            log(f"💾 Saving progress to Sheets...")
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            batch_list = []
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))

finally:
    if batch_list:
        sheet_data.batch_update(batch_list, value_input_option='RAW')
    driver.quit()
    log("🏁 Shard Task Finished.")
