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

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "25")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(90)
    return driver

# ---------------- SCRAPER (Optimized for Accuracy) ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 {url_type}: {url}")
    for attempt in range(3):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 45)
            
            # 1. Wait for the element to exist
            target_selector = "[class*='valueValue']"
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, target_selector)))
            
            # 2. ACCURACY FIX: Wait until the text is NOT empty or placeholder (∅ or —)
            # This ensures the TradingView engine has finished its math
            try:
                wait.until(lambda d: d.find_element(By.CSS_SELECTOR, target_selector).text not in ["", "∅", "—"])
            except:
                log(f"   ⏳ Data still calculating... forcing 10s wait.")
                time.sleep(10)

            # 3. FAST EXTRACTION: Use JS to pull all values at once (Faster than BeautifulSoup)
            raw_values = driver.execute_script("""
                return Array.from(document.querySelectorAll("[class*='valueValue']")).map(el => el.innerText.strip());
            """)
            
            final_values = [str(v) for v in raw_values if v and v not in ["∅", "—"]]
            
            if len(final_values) > 5:
                log(f"   ✅ SUCCESS: Found {len(final_values)} indicators.")
                return final_values
            else:
                log(f"   ⚠️ Incomplete data. Retrying (Attempt {attempt+1}/3)...")
                driver.refresh()
                time.sleep(5)
        except Exception as e:
            log(f"   ❌ Error: {str(e)[:50]}")
    return []

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)
    log(f"✅ Data loaded: {len(company_list)} tickers.")
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
BATCH_SIZE = 25 # Smaller batch for more frequent saving
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    log(f"🚀 Saving batch to Sheets...")
    for attempt in range(5):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log(f"✅ Batch Saved.")
            batch_list = []
            return
        except Exception as e:
            wait = 60 if "429" in str(e) else 15
            time.sleep(wait)

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] {name} ---")
        active_driver = ensure_driver()
        
        u_d = url_d_list[i] if i < len(url_d_list) and "http" in str(url_d_list[i]) else None
        u_h = url_h_list[i] if i < len(url_h_list) and "http" in str(url_h_list[i]) else None
        
        vals_d = scrape_tradingview(active_driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(active_driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_idx}", "values": [combined]})
        
        if len(batch_list) >= (BATCH_SIZE * 3):
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        
        time.sleep(random.uniform(1, 2))

finally:
    if batch_list: flush_batch()
    if driver: driver.quit()
    log("🏁 Shard Completed.")
