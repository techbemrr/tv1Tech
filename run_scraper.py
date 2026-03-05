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
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- CONFIG & BLOCK SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
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
    driver.set_page_load_timeout(100)
    return driver

# ---------------- SCRAPER (Your Paths + Accuracy Wait) ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 {url_type}: {url}")
    for attempt in range(1, 4):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 45)
            
            # Selector for technical values
            target_css = "[class*='valueValue']"
            
            # 1. Wait for element to exist
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, target_css)))
            
            # 2. ANTI-EMPTY WAIT (Stops ∅ errors)
            try:
                wait.until(lambda d: d.find_element(By.CSS_SELECTOR, target_css).text.strip() not in ["", "∅", "—"])
            except:
                log(f"   ⏳ Math calculation slow... pausing 15s.")
                time.sleep(15)

            # Scroll to load data
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(3)
            
            # 3. EXTRACTION (Your requested paths)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # Path V1
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            # Path V2
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            # Path V3
            v3 = [el.text.strip() for el in driver.find_elements(By.XPATH, "//div[contains(@class, 'value') and contains(@class, 'Value')]")]
            
            raw_values = v1 or v2 or v3
            final_values = [str(v) for v in raw_values if v and v not in ["∅", "—"]]
            
            if len(final_values) > 5:
                log(f"   ✅ SUCCESS: {len(final_values)} values. Sample: {final_values[:3]}")
                return final_values
            else:
                log(f"   ⚠️ Low data count. Refreshing...")
                driver.refresh()
                time.sleep(10)
        except Exception as e:
            log(f"   ❌ Scrape Error: {str(e)[:50]}")
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

# ---------------- SHARD RANGE ---------------- #
start_idx = SHARD_INDEX * SHARD_SIZE
end_idx = total_count if SHARD_INDEX == 4 else min(start_idx + SHARD_SIZE, total_count)

if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        saved = f.read().strip()
        current_idx = int(saved) if saved.isdigit() else start_idx
else:
    current_idx = start_idx

current_idx = max(start_idx, current_idx)
log(f"🚀 Shard {SHARD_INDEX} | Rows {start_idx+1} to {end_idx}")

# ---------------- MAIN ---------------- #
driver = create_driver()
batch_list = []
BATCH_LIMIT = 15 
current_date = date.today().strftime("%m/%d/%Y")

def flush():
    global batch_list
    if not batch_list: return
    log("💾 Saving batch...")
    for _ in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            batch_list = []
            return
        except: time.sleep(20)

try:
    for i in range(current_idx, end_idx):
        symbol = company_list[i].strip()
        if not symbol or symbol.lower() == "symbol": continue

        log(f"--- [Row {i+1}] {symbol} ---")
        
        u_d = url_d_list[i] if i < len(url_d_list) and "http" in str(url_d_list[i]) else None
        u_h = url_h_list[i] if i < len(url_h_list) and "http" in str(url_h_list[i]) else None
        
        vals_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_num = i + 1
        batch_list.append({"range": f"A{row_num}", "values": [[symbol]]})
        batch_list.append({"range": f"J{row_num}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_num}", "values": [combined]})
        
        if len(batch_list) >= (BATCH_LIMIT * 3):
            flush()
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
        
        time.sleep(1)

finally:
    if batch_list: flush()
    driver.quit()
    log("🏁 Shard Completed.")
