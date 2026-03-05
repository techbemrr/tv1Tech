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

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "25")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(90)
    return driver

# ---------------- SCRAPER (Correct Path + Math Wait) ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 {url_type}: {url}")
    for attempt in range(1, 4):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 45)
            
            # 1. Wait for the element to exist
            target_css = "[class*='valueValue']"
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, target_css)))
            
            # 2. THE FIX: Wait until the text is NOT '∅' or empty
            # We give it up to 20 seconds specifically for the math to load
            try:
                wait.until(lambda d: d.find_element(By.CSS_SELECTOR, target_css).text.strip() not in ["", "∅", "—"])
            except:
                log(f"   ⏳ Data still calculating (∅)... attempt {attempt}")

            # 3. Trigger rendering
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(5) # Final settle wait
            
            # 4. Extraction using your EXACT PATHS
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # Path v1
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            # Path v2
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            # Path v3
            v3 = [el.text.strip() for el in driver.find_elements(By.XPATH, "//div[contains(@class, 'value') and contains(@class, 'Value')]")]
            
            raw_values = v1 or v2 or v3
            # Filter out any lingering placeholders
            final_values = [str(v) for v in raw_values if v and v not in ["∅", "—"]]
            
            if len(final_values) > 5: # Assuming you expect more than 5 indicators
                log(f"   ✅ SUCCESS: Captured {len(final_values)} values.")
                return final_values
            else:
                log(f"   ⚠️ Low data/Placeholders found. Refreshing...")
                driver.refresh()
                time.sleep(10)
        except Exception as e:
            log(f"   ❌ Error: {str(e)[:60]}")
    return []

# ---------------- SETUP DATA ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
BATCH_SIZE = 15 # Smaller batch for more frequent saving
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    log(f"💾 Saving batch to Google Sheets...")
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            batch_list = []
            return
        except:
            time.sleep(10)

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        symbol = company_list[i].strip()
        if not symbol or symbol.lower() == "symbol": continue

        log(f"--- [Row {i+1}] {symbol} ---")
        if driver is None: driver = create_driver()
        
        u_d = url_d_list[i] if i < len(url_d_list) and "http" in str(url_d_list[i]) else None
        u_h = url_h_list[i] if i < len(url_h_list) and "http" in str(url_h_list[i]) else None
        
        vals_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[symbol]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_idx}", "values": [combined]})
        
        if len(batch_list) >= (BATCH_SIZE * 3):
            flush_batch()
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
        
        time.sleep(1)

finally:
    if batch_list: flush_batch()
    if driver: driver.quit()
    log("🏁 Shard Completed.")
