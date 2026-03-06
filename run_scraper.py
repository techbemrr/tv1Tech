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
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG & RANGE ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW   = START_ROW + SHARD_SIZE

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else START_ROW

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing...")
    opts = Options()
    opts.page_load_strategy = "normal" 
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(90)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(4)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try: driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            driver.refresh()
            time.sleep(4)
        except: pass
    return driver

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"    📡 Navigating {url_type}...")
    try:
        driver.get(url)
        time.sleep(12) 
        
        final_values = []
        for check in range(5): 
            soup = BeautifulSoup(driver.page_source, "html.parser")
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            
            raw_values = v1 or v2
            final_values = [str(v) for v in raw_values if v and v.strip()]
            
            if final_values:
                log(f"    📊 Found {len(final_values)} values.")
                return final_values
            
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(2)
            
    except Exception as e:
        log(f"    ❌ {url_type} ERROR: {str(e)[:50]}")
    return []

# ---------------- MAIN ---------------- #
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

driver = create_driver()
batch_list = []
BATCH_SIZE = 10 # Reduced size to avoid API timeouts; change back to 300 if stable
current_date = date.today().strftime("%m/%d/%Y")

try:
    for i in range(last_i, min(END_ROW, len(company_list))):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] {name} ---")

        u_d = url_d_list[i] if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i] if i < len(url_h_list) and url_h_list[i].startswith("http") else None
        
        vals_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        
        # REQUIRED CHANGE: Structure the batch update correctly for gspread
        batch_list.append({'range': f'A{row_idx}', 'values': [[name]]})
        batch_list.append({'range': f'J{row_idx}', 'values': [[current_date]]})
        
        if combined:
            # We wrap combined in another list because values expects a list of rows
            batch_list.append({'range': f'K{row_idx}', 'values': [combined]})
        
        # Check based on how many symbols we've processed (3 entries per symbol)
        if len(batch_list) >= (BATCH_SIZE * 3):
            log(f"🚀 Batch threshold reached. Saving to sheet...")
            try:
                sheet_data.batch_update(batch_list, value_input_option='USER_ENTERED')
                batch_list = []
            except Exception as e:
                log(f"❌ Batch Update Failed: {e}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        time.sleep(1)

finally:
    if batch_list:
        log(f"📤 Final Flush: Uploading remaining symbols...")
        try:
            sheet_data.batch_update(batch_list, value_input_option='USER_ENTERED')
        except Exception as e:
            log(f"❌ Final Flush Failed: {e}")
    
    if driver: 
        driver.quit()
    log("🏁 Shard Completed.")
