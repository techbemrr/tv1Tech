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
    print(msg, flush=True)

# ---------------- CONFIG & SEQUENTIAL SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing Chrome...")
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
            time.sleep(5)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            driver.refresh()
            time.sleep(5)
            log("✅ Cookies applied.")
        except: 
            log("⚠️ Cookie error.")
    return driver

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"    📡 Navigating to {url_type}: {url}")
    for attempt in range(3):
        try:
            driver.get(url)
            try:
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']"))
                )
            except TimeoutException:
                log(f"    ⏳ Timeout on {url_type}, trying force load...")

            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(25) 
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            
            raw_values = v1 or v2
            # FILTER ∅ and —
            final_values = [str(v) for v in raw_values if v and v != "∅" and v != "—"]
            
            if final_values and len(final_values) > 5:
                return final_values
            else:
                log(f"    ⚠️ Data Invalid (∅). Retry {attempt+1}/3...")
                driver.refresh()
                time.sleep(15)
        except Exception as e:
            log(f"    ❌ Error: {str(e)[:50]}")
            time.sleep(5)
    return []

# ---------------- SETUP DATA ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    all_names = sheet_main.col_values(1)
    all_url_d = sheet_main.col_values(4)
    all_url_h = sheet_main.col_values(8)
    total_symbols = len(all_names)
    log(f"✅ Total Symbols Found: {total_symbols}")
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- RANGE CALCULATION ---------------- #
start_row = SHARD_INDEX * SHARD_SIZE
# Final Shard (4) takes everything left + 300 buffer safety
if SHARD_INDEX == 4:
    end_row = total_symbols
else:
    end_row = start_row + SHARD_SIZE

# Resuming from checkpoint
current_start = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else start_row
current_start = max(start_row, current_start)

log(f"🚀 Shard {SHARD_INDEX} processing indices {current_start} to {end_row}")

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
BATCH_SIZE = 25
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    for attempt in range(5):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log(f"✅ Batch Saved.")
            batch_list = []
            return
        except Exception as e:
            wait = 60 if "429" in str(e) else 15
            log(f"⚠️ API Error. Retrying in {wait}s...")
            time.sleep(wait)

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

try:
    for i in range(current_start, end_row):
        if i >= len(all_names): break # Buffer safety
        
        name = all_names[i].strip()
        log(f"--- [ROW {i+1}] {name} ---")

        active_driver = ensure_driver()
        u_d = all_url_d[i] if i < len(all_url_d) and all_url_d[i].startswith("http") else None
        u_h = all_url_h[i] if i < len(all_url_h) and all_url_h[i].startswith("http") else None
        
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
        
        time.sleep(2)

finally:
    if batch_list: flush_batch()
    if driver: driver.quit()
    log("🏁 Shard Completed.")
