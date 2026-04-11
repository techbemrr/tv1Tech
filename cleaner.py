import time
import json
import random
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
EXPECTED_COUNT = 26
COOKIE_FILE = "cookies.json"
CREDS_FILE = "credentials.json"
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# Column Indexing for MV2 DAY (1-based for gspread)
NAME_COL = 1        
STATUS_COL = 29     
URL_COL = 30        
DATA_START_COL = "C"
DATA_END_COL = "AB"

# ---------------- DRIVER ---------------- #
def create_driver():
    log("🌐 Initializing Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    
    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                cookie_data = {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                drv.add_cookie(cookie_data)
            drv.refresh()
        except Exception as e:
            log(f"⚠️ Cookie Warning: {e}")
            
    return drv

# ---------------- SCRAPER ---------------- #
def aggressive_scrape(drv, url):
    try:
        drv.get(url)
        drv.execute_script("document.body.style.zoom='50%'")
        WebDriverWait(drv, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='value']")))
        
        for i in range(4):
            js_script = 'return Array.from(document.querySelectorAll("[class*=\'valueValue\'], [class*=\'value-\']")).map(el => el.innerText.trim()).filter(txt => txt.length > 0);'
            vals = drv.execute_script(js_script)
            if len(vals) >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT]
            drv.execute_script(f"window.scrollBy(0, 400);")
            time.sleep(2)
        return vals
    except Exception as e:
        log(f"   ❌ Scrape Failed: {str(e)[:50]}")
        return []

# ---------------- MAIN CLEANUP ---------------- #
def run_cleanup():
    log("🚀 Starting Cleanup Process...")
    
    if not os.path.exists(CREDS_FILE):
        log(f"❌ CRITICAL: {CREDS_FILE} is missing!")
        return

    try:
        log(f"🔑 Attempting to authenticate with {CREDS_FILE}...")
        gc = gspread.service_account(filename=CREDS_FILE)
        
        log("📊 Opening Worksheets...")
        sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
        sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
        
        log("📥 Fetching Sheet Data...")
        all_data_rows = sh_data.get_all_values()
        stock_list_raw = sh_main.get_all_values()
        log(f"✅ Found {len(all_data_rows)} rows in MV2 DAY.")
    except Exception as e:
        log(f"❌ Connection Error: {e}")
        # Debug: Print file content length to see if it's empty
        if os.path.exists(CREDS_FILE):
            log(f"DEBUG: {CREDS_FILE} size is {os.path.getsize(CREDS_FILE)} bytes.")
        return

    url_map = {row[0].strip(): row[3].strip() for row in stock_list_raw if len(row) > 3}
    driver = create_driver()
    
    fixed_count = 0
    try:
        for i, row in enumerate(all_data_rows):
            if i == 0: continue
            row_num = i + 1
            
            symbol = row[NAME_COL-1].strip()
            status = row[STATUS_COL-1] if len(row) >= STATUS_COL else "EMPTY"
            
            if status != "OK":
                url = url_map.get(symbol)
                if not url:
                    log(f"⏭️ Row {row_num} [{symbol}]: No URL found in Stocklist. Skipping.")
                    continue

                log(f"🔍 Row {row_num} [{symbol}]: Status is '{status}'. Retrying...")
                new_vals = aggressive_scrape(driver, url)

                if len(new_vals) >= EXPECTED_COUNT:
                    sh_data.update(f"{DATA_START_COL}{row_num}:{DATA_END_COL}{row_num}", [new_vals])
                    sh_data.update_cell(row_num, STATUS_COL, "OK")
                    log(f"✅ Row {row_num} [{symbol}]: Fixed successfully.")
                    fixed_count += 1
                else:
                    log(f"⚠️ Row {row_num} [{symbol}]: Only found {len(new_vals)} values. Still failing.")
                
                time.sleep(1)

    finally:
        if driver: driver.quit()
        log(f"🏁 Cleanup Finished. Total symbols fixed: {fixed_count}")

if __name__ == "__main__":
    run_cleanup()
