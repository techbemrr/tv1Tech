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
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
# Matches your YML shard setup
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "510")) 
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

EXPECTED_COUNT = 26
COOKIE_FILE = "cookies.json"
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3 

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

STATUS_COL_IDX = DAY_OUTPUT_START_COL + EXPECTED_COUNT
DATA_START_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DATA_END_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (attempt * 2) + 5 
            log(f"⚠️ API Issue: {str(e)[:50]}. Waiting {wait}s...")
            time.sleep(wait)
    return None

# ---------------- DRIVER ---------------- #
def create_driver():
    log(f"🌐 [CLEANSER Shard {SHARD_INDEX}] Initializing...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r") as f:
                cookies = json.load(f)
            for c in cookies:
                drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
            drv.refresh()
            time.sleep(2)
        except: pass
    return drv

# ---------------- SCRAPER ---------------- #
def aggressive_scrape(drv, url):
    try:
        drv.get(url)
        # Force browser to zoom out so more data is "visible" for the scraper
        drv.execute_script("document.body.style.zoom='50%'")
        
        # Longer wait for the data container
        WebDriverWait(drv, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='value']")))
        
        for _ in range(4):
            # JS script pulls data directly from DOM memory (more reliable than .text)
            js = "return Array.from(document.querySelectorAll(\"[class*='valueValue'], [class*='value-']\")).map(el => el.innerText.trim()).filter(txt => txt.length > 0);"
            vals = drv.execute_script(js)
            if len(vals) >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT]
            
            # Slow scroll to trigger lazy loading
            drv.execute_script("window.scrollBy(0, 500);")
            time.sleep(3)
        return vals
    except:
        return []

# ---------------- MAIN ---------------- #
def run_cleanup():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")

    log("📥 Fetching sheet data...")
    all_data = sh_data.get_all_values()
    stock_list = sh_main.get_all_values()
    
    # Map Symbol to URL
    url_map = {row[0].strip(): row[3].strip() for row in stock_list if len(row) > 3}

    driver = create_driver()
    fixed_count = 0
    
    loop_end = min(END_ROW, len(all_data))
    
    for i in range(START_ROW, loop_end):
        if i == 0: continue # Skip header
        
        row = all_data[i]
        symbol = row[0].strip()
        status = row[STATUS_COL_IDX - 1].strip().upper() if len(row) >= STATUS_COL_IDX else ""
        
        # TARGET: Anything that isn't 'OK'
        if status != "OK":
            url = url_map.get(symbol)
            if not url: continue

            log(f"🛠️ Cleansing Row {i+1} [{symbol}] | Current Status: {status if status else 'BLANK'}")
            new_vals = aggressive_scrape(driver, url)

            if len(new_vals) >= EXPECTED_COUNT:
                # Update cells one by one with a delay to prevent 429 Quota errors
                api_retry(sh_data.update, [new_vals], f"{DATA_START_LETTER}{i+1}:{DATA_END_LETTER}{i+1}")
                time.sleep(2) 
                
                api_retry(sh_data.update_cell, i+1, STATUS_COL_IDX, "OK")
                log(f"   ✅ Successfully Fixed!")
                fixed_count += 1
                time.sleep(3) # Extra cooldown
            else:
                log(f"   ⚠️ Still incomplete ({len(new_vals)}/26 values found)")

        # Refresh browser periodically to keep memory clean
        if (i + 1) % 20 == 0:
            driver.quit()
            driver = create_driver()

    driver.quit()
    log(f"🏁 Cleanser Shard {SHARD_INDEX} complete. Total fixed: {fixed_count}")

if __name__ == "__main__":
    run_cleanup()
