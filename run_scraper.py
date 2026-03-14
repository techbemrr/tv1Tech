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
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 22  
BATCH_SIZE = 5  
RESTART_EVERY_ROWS = 15 # Restart more frequently to keep memory clean
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3 

# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            last_i = max(int(f.read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:100]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return None

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [DAY Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            drv.refresh()
            time.sleep(2)
        except: pass
    return drv

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try: driver.quit()
        except: pass
    driver = None

# ---------------- SCRAPER ---------------- #
def get_values(drv):
    try:
        elements = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
        return [el.text.strip() for el in elements if el.text.strip() and el.text.strip() not in ("—", "")]
    except: return []

def scrape_day(url):
    if not url: return []
    for attempt in range(3): # Increased to 3 attempts per symbol
        try:
            drv = ensure_driver()
            drv.get(url)
            
            # Wait for specific UI element
            wait = WebDriverWait(drv, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']")))
            
            # Dynamic Wait: Check if values actually appeared
            for _ in range(5): # Check 5 times with small sleeps
                vals = get_values(drv)
                if len(vals) >= 10: # If we see at least 10 indicators, it's loading well
                    break
                drv.execute_script("window.scrollTo(0, 500);")
                time.sleep(1.5)
            
            # Final scroll for bottom indicators
            drv.execute_script("window.scrollTo(0, 1000);")
            time.sleep(2)
            vals = get_values(drv)

            if len(vals) > 0:
                # Pad to ensure the list length matches your columns
                return (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                
        except Exception as e:
            log(f"    ❌ Attempt {attempt+1} failed for {url[:50]}")
            restart_driver() # Hard reset driver on failure
            time.sleep(2)
            
    return []

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4) 
    log(f"✅ Connection Stable. Processing {len(company_list)} symbols.")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}"); sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None
        
        vals = scrape_day(url)
        
        # If we got absolutely nothing after 3 tries, log it but don't break the sheet
        if not vals:
            log(f"🛑 [{i+1}] {name}: SKIPPED (No data after retries)")
            with open(checkpoint_file, "w") as f: f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}/{loop_end}] {name} | Found {len([v for v in vals if v])} values")
        
        row_idx = i + 1
        
        # Add to batch
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"B{row_idx}", "values": [[current_date]]})
        batch_list.append({
            "range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}",
            "values": [vals]
        })

        # Save progress
        with open(checkpoint_file, "w") as f: f.write(str(i + 1))
        
        # Periodic driver restart to prevent lag
        if (i + 1) % RESTART_EVERY_ROWS == 0: 
            restart_driver()

        # Small batch upload for safety
        if len(batch_list) >= (BATCH_SIZE * 3):
            log(f"🚀 Pushing {BATCH_SIZE} rows to Google Sheets...")
            if api_retry(sheet_data.batch_update, batch_list, value_input_option="USER_ENTERED"):
                batch_list = []

finally:
    if batch_list:
        log("🚀 Pushing final rows...")
        api_retry(sheet_data.batch_update, batch_list, value_input_option="USER_ENTERED")
    restart_driver()
    log("🏁 SHARD COMPLETED.")
