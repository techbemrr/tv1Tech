import time
import json
import random
import os
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
# These file names must match the YAML output filenames
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
    log("🌐 Initializing High-Intensity Cleanup Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    
    # Load Cookies if file exists and is valid
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                cookie_data = {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                drv.add_cookie(cookie_data)
            drv.refresh()
            time.sleep(2)
        except Exception as e:
            log(f"⚠️ Cookie loading skipped: {e}")
            
    return drv

# ---------------- SCRAPER ---------------- #
def get_values_js(drv):
    js_script = """
    return Array.from(document.querySelectorAll("[class*='valueValue'], [class*='value-']"))
                .map(el => el.innerText.trim())
                .filter(txt => txt.length > 0);
    """
    return drv.execute_script(js_script)

def aggressive_scrape(drv, url):
    try:
        drv.get(url)
        drv.execute_script("document.body.style.zoom='50%'")
        
        # Long wait for elements to appear
        wait = WebDriverWait(drv, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='value']")))
        
        vals = []
        for i in range(5):
            vals = get_values_js(drv)
            if len(vals) >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT]
            
            drv.execute_script(f"window.scrollTo(0, {(i+1)*400});")
            time.sleep(3) 
            
        return vals
    except Exception as e:
        log(f"   ❌ Scrape Error: {str(e)[:50]}")
        return []

# ---------------- MAIN CLEANUP ---------------- #
def run_cleanup():
    # Verify Credentials file
    if not os.path.exists(CREDS_FILE):
        log(f"❌ Error: {CREDS_FILE} not found. Check GitHub Secrets.")
        return

    # Connect to Sheets
    try:
        gc = gspread.service_account(filename=CREDS_FILE)
        sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
        sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    except Exception as e:
        log(f"❌ Connection Error: {e}")
        return

    log("Reading sheets and mapping URLs...")
    all_data_rows = sh_data.get_all_values()
    stock_list_raw = sh_main.get_all_values()
    
    # Map Symbol -> URL from STOCKLIST 2
    url_map = {row[0].strip(): row[3].strip() for row in stock_list_raw if len(row) > 3}

    driver = create_driver()
    
    try:
        for i, row in enumerate(all_data_rows):
            row_num = i + 1
            if i == 0: continue # Skip header
            
            symbol = row[NAME_COL-1].strip()
            # Handle rows that might not have a status column yet
            status = row[STATUS_COL-1] if len(row) >= STATUS_COL else "EMPTY"
            
            # Target everything that is NOT marked "OK"
            if status != "OK":
                url = url_map.get(symbol)
                if not url or "http" not in url:
                    continue

                log(f"🛠️ Attempting Fix [{row_num}] {symbol}")
                new_vals = aggressive_scrape(driver, url)

                if len(new_vals) >= EXPECTED_COUNT:
                    # Batch the update to status and data
                    sh_data.update(f"{DATA_START_COL}{row_num}:{DATA_END_COL}{row_num}", [new_vals])
                    sh_data.update_cell(row_num, STATUS_COL, "OK")
                    sh_data.update_cell(row_num, URL_COL, url)
                    log(f"✅ Row {row_num} Fixed.")
                else:
                    log(f"⚠️ Row {row_num} still incomplete ({len(new_vals)} values)")
                
                time.sleep(random.uniform(1, 3))

            # Periodic browser refresh
            if row_num % 20 == 0:
                driver.quit()
                driver = create_driver()

    except Exception as e:
        log(f"❌ Cleanup Loop Error: {e}")
    finally:
        if driver:
            driver.quit()
        log("🏁 Cleanup Process Finished.")

if __name__ == "__main__":
    run_cleanup()
