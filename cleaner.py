import time
import json
import random
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
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# Column Indexing for MV2 DAY (1-based for gspread)
NAME_COL = 1        # Col A
STATUS_COL = 29     # Col AC
URL_COL = 30        # Col AD
DATA_START_COL = "C"
DATA_END_COL = "AB"

# ---------------- DRIVER ---------------- #
def create_driver():
    log("🌐 Initializing High-Intensity Cleanup Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
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
        # Zoom out to ensure all lazy elements are triggered
        drv.execute_script("document.body.style.zoom='50%'")
        
        # Long wait for initial render
        wait = WebDriverWait(drv, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='value']")))
        
        vals = []
        # Multi-stage scrolling and checking
        for i in range(5):
            vals = get_values_js(drv)
            if len(vals) >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT]
            
            drv.execute_script(f"window.scrollTo(0, {(i+1)*400});")
            time.sleep(3) # Heavy sleep to ensure JS execution
            
        return vals
    except Exception as e:
        log(f"   ❌ Scrape Error: {str(e)[:50]}")
        return []

# ---------------- MAIN CLEANUP ---------------- #
def run_cleanup():
    # Connect to Sheets
    gc = gspread.service_account("credentials.json")
    try:
        sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
        sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    except Exception as e:
        log(f"❌ Connection Error: {e}")
        return

    # 1. Load Data
    log("Reading sheets...")
    all_data_rows = sh_data.get_all_values()
    # Create a mapping of Symbol -> URL from STOCKLIST 2 for safety
    stock_list_raw = sh_main.get_all_values()
    url_map = {row[0].strip(): row[3].strip() for row in stock_list_raw if len(row) > 3}

    driver = create_driver()
    
    try:
        for i, row in enumerate(all_data_rows):
            row_num = i + 1
            if i == 0: continue # Skip header
            
            symbol = row[NAME_COL-1].strip()
            status = row[STATUS_COL-1] if len(row) >= STATUS_COL else ""
            
            # CHECK: If status is not OK, we process it
            if status != "OK":
                url = url_map.get(symbol)
                if not url or "http" not in url:
                    log(f"⏩ Skipping [{row_num}] {symbol}: No URL found in Stocklist")
                    continue

                log(f"🛠️ Fixing Row {row_num}: {symbol}")
                new_vals = aggressive_scrape(driver, url)

                if len(new_vals) >= EXPECTED_COUNT:
                    # Update Data Range
                    sh_data.update(f"{DATA_START_COL}{row_num}:{DATA_END_COL}{row_num}", [new_vals])
                    # Update Status and Browser URL
                    sh_data.update_cell(row_num, STATUS_COL, "OK")
                    sh_data.update_cell(row_num, URL_COL, url)
                    log(f"✅ Row {row_num} recovered successfully.")
                else:
                    log(f"⚠️ Row {row_num} failed again ({len(new_vals)} found).")
                
                # Random cooldown to prevent detection during cleanup
                time.sleep(random.uniform(2, 4))

            # Restart driver every 15 fixes to keep it fresh
            if row_num % 15 == 0:
                driver.quit()
                driver = create_driver()

    except KeyboardInterrupt:
        log("🛑 Cleanup stopped by user.")
    finally:
        if driver:
            driver.quit()
        log("🏁 Cleanup Process Finished.")

if __name__ == "__main__":
    run_cleanup()
