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
    driver.set_page_load_timeout(120)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(5)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try: driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            driver.refresh()
            time.sleep(5)
        except: pass
    return driver

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"    📡 Navigating {url_type}...")
    try:
        driver.get(url)
        # Dynamic wait for the specific 'value' elements to appear
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")) 
            or EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'valueValue')]"))
        )
        time.sleep(2) 
        
        for check in range(3): 
            soup = BeautifulSoup(driver.page_source, "html.parser")
            # Find all divs where class name starts with 'valueValue'
            raw_elements = soup.find_all("div", class_=lambda x: x and 'valueValue' in x)
            final_values = [el.get_text().strip() for el in raw_elements if el.get_text().strip()]
            
            if final_values:
                log(f"    📊 {url_type} Found {len(final_values)} values: {final_values[:3]}...")
                return final_values
            
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(2)
            
    except Exception as e:
        log(f"    ❌ {url_type} ERROR: {str(e)[:60]}")
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
batch_updates = []
BATCH_LIMIT = 50 
current_date = date.today().strftime("%m/%d/%Y")

try:
    for i in range(last_i, min(END_ROW, len(company_list))):
        name = company_list[i].strip()
        if not name: continue
        
        log(f"--- [ROW {i+1}] {name} ---")

        u_d = url_d_list[i] if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i] if i < len(url_h_list) and url_h_list[i].startswith("http") else None
        
        vals_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        
        # Build one complete row: A=Name, J=Date, K+=Values
        # Col A (1), Col J (10), Col K (11)
        # We need 8 empty strings between A and J
        full_row_data = [name] + ([""] * 8) + [current_date] + combined
        
        batch_updates.append({
            'range': f'A{row_idx}', 
            'values': [full_row_data]
        })
        
        if len(batch_updates) >= BATCH_LIMIT:
            log(f"🚀 Saving batch of {len(batch_updates)} stocks...")
            try:
                sheet_data.batch_update(batch_updates, value_input_option='USER_ENTERED')
                batch_updates = []
            except Exception as e:
                log(f"❌ Batch Save Failed: {e}")
                time.sleep(5) # Cooldown on error

        # Save checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        
        # Small delay to keep TradingView happy
        time.sleep(random.uniform(1, 2))

finally:
    if batch_updates:
        log(f"📤 Final Flush: Uploading last {len(batch_updates)} items...")
        try:
            sheet_data.batch_update(batch_updates, value_input_option='USER_ENTERED')
        except Exception as e:
            log(f"❌ Final Flush Failed: {e}")
    
    if driver: 
        driver.quit()
    log("🏁 Shard Completed.")
