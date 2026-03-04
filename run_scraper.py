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

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "25")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

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
    driver.set_page_load_timeout(60)

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
            time.sleep(3)
            log("✅ Cookies applied.")
        except: 
            log("⚠️ Cookie error (ignored).")
    return driver

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 Shard {SHARD_INDEX} | {url_type} | Navigating to: {url}")
    for attempt in range(3):
        try:
            driver.get(url)
            try:
                WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']")))
            except TimeoutException:
                log(f"   ⏳ Timed out waiting for UI elements on {url_type}, attempting scrape anyway...")

            time.sleep(12) # JS Render Wait
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            v3 = [el.text.strip() for el in driver.find_elements(By.XPATH, "//div[contains(@class, 'value') and contains(@class, 'Value')]")]
            
            raw_values = v1 or v2 or v3
            final_values = [str(v) for v in raw_values if v is not None]
            
            if final_values:
                # Log a preview of the values found
                preview = ", ".join(final_values[:5]) + ("..." if len(final_values) > 5 else "")
                log(f"   ✅ {url_type} SUCCESS: Captured {len(final_values)} values: [{preview}]")
                return final_values
            else:
                log(f"   ⚠️ {url_type} EMPTY: No values found. Attempt {attempt+1}/3. Refreshing...")
                driver.refresh()
                time.sleep(5)
        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:100]}")
            time.sleep(3)
    return []

# ---------------- ANTI-COLLISION STARTUP ---------------- #
startup_wait = random.uniform(2, 45)
log(f"⏳ Anti-collision: Shard {SHARD_INDEX} staggered start. Waiting {startup_wait:.1f}s...")
time.sleep(startup_wait)

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    company_list, url_d_list, url_h_list = [], [], []
    for attempt in range(3):
        try:
            company_list = sheet_main.col_values(1)
            url_d_list = sheet_main.col_values(4)
            url_h_list = sheet_main.col_values(8)
            log(f"✅ Sheet Ready: {len(company_list)} tickers loaded.")
            break
        except Exception as e:
            log(f"⚠️ Read Error: {e}. Retrying...")
            time.sleep(15)
            
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
BATCH_SIZE = 100 
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    jitter = random.uniform(2, 12)
    log(f"🚀 [Shard {SHARD_INDEX}] BUFFER FULL ({len(batch_list)//3}/{BATCH_SIZE} rows). Waiting {jitter:.1f}s jitter...")
    time.sleep(jitter) 
    for attempt in range(5):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log(f"✅ [Shard {SHARD_INDEX}] SUCCESS: Google Sheets updated.")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            wait = 60 if "429" in msg else 10
            log(f"⚠️ Sheets API Busy. Retrying in {wait}s...")
            time.sleep(wait + random.uniform(1, 5))

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] Processing: {name} ---")

        active_driver = ensure_driver()
        
        u_d = url_d_list[i] if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i] if i < len(url_h_list) and url_h_list[i].startswith("http") else None
        
        vals_d = scrape_tradingview(active_driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(active_driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_idx}", "values": [combined]})
            log(f"   📦 Added to Buffer: {len(combined)} values total.")
        
        # Immediate buffer progress log
        log(f"📊 SHARD {SHARD_INDEX} PROGRESS: {len(batch_list)//3}/{BATCH_SIZE} rows in memory.")

        if len(batch_list) >= (BATCH_SIZE * 3):
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        
        time.sleep(1.5)

except Exception as e:
    log(f"❌ CRITICAL ERROR: {e}")

finally:
    if batch_list:
        log("📦 FINAL FLUSH: Saving remaining items before exit...")
        flush_batch()
    if driver: 
        driver.quit()
    log(f"🏁 SHARD {SHARD_INDEX} FINISHED.")
