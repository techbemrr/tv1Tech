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

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500")) 
checkpoint_file = f"checkpoint_{SHARD_INDEX}.txt"

# ---------------- BROWSER (High Performance) ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Launching Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Optimize: Disable images
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)
    return driver

# ---------------- SCRAPER (Your Paths + Performance) ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 {url_type}: {url}")
    for attempt in range(1, 3):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 45)
            
            # Wait for your specific element to exist
            target_css = "[class*='valueValue']"
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, target_css)))
            
            # Scroll to trigger rendering
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")

            # Mandatory render wait for calculations to finish
            time.sleep(20) 

            # --- YOUR EXACT EXTRACTION PATHS ---
            soup = BeautifulSoup(driver.page_source, "html.parser")
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            v3 = [el.text.strip() for el in driver.find_elements(By.XPATH, "//div[contains(@class, 'value') and contains(@class, 'Value')]")]
            
            raw_values = v1 or v2 or v3
            # Clean non-printable characters and placeholders
            final_values = [str(v) for v in raw_values if v and v not in ["∅", "—"]]
            
            if len(final_values) > 5:
                # Logging exactly what it got
                preview = ", ".join(final_values[:15])
                log(f"   ✅ {url_type} SUCCESS: Found {len(final_values)} values.")
                log(f"   📊 CAPTURED: [{preview}...]")
                return final_values
            else:
                log(f"   ⏳ Attempt {attempt}: Only placeholders found. Refreshing...")
                driver.refresh()
                time.sleep(5)
        except Exception as e:
            log(f"   ❌ Scrape Error: {str(e)[:50]}")
    return []

# ---------------- DATA SETUP ---------------- #
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

# ---------------- RANGE LOGIC ---------------- #
start_idx = SHARD_INDEX * SHARD_SIZE
end_idx = min(start_idx + SHARD_SIZE, len(company_list))

if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        val = f.read().strip()
        current_idx = int(val) if val.isdigit() else start_idx
else:
    current_idx = start_idx

current_idx = max(start_idx, current_idx)
log(f"🚀 Processing Rows {start_idx+1} to {end_idx}")

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

try:
    for i in range(current_idx, end_idx):
        symbol = company_list[i].strip()
        if not symbol or symbol.lower() == "symbol": continue

        log(f"--- [Row {i+1}] {symbol} ---")
        
        u_d = url_d_list[i] if i < len(url_d_list) and "http" in str(url_d_list[i]) else None
        u_h = url_h_list[i] if i < len(url_h_list) and "http" in str(url_h_list[i]) else None
        
        data_d = scrape_tradingview(driver, u_d, "DAILY") if u_d else []
        data_h = scrape_tradingview(driver, u_h, "HOURLY") if u_h else []
        combined = data_d + data_h

        row_num = i + 1
        batch_list.append({"range": f"A{row_num}", "values": [[symbol]]})
        batch_list.append({"range": f"J{row_num}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_num}", "values": [combined]})
        
        # Save every 10 Symbols (30 updates)
        if len(batch_list) >= 30:
            log("💾 Saving batch...")
            try:
                sheet_data.batch_update(batch_list, value_input_option='RAW')
                batch_list = []
                with open(checkpoint_file, "w") as f: f.write(str(i + 1))
                log("✅ Progress Saved.")
            except Exception as e:
                log(f"⚠️ Sheets Error: {e}")
        
        time.sleep(1)

finally:
    if batch_list:
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log("✅ Final batch saved.")
        except: pass
    driver.quit()
    log("🏁 Shard Completed.")
