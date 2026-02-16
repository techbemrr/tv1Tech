import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ================= CONFIG =================
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

CHROME_DRIVER_PATH = ChromeDriverManager().install()

BATCH_SIZE = 50
CHECKPOINT_EVERY = 10
ROW_SLEEP = 0.05
FILLER = "NA"

# ================= HELPERS =================
def clean_cell_text(s):
    return "".join(str(s).split())

def clean_list(values):
    return [clean_cell_text(v) for v in values if str(v).strip()]

def last_three(values):
    vals = clean_list(values)
    return vals[-3:] if len(vals) >= 3 else vals

def pad(vals, n=3):
    vals = list(vals)
    while len(vals) < n:
        vals.append(FILLER)
    return vals[:n]

def safe_get(lst, idx):
    return (lst[idx] if idx < len(lst) else "").strip()

# ================= BROWSER =================
def create_driver():
    opts = Options()
    opts.page_load_strategy = "eager"
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(40)
    return driver

# ================= SCRAPER =================
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
            '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        return [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        return "RESTART"

def scrape_with_retry(driver, url):
    values = scrape_tradingview(driver, url)
    if values == []:
        try:
            driver.refresh()
            time.sleep(0.7)
        except:
            pass
        values = scrape_tradingview(driver, url)
    return values

# ================= SHEETS =================
log("📊 Connecting to Google Sheets...")
gc = gspread.service_account("credentials.json")
sheet_main = gc.open("Stock List").worksheet("Sheet1")
sheet_data = gc.open("MV2 for SQL").worksheet("Sheet16")

names = sheet_main.col_values(1)
urls_c = sheet_main.col_values(3)
urls_d = sheet_main.col_values(4)

total_rows = max(len(names), len(urls_c), len(urls_d))

if sheet_data.row_count < total_rows + 5:
    sheet_data.resize(rows=total_rows + 5)

# ================= MAIN =================
driver = create_driver()
batch = []
rows_buffered = 0
total_flushes = 0
last_checkpoint_written = last_i
today = date.today().strftime("%m/%d/%Y")

def flush():
    global batch, rows_buffered, total_flushes
    if not batch:
        return
    sheet_data.batch_update(batch)
    batch.clear()
    rows_buffered = 0
    total_flushes += 1
    log(f"🚀 FLUSH OK | FlushCount={total_flushes}")

try:
    for i in range(last_i, total_rows):
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        name = safe_get(names, i)
        url_c = safe_get(urls_c, i)
        url_d = safe_get(urls_d, i)
        row = i + 1

        values_c = scrape_with_retry(driver, url_c) if url_c.startswith("http") else []
        values_d = scrape_with_retry(driver, url_d) if url_d.startswith("http") else []

        c_vals = pad(last_three(values_c))
        d_vals = pad(last_three(values_d))

        # A=name, B=date, C-H=values
        batch.append({"range": f"A{row}", "values": [[name]]})
        batch.append({"range": f"B{row}", "values": [[today]]})
        batch.append({"range": f"C{row}:H{row}", "values": [c_vals + d_vals]})

        rows_buffered += 1

        if len(batch) >= BATCH_SIZE:
            flush()

        if (row - last_checkpoint_written) >= CHECKPOINT_EVERY:
            with open(checkpoint_file, "w") as f:
                f.write(str(row))
            last_checkpoint_written = row

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush()
    try:
        driver.quit()
    except:
        pass

    log(f"🏁 DONE | TotalFlushes={total_flushes}")
