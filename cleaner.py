import time
import json
import random
from datetime import date
import gspread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
EXPECTED_COUNT = 22
DAY_OUTPUT_START_COL = 3
COOKIE_FILE = "cookies.json"
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOG ---------------- #
def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- COLUMN UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

def col_letter_to_num(col):
    num = 0
    for c in col:
        num = num * 26 + (ord(c.upper()) - ord('A') + 1)
    return num

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)
STATUS_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT)
SHEET_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1)
BROWSER_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2)

# ---------------- API RETRY ---------------- #
def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API retry in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log("🌐 Starting browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

    try:
        drv.get("https://in.tradingview.com/")
        with open(COOKIE_FILE, "r") as f:
            cookies = json.load(f)
        for c in cookies:
            drv.add_cookie({k: v for k, v in c.items() if k in ("name","value","path","secure","expiry")})
        drv.refresh()
        time.sleep(2)
    except:
        pass

    return drv

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try:
            driver.quit()
        except:
            pass
    driver = None

# ---------------- SCRAPER ---------------- #
def get_values(drv):
    elements = drv.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")
    return [el.text.strip() for el in elements if el.text.strip()]

def scrape_day(url):
    if not url:
        return [""] * EXPECTED_COUNT, "NOT OK", "", ""

    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)

            WebDriverWait(drv, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']"))
            )

            time.sleep(3)
            vals = get_values(drv)

            if len(vals) < EXPECTED_COUNT:
                for y in [600, 1200, 2000]:
                    drv.execute_script(f"window.scrollTo(0, {y});")
                    time.sleep(1.5)
                    new_vals = get_values(drv)
                    if len(new_vals) > len(vals):
                        vals = new_vals
                    if len(vals) >= EXPECTED_COUNT:
                        break

            browser_url = drv.current_url
            count = len(vals)

            if count >= EXPECTED_COUNT:
                log(f"   ✅ Found {count}/{EXPECTED_COUNT}")
                return vals[:EXPECTED_COUNT], "OK", url, browser_url
            else:
                log(f"   ⚠️ Found {count}/{EXPECTED_COUNT}")
                padded = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                return padded, "NOT OK", url, browser_url

        except:
            log(f"   ❌ Attempt {attempt+1} failed, restarting browser...")
            restart_driver()

    return [""] * EXPECTED_COUNT, "NOT OK", url, ""

# ---------------- SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

# ---------------- FIND NOT OK ---------------- #
def find_not_ok_rows(sheet_data, company_list):
    log("🔍 Scanning for NOT OK rows...")

    status_values = api_retry(
        sheet_data.col_values,
        col_letter_to_num(STATUS_COL)
    )

    indices = []

    for i in range(1, len(status_values)):  # skip header
        if status_values[i].strip().upper() == "NOT OK":
            sheet_row = i + 1  # actual row number
            name = company_list[i].strip() if i < len(company_list) else "UNKNOWN"

            log(f"❌ Found NOT OK → Row {sheet_row} | {name}")
            indices.append(sheet_row)

    log(f"⚠️ Total NOT OK rows: {len(indices)}")
    return indices

# ---------------- PROCESS ---------------- #
def process_row(sheet_row, company_list, url_list, current_date):
    idx = sheet_row - 1  # convert to list index

    name = company_list[idx].strip()
    url = url_list[idx].strip() if "http" in url_list[idx] else None

    log(f"🚀 Processing → Row {sheet_row} | {name}")

    vals, status, sheet_url, browser_url = scrape_day(url)

    filled = sum(1 for v in vals if v.strip())
    log(f"📊 Result → {name} | {filled}/{EXPECTED_COUNT} | {status}")

    return [
        {"range": f"A{sheet_row}", "values": [[name]]},
        {"range": f"B{sheet_row}", "values": [[current_date]]},
        {"range": f"{DAY_START_COL_LETTER}{sheet_row}:{DAY_END_COL_LETTER}{sheet_row}", "values": [vals]},
        {"range": f"{STATUS_COL}{sheet_row}", "values": [[status]]},
        {"range": f"{SHEET_URL_COL}{sheet_row}", "values": [[sheet_url]]},
        {"range": f"{BROWSER_URL_COL}{sheet_row}", "values": [[browser_url]]}
    ]

# ---------------- MAIN ---------------- #
def main():
    sheet_main, sheet_data = connect_sheets()

    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)

    not_ok_rows = find_not_ok_rows(sheet_data, company_list)

    if not not_ok_rows:
        log("✅ No NOT OK rows found")
        return

    restart_driver()
    batch = []
    current_date = date.today().strftime("%m/%d/%Y")

    total = len(not_ok_rows)

    for idx, row in enumerate(not_ok_rows):
        log(f"🔄 Progress: {idx+1}/{total}")

        batch.extend(process_row(row, company_list, url_list, current_date))

        if (idx + 1) % 10 == 0:
            restart_driver()
            log("🚀 Uploading batch...")
            api_retry(sheet_data.batch_update, batch, value_input_option="RAW")
            batch = []

    if batch:
        log("🚀 Final upload...")
        api_retry(sheet_data.batch_update, batch, value_input_option="RAW")

    restart_driver()
    log("🏁 CLEANER COMPLETED SUCCESSFULLY")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    main()
