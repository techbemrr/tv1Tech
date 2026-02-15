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
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
FAILED_FILE = os.getenv("FAILED_FILE", f"failed_{SHARD_INDEX}.txt")

# ✅ ensure files exist ALWAYS (so artifact upload works)
try:
    open(checkpoint_file, "a").close()
    open(FAILED_FILE, "a").close()
except:
    pass

# ✅ Proper checkpoint format:
# DONE:<index>   => last fully completed index
# INPROG:<index> => currently working index (not completed)
def read_checkpoint():
    if not os.path.exists(checkpoint_file):
        return 0
    try:
        raw = open(checkpoint_file, "r").read().strip()
        if raw.startswith("DONE:"):
            return int(raw.split("DONE:")[1].strip()) + 1
        if raw.startswith("INPROG:"):
            return int(raw.split("INPROG:")[1].strip())
        if raw == "":
            return 0
        return int(raw)  # legacy fallback
    except:
        return 0

def write_checkpoint_inprog(i):
    try:
        with open(checkpoint_file, "w") as f:
            f.write(f"INPROG:{i}")
    except:
        pass

def write_checkpoint_done(i):
    try:
        with open(checkpoint_file, "w") as f:
            f.write(f"DONE:{i}")
    except:
        pass

last_i = read_checkpoint()

_failed_seen = set()
def mark_failed(i, reason="NO_VALUES"):
    if i in _failed_seen:
        return
    _failed_seen.add(i)
    try:
        with open(FAILED_FILE, "a") as f:
            f.write(f"{i}|{reason}\n")
    except:
        pass

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")

    chrome_bin = os.getenv("CHROME_BINARY", "/usr/bin/chromium")
    driver_bin = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")

    # show paths in logs to debug instantly
    log(f"🔧 CHROME_BINARY={chrome_bin}")
    log(f"🔧 CHROMEDRIVER_PATH={driver_bin}")

    if not os.path.exists(chrome_bin):
        raise RuntimeError(f"Chrome binary not found: {chrome_bin}")
    if not os.path.exists(driver_bin):
        raise RuntimeError(f"ChromeDriver not found: {driver_bin}")

    opts = Options()
    opts.binary_location = chrome_bin
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")

    # ✅ helps prevent hang on GH Actions
    opts.add_argument("--remote-debugging-port=9222")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    service = Service(driver_bin)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(45)

    # ---- COOKIE LOGIC (safe + domain forced) ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)

            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            ok = 0
            for c in cookies:
                try:
                    cc = {
                        "name": c.get("name"),
                        "value": c.get("value"),
                        "path": c.get("path", "/"),
                        "secure": bool(c.get("secure", False)),
                        "httpOnly": bool(c.get("httpOnly", False)),
                        "domain": "in.tradingview.com",
                    }
                    if "expiry" in c:
                        try:
                            cc["expiry"] = int(c["expiry"])
                        except:
                            pass
                    driver.add_cookie(cc)
                    ok += 1
                except:
                    continue

            driver.refresh()
            time.sleep(1)
            log(f"✅ Cookies applied successfully ({ok}/{len(cookies)})")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:160]}")
    else:
        log("⚠️ cookies.json not found (running without cookies)")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
# ✅ DO NOT CHANGE: your exact XPATH + your exact class parsing kept as-is
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )

        html = driver.page_source.lower()
        if "captcha" in html or "access denied" in html or "sign in" in html:
            return "RESTART"

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

def scrape_with_retries(driver, url, tries=7):
    delay = 1.0
    for _ in range(tries):
        values = scrape_tradingview(driver, url)

        if values == "RESTART":
            return "RESTART"

        if isinstance(values, list) and values:
            return values

        try:
            driver.refresh()
        except:
            pass

        time.sleep(delay + random.uniform(0.1, 0.7))
        delay = min(delay * 1.7, 12)

    return []

def last3(values):
    if isinstance(values, list) and len(values) >= 3:
        return values[-3:]
    return []

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")

    # ✅ store to Sheet16
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet16")

    # ✅ read BOTH column C and D
    company_list_c = sheet_main.col_values(3)  # C
    company_list_d = sheet_main.col_values(4)  # D
    name_list = sheet_main.col_values(1)       # A

    total_rows = max(len(company_list_c), len(company_list_d), len(name_list))
    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {total_rows}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []

BATCH_SIZE = 300
current_date = date.today().strftime("%m/%d/%Y")
ROW_SLEEP = 0.05

def flush_batch():
    global batch_list
    if not batch_list:
        return
    for attempt in range(5):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ API Error: {msg[:160]}")
            if "429" in msg or "quota" in msg.lower() or "rate" in msg.lower():
                sleep_s = 60 + (SHARD_INDEX * 7)
                log(f"⏳ Quota hit, sleeping {sleep_s}s...")
                time.sleep(sleep_s)
            else:
                time.sleep(3 + attempt)

try:
    driver = create_driver()

    for i in range(last_i, total_rows):

        if i % SHARD_STEP != SHARD_INDEX:
            continue

        write_checkpoint_inprog(i)

        url_c = (company_list_c[i] or "").strip() if i < len(company_list_c) else ""
        url_d = (company_list_d[i] or "").strip() if i < len(company_list_d) else ""
        name  = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        if not (url_c.startswith("http") or url_d.startswith("http")):
            log(f"⏭️ Row {i+1}: invalid/blank URL in both C/D -> skipped")
            mark_failed(i, "BAD_URL_BOTH")
            write_checkpoint_done(i)
            continue

        log(f"🔍 [{i+1}/{total_rows}] Scraping: {name}")

        # ---- scrape C ----
        values_c = []
        if url_c.startswith("http"):
            values_c = scrape_with_retries(driver, url_c, tries=7)
            if values_c == "RESTART":
                try: driver.quit()
                except: pass
                driver = create_driver()
                values_c = scrape_with_retries(driver, url_c, tries=5)
                if values_c == "RESTART":
                    values_c = []
        else:
            mark_failed(i, "BAD_URL_C")

        # ---- scrape D ----
        values_d = []
        if url_d.startswith("http"):
            values_d = scrape_with_retries(driver, url_d, tries=7)
            if values_d == "RESTART":
                try: driver.quit()
                except: pass
                driver = create_driver()
                values_d = scrape_with_retries(driver, url_d, tries=5)
                if values_d == "RESTART":
                    values_d = []
        else:
            mark_failed(i, "BAD_URL_D")

        last3_c = last3(values_c)
        last3_d = last3(values_d)

        target_row = i + 1

        # always write name + date
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})

        # C -> K:M
        if last3_c:
            batch_list.append({"range": f"K{target_row}:M{target_row}", "values": [last3_c]})
        else:
            mark_failed(i, "NO_LAST3_C")

        # D -> N:P
        if last3_d:
            batch_list.append({"range": f"N{target_row}:P{target_row}", "values": [last3_d]})
        else:
            mark_failed(i, "NO_LAST3_D")

        log(f"✅ C_last3={len(last3_c)} | D_last3={len(last3_d)} | Buffered: {len(batch_list)}/{BATCH_SIZE}")

        if len(batch_list) >= BATCH_SIZE:
            flush_batch()

        write_checkpoint_done(i)

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush_batch()
    try:
        if driver:
            driver.quit()
    except:
        pass
    log("🏁 Scraping completed successfully")
    log(f"📌 Failed rows file: {FAILED_FILE}")
    log(f"📌 Checkpoint file: {checkpoint_file}")
