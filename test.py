import sys
import os
import time
import json
import random
from datetime import date
from selenium import webdriver
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

# always create files
open(checkpoint_file, "a").close()
open(FAILED_FILE, "a").close()

# checkpoint: DONE:<i> / INPROG:<i>
def read_checkpoint():
    try:
        raw = open(checkpoint_file, "r").read().strip()
        if raw.startswith("DONE:"):
            return int(raw.split("DONE:")[1].strip()) + 1
        if raw.startswith("INPROG:"):
            return int(raw.split("INPROG:")[1].strip())
        if raw == "":
            return 0
        return int(raw)
    except:
        return 0

def write_checkpoint(tag, i):
    try:
        with open(checkpoint_file, "w") as f:
            f.write(f"{tag}:{i}")
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

# ---------------- DRIVER (ONE TIME) ---------------- #
def build_driver():
    log("🌐 Initializing Hardened Chrome Instance...")

    chrome_bin = os.getenv("CHROME_BINARY", "/usr/bin/google-chrome")

    opts = Options()
    if os.path.exists(chrome_bin):
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
    opts.add_argument("--remote-debugging-port=9222")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(45)
    return driver

def apply_cookies_once(driver):
    # apply cookies ONCE only
    if not os.path.exists("cookies.json"):
        log("⚠️ cookies.json not found (running without cookies)")
        return False

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
        return ok > 0
    except Exception as e:
        log(f"⚠️ Cookie error: {str(e)[:160]}")
        return False

def soft_recover(driver):
    # NO cookie reinjection, just reset to home
    try:
        driver.get("https://in.tradingview.com/")
        time.sleep(1.5)
        driver.refresh()
        time.sleep(1.0)
    except:
        pass

# ---------------- SCRAPER LOGIC ---------------- #
XPATH_WAIT = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
VALUE_CLASS = "valueValue-l31H9iuA apply-common-tooltip"

def scrape_tradingview(driver, url):
    try:
        driver.get(url)

        WebDriverWait(driver, 35).until(
            EC.visibility_of_element_located((By.XPATH, XPATH_WAIT))
        )

        html = driver.page_source.lower()
        # block detection (kept same idea)
        if "captcha" in html or "access denied" in html or "sign in" in html:
            return "RESTART"

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_=VALUE_CLASS)
        ]
        return values

    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

def scrape_with_retries(driver, url, tries=5):
    delay = 0.8
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

        time.sleep(delay + random.uniform(0.1, 0.6))
        delay = min(delay * 1.6, 8)

    return []

def last3(values):
    return values[-3:] if isinstance(values, list) and len(values) >= 3 else []

# ---------------- SHEETS SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet16")

    company_list_c = sheet_main.col_values(3)  # C
    company_list_d = sheet_main.col_values(4)  # D
    name_list = sheet_main.col_values(1)       # A

    total_rows = max(len(company_list_c), len(company_list_d), len(name_list))
    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {total_rows}")

except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
BATCH_SIZE = 300
current_date = date.today().strftime("%m/%d/%Y")
ROW_SLEEP = 0.03

batch_list = []

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
                sleep_s = 45 + (SHARD_INDEX * 5)
                log(f"⏳ Quota hit, sleeping {sleep_s}s...")
                time.sleep(sleep_s)
            else:
                time.sleep(2 + attempt)

driver = None
consec_restarts = 0
MAX_CONSEC_RESTARTS = 3

try:
    driver = build_driver()
    apply_cookies_once(driver)

    for i in range(last_i, total_rows):
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        write_checkpoint("INPROG", i)

        url_c = (company_list_c[i] or "").strip() if i < len(company_list_c) else ""
        url_d = (company_list_d[i] or "").strip() if i < len(company_list_d) else ""
        name  = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        if not (url_c.startswith("http") or url_d.startswith("http")):
            log(f"⏭️ Row {i+1}: invalid/blank URL in both C/D -> skipped")
            mark_failed(i, "BAD_URL_BOTH")
            write_checkpoint("DONE", i)
            continue

        log(f"🔍 [{i+1}/{total_rows}] Scraping: {name}")

        # -------- scrape C --------
        values_c = []
        if url_c.startswith("http"):
            v = scrape_with_retries(driver, url_c, tries=5)
            if v == "RESTART":
                consec_restarts += 1
                soft_recover(driver)
                values_c = []
            else:
                consec_restarts = 0
                values_c = v
        else:
            mark_failed(i, "BAD_URL_C")

        # -------- scrape D --------
        values_d = []
        if url_d.startswith("http"):
            v = scrape_with_retries(driver, url_d, tries=5)
            if v == "RESTART":
                consec_restarts += 1
                soft_recover(driver)
                values_d = []
            else:
                consec_restarts = 0
                values_d = v
        else:
            mark_failed(i, "BAD_URL_D")

        # if too many restarts, do ONE hard restart (cookies not re-added every time)
        if consec_restarts >= MAX_CONSEC_RESTARTS:
            log("♻️ Too many RESTART signals, restarting browser once...")
            try:
                driver.quit()
            except:
                pass
            driver = build_driver()
            apply_cookies_once(driver)
            consec_restarts = 0

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

        write_checkpoint("DONE", i)

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
