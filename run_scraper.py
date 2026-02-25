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
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ✅ Speed: resolve chromedriver path ONCE (restart will reuse it)
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()

    # ✅ Faster: don't wait for full page assets
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    # ✅ speed/consistency flags
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

    # ✅ Hard timeouts so it never “hangs”
    driver.set_page_load_timeout(12)
    driver.set_script_timeout(12)

    # ---- COOKIE LOGIC ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(0.6)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue

            driver.refresh()
            time.sleep(0.4)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:120]}")

    return driver

# ---------------- STUCK-SAFE NAVIGATION ---------------- #
def safe_get(driver, url):
    try:
        driver.get(url)
        return True
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except:
            pass
        return False
    except WebDriverException as e:
        log(f"🛑 WebDriver error on get: {str(e)[:120]}")
        return "RESTART"

# ---------------- SCRAPER LOGIC ---------------- #
# ✅ Same XPATH target, but ensures value list is fully loaded (fixes "18 values only")
def scrape_tradingview(driver, url):
    try:
        ok = safe_get(driver, url)
        if ok == "RESTART":
            return "RESTART"
        if ok is False:
            return []

        WebDriverWait(driver, 14).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )

        # ✅ Wait for values count to "stabilize" (fast + reliable)
        last_count = -1
        stable_hits = 0
        for _ in range(6):
            els = driver.find_elements(By.CSS_SELECTOR, "div.valueValue-l31H9iuA.apply-common-tooltip")
            c = len(els)
            if c == last_count and c > 0:
                stable_hits += 1
                if stable_hits >= 2:
                    break
            else:
                stable_hits = 0
                last_count = c
            time.sleep(0.25)

        els = driver.find_elements(By.CSS_SELECTOR, "div.valueValue-l31H9iuA.apply-common-tooltip")
        values = []
        for el in els:
            t = (el.text or "").strip()

            # ✅ No "None" / "∅" in sheet — keep blank instead
            if not t or t == "∅" or t.lower() == "none":
                t = ""

            t = t.replace("−", "-")
            values.append(t)

        # Trim trailing blanks
        while values and values[-1] == "":
            values.pop()

        return values

    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

def scrape_with_recovery(driver, url):
    values = scrape_tradingview(driver, url)

    if values == "RESTART":
        try:
            driver.quit()
        except:
            pass
        driver = create_driver()
        values = scrape_tradingview(driver, url)
        if values == "RESTART":
            values = []

    return driver, values

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(4)  # URLs in D
    url_h_list   = sheet_main.col_values(8)  # URLs in H
    name_list    = sheet_main.col_values(1)  # Names

    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {len(company_list)}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []

# Keep big buffer for speed (override with env var if needed)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "300"))

current_date = date.today().strftime("%m/%d/%Y")
ROW_SLEEP = 0

def flush_batch():
    global batch_list
    if not batch_list:
        return
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ API Error: {msg[:160]}")
            if "429" in msg:
                log("⏳ Quota hit, sleeping 60s...")
                time.sleep(60)
            else:
                time.sleep(2)

try:
    for i in range(last_i, len(company_list)):

        if i % SHARD_STEP != SHARD_INDEX:
            continue

        url_d = (company_list[i] or "").strip()
        url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
        name  = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        # Skip only if BOTH invalid
        if (not url_d.startswith("http")) and (not url_h.startswith("http")):
            log(f"⏭️ Row {i+1}: invalid/blank URLs in D & H -> skipped")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        # If H == D, don’t scrape twice
        if url_h and url_d and url_h == url_d:
            url_h = ""

        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name}")

        all_values = []

        # ---- scrape D ----
        values_d = []
        if url_d.startswith("http"):
            driver, values_d = scrape_with_recovery(driver, url_d)
            if isinstance(values_d, list) and values_d:
                all_values.extend(values_d)

        # ---- scrape H and APPEND after D ----
        values_h = []
        if url_h.startswith("http"):
            driver, values_h = scrape_with_recovery(driver, url_h)
            if isinstance(values_h, list) and values_h:
                all_values.extend(values_h)

        # ✅ debug counts (so you can verify H is being appended)
        log(f"   D values: {len(values_d) if isinstance(values_d, list) else 0} | H values: {len(values_h) if isinstance(values_h, list) else 0} | Total: {len(all_values)}")

        target_row = i + 1

        # Always write A + J
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})

        # Write K+ only if values exist
        if isinstance(all_values, list) and all_values:
            batch_list.append({"range": f"K{target_row}", "values": [all_values]})
        else:
            log(f"⚠️ No values found for {name} (A/J updated only)")

        if len(batch_list) >= BATCH_SIZE:
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush_batch()
    try:
        driver.quit()
    except:
        pass
    log("🏁 Scraping completed successfully")
