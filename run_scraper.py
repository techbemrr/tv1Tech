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

import gspread
from webdriver_manager.chrome import ChromeDriverManager


def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ---- ADDED: clearer errors + debug screenshots ---- #

def describe_error(e):
    # Selenium's TimeoutException has an empty message, which is why the
    # log only showed "Failed: Message:". Always include the error type.
    msg = str(e).strip().replace("\n", " ")
    return f"{type(e).__name__}: {msg[:150] if msg else '(no message)'}"


DEBUG_DIR = os.getenv("DEBUG_DIR", "debug")
MAX_DEBUG_SAVES = int(os.getenv("MAX_DEBUG_SAVES", "5"))
_debug_saves = 0


def save_debug(drv, label):
    # Saves what the browser actually loaded (first few failures only)
    global _debug_saves
    if drv is None or _debug_saves >= MAX_DEBUG_SAVES:
        return
    _debug_saves += 1
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        safe = "".join(ch if ch.isalnum() else "_" for ch in label)[:60]
        base = os.path.join(DEBUG_DIR, f"{_debug_saves:02d}_{safe}")
        log(f"   🧾 Page title: {drv.title!r} | URL: {drv.current_url}")
        drv.save_screenshot(base + ".png")
        with open(base + ".html", "w", encoding="utf-8") as f:
            f.write(drv.page_source)
        log(f"   🧾 Saved debug files: {base}.png / .html")
    except Exception as e:
        log(f"   ⚠️ Could not save debug files: {describe_error(e)}")


# ---------------- CONFIG ---------------- #

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))

START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

checkpoint_file = os.getenv(
    "CHECKPOINT_FILE",
    f"checkpoint_day_{SHARD_INDEX}.txt"
)

EXPECTED_COUNT = 18
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 20

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")

CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3

# NEW TradingView value class
VALUE_SELECTOR = ".valueValue-quatTGAC"


# ---------------- COLUMN UTILS ---------------- #

def col_num_to_letter(n):
    result = ""

    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result

    return result


DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)

DAY_END_COL_LETTER = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1
)

STATUS_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT
)

SHEET_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1
)

BROWSER_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2
)


# ---------------- API RETRY ---------------- #

def api_retry(func, *args, **kwargs):

    for attempt in range(5):

        try:
            return func(*args, **kwargs)

        except Exception as e:

            wait = (2 ** attempt) + random.random()

            log(
                f"⚠️ API Issue: {str(e)[:100]}. "
                f"Retrying in {wait:.1f}s..."
            )

            time.sleep(wait)

    return func(*args, **kwargs)


# ---------------- STATE ---------------- #

if os.path.exists(checkpoint_file):

    try:

        last_i = max(
            int(open(checkpoint_file).read().strip()),
            START_ROW
        )

    except Exception:

        last_i = START_ROW

else:

    last_i = START_ROW


# ---------------- DRIVER ---------------- #

driver = None


def create_driver():

    log(
        f"🌐 [Shard {SHARD_INDEX}] "
        f"Initializing browser..."
    )

    opts = Options()

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "--disable-blink-features=AutomationControlled"
    )

    # CHANGED: removed the hard-coded "Chrome/120.0.0.0" user agent.
    # Chrome 120 is years older than the Chrome GitHub installs; the real
    # version is set right after the browser starts instead.

    drv = webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )

    try:
        ua = drv.execute_script("return navigator.userAgent")
        ua = ua.replace("HeadlessChrome", "Chrome")
        drv.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {"userAgent": ua}
        )
        log(f"   🧭 Browser: {ua}")
    except Exception as e:
        log(f"   ⚠️ Could not set user agent: {describe_error(e)}")

    # ---------------- LOAD COOKIES ---------------- #

    if os.path.exists(COOKIE_FILE):

        try:

            drv.get("https://in.tradingview.com/")

            with open(
                COOKIE_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                cookies = json.load(f)

            # ADDED: cookie report (an expired login is a common
            # reason for values to disappear)
            added = failed = expired = 0
            now = time.time()

            for c in cookies:

                exp = c.get("expiry") or c.get("expirationDate")
                try:
                    if exp and float(exp) < now:
                        expired += 1
                except Exception:
                    pass


                cookie = {
                    k: v
                    for k, v in c.items()
                    if k in (
                        "name",
                        "value",
                        "path",
                        "secure",
                        "expiry"
                    )
                }

                try:
                    drv.add_cookie(cookie)
                    added += 1
                except Exception:
                    failed += 1

            log(
                f"   🍪 Cookies: {added} added, {failed} failed, "
                f"{expired} already expired (site: {drv.current_url})"
            )

            drv.refresh()

            time.sleep(2)

        except Exception as e:

            log(
                f"⚠️ Cookie loading failed: "
                f"{str(e)[:100]}"
            )

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

        except Exception:
            pass

    driver = None


# ---------------- SCRAPER ---------------- #

def get_values(drv):

    try:

        elements = drv.find_elements(
            By.CSS_SELECTOR,
            VALUE_SELECTOR
        )

        vals = [
            el.text.strip()
            for el in elements
            if el.text.strip()
        ]

        return vals

    except Exception as e:

        log(
            f"⚠️ Could not read values: "
            f"{str(e)[:100]}"
        )

        return []


def scrape_day(url):

    if not url:

        return (
            [""] * EXPECTED_COUNT,
            "NOT OK",
            "",
            ""
        )

    for attempt in range(2):

        try:

            drv = ensure_driver()

            log(
                f"   🌐 Opening URL "
                f"(attempt {attempt + 1})"
            )

            drv.get(url)

            # Wait for the NEW class
            WebDriverWait(
                drv,
                20
            ).until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        VALUE_SELECTOR
                    )
                )
            )

            # Allow page to finish rendering
            time.sleep(3)

            vals = get_values(drv)

            # ---------------- SCROLL CHECK ---------------- #

            if len(vals) < EXPECTED_COUNT:

                for scroll_y in [
                    600,
                    1200,
                    2000
                ]:

                    drv.execute_script(
                        f"window.scrollTo(0, {scroll_y});"
                    )

                    time.sleep(1.5)

                    new_vals = get_values(drv)

                    if len(new_vals) > len(vals):

                        vals = new_vals

                    if len(vals) >= EXPECTED_COUNT:

                        break

            browser_url = drv.current_url

            found_count = len(vals)

            # ---------------- RESULT ---------------- #

            if found_count >= EXPECTED_COUNT:

                log(
                    f"   ✅ Found "
                    f"{found_count}/{EXPECTED_COUNT}"
                )

                return (
                    vals[:EXPECTED_COUNT],
                    "OK",
                    url,
                    browser_url
                )

            else:

                log(
                    f"   ⚠️ Found "
                    f"{found_count}/{EXPECTED_COUNT} "
                    f"(Marking NOT OK)"
                )

                save_debug(drv, f"partial_{url[-40:]}")  # ADDED

                padded = (
                    vals +
                    [""] * EXPECTED_COUNT
                )[:EXPECTED_COUNT]

                return (
                    padded,
                    "NOT OK",
                    url,
                    browser_url
                )

        except Exception as e:

            # CHANGED: show the error type (the message alone was empty)
            log(
                f"   ❌ Attempt "
                f"{attempt + 1} Failed: "
                f"{describe_error(e)}"
            )

            save_debug(driver, f"error_{url[-40:]}")  # ADDED

            restart_driver()

    return (
        [""] * EXPECTED_COUNT,
        "NOT OK",
        url,
        ""
    )


# ---------------- GOOGLE SHEETS ---------------- #

def connect_sheets():

    gc = gspread.service_account(
        "credentials.json"
    )

    sh_main = gc.open(
        "STOCKLIST 2"
    ).worksheet("Sheet1")

    sh_data = gc.open(
        "MV2 DAY"
    ).worksheet("Sheet1")

    return sh_main, sh_data


# ---------------- PROCESS ROW ---------------- #

def process_row(
    i,
    company_list,
    url_list,
    current_date
):

    name = (
        company_list[i].strip()
        if i < len(company_list)
        else ""
    )

    url = (
        url_list[i].strip()
        if i < len(url_list)
        and "http" in url_list[i]
        else None
    )

    log(
        f"🔍 [{i + 1}] {name} | "
        f"URL: {url if url else 'No URL'}"
    )

    vals, status, sheet_url_used, browser_url_used = scrape_day(url)

    row_idx = i + 1

    row_payload = [

        {
            "range": f"A{row_idx}",
            "values": [[name]]
        },

        {
            "range": f"B{row_idx}",
            "values": [[current_date]]
        },

        {
            "range": (
                f"{DAY_START_COL_LETTER}{row_idx}:"
                f"{DAY_END_COL_LETTER}{row_idx}"
            ),
            "values": [vals]
        },

        {
            "range": f"{STATUS_COL}{row_idx}",
            "values": [[status]]
        },

        {
            "range": f"{SHEET_URL_COL}{row_idx}",
            "values": [[sheet_url_used]]
        },

        {
            "range": f"{BROWSER_URL_COL}{row_idx}",
            "values": [[browser_url_used]]
        }

    ]

    return row_payload, (status == "OK")


# ---------------- CONNECT ---------------- #

try:

    sheet_main, sheet_data = connect_sheets()

    company_list = api_retry(
        sheet_main.col_values,
        1
    )

    url_list = api_retry(
        sheet_main.col_values,
        4
    )

    log(
        f"✅ Starting rows "
        f"{last_i + 1} to "
        f"{min(END_ROW, len(company_list))}"
    )

except Exception as e:

    log(
        f"❌ Connection Error: {e}"
    )

    sys.exit(1)


# ---------------- PROCESSING ---------------- #

retry_indices = []

batch_list = []

current_date = date.today().strftime(
    "%m/%d/%Y"
)

loop_end = min(
    END_ROW,
    len(company_list)
)


# ---------------- FIRST PASS ---------------- #

for i in range(
    last_i,
    loop_end
):

    payload, success = process_row(
        i,
        company_list,
        url_list,
        current_date
    )

    batch_list.extend(payload)

    if not success:

        retry_indices.append(i)

    # Save checkpoint

    with open(
        checkpoint_file,
        "w"
    ) as f:

        f.write(str(i + 1))

    # Restart browser periodically

    if (i + 1) % RESTART_EVERY_ROWS == 0:

        restart_driver()

    # Upload batch

    if len(batch_list) // 6 >= BATCH_SIZE:

        log(
            "🚀 Uploading batch..."
        )

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )

        batch_list = []


# ---------------- FINAL FIRST PASS UPLOAD ---------------- #

if batch_list:

    api_retry(
        sheet_data.batch_update,
        batch_list,
        value_input_option="RAW"
    )

    batch_list = []


# ---------------- RETRY PASS ---------------- #

if retry_indices:

    log(
        f"🔁 Retrying "
        f"{len(retry_indices)} "
        f"symbols labeled 'NOT OK'..."
    )

    restart_driver()

    batch_list = []

    for idx, i in enumerate(
        retry_indices
    ):

        payload, success = process_row(
            i,
            company_list,
            url_list,
            current_date
        )

        batch_list.extend(payload)

        if (idx + 1) % 10 == 0:

            restart_driver()

            api_retry(
                sheet_data.batch_update,
                batch_list,
                value_input_option="RAW"
            )

            batch_list = []

    if batch_list:

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )


# ---------------- CLEANUP ---------------- #

restart_driver()

log(
    "🏁 SCRAPING COMPLETED."
)
