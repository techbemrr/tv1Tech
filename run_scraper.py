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


# ============================================================
# LOGGING
# ============================================================

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ============================================================
# CONFIG
# ============================================================

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


# ============================================================
# COLUMN HELPERS
# ============================================================

def col_num_to_letter(n):
    result = ""

    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result

    return result


DAY_START_COL_LETTER = col_num_to_letter(
    DAY_OUTPUT_START_COL
)

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


# ============================================================
# GOOGLE API RETRY
# ============================================================

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


# ============================================================
# CHECKPOINT
# ============================================================

if os.path.exists(checkpoint_file):

    try:

        with open(checkpoint_file, "r") as f:
            last_i = max(
                int(f.read().strip()),
                START_ROW
            )

    except Exception:

        last_i = START_ROW

else:

    last_i = START_ROW


# ============================================================
# DRIVER
# ============================================================

driver = None


def create_driver():

    log(
        f"🌐 [Shard {SHARD_INDEX}] "
        f"Initializing browser..."
    )

    opts = Options()

    # GitHub Actions / server friendly
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    opts.add_argument("--window-size=1920,1080")

    # Reduce automation detection
    opts.add_argument(
        "--disable-blink-features=AutomationControlled"
    )

    opts.add_argument(
        "--disable-gpu"
    )

    opts.add_argument(
        "--disable-extensions"
    )

    opts.add_argument(
        "--disable-notifications"
    )

    opts.add_argument(
        "--disable-popup-blocking"
    )

    opts.add_argument(
        "--lang=en-US"
    )

    opts.add_argument(
        "--user-agent=Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/154.0.0.0 Safari/537.36"
    )

    drv = webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )

    # Hide webdriver property
    try:

        drv.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(
                        navigator,
                        'webdriver',
                        {
                            get: () => undefined
                        }
                    );

                    Object.defineProperty(
                        navigator,
                        'platform',
                        {
                            get: () => 'Win32'
                        }
                    );

                    Object.defineProperty(
                        navigator,
                        'languages',
                        {
                            get: () => ['en-US', 'en']
                        }
                    );
                """
            }
        )

    except Exception as e:

        log(
            f"⚠️ CDP anti-detection setup failed: "
            f"{str(e)[:100]}"
        )

    # ========================================================
    # COOKIES
    # ========================================================

    if os.path.exists(COOKIE_FILE):

        try:

            log("🍪 Loading TradingView cookies...")

            drv.get("https://in.tradingview.com/")

            time.sleep(2)

            with open(
                COOKIE_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                cookies = json.load(f)

            loaded = 0

            for c in cookies:

                try:

                    cookie = {
                        k: v
                        for k, v in c.items()
                        if k in (
                            "name",
                            "value",
                            "path",
                            "secure",
                            "expiry",
                            "domain"
                        )
                    }

                    # Selenium sometimes rejects
                    # invalid domain cookies
                    if "domain" in cookie:
                        if "tradingview.com" not in cookie["domain"]:
                            cookie.pop("domain", None)

                    drv.add_cookie(cookie)

                    loaded += 1

                except Exception:
                    pass

            log(f"🍪 Loaded {loaded} cookies")

            drv.refresh()

            time.sleep(3)

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


# ============================================================
# TRADINGVIEW VALUE SELECTORS
# ============================================================

VALUE_SELECTORS = [

    # Current / old TradingView selector
    "[class*='valueValue']",

    # Possible quote value containers
    "[class*='value']",

    # Symbol overview values
    "[class*='cell'] [class*='value']",

    # Technical analysis values
    "[class*='rating']",

    # Table cells
    "td",

]


# ============================================================
# GET VALUES
# ============================================================

def get_values(drv):

    all_values = []

    seen = set()

    for selector in VALUE_SELECTORS:

        try:

            elements = drv.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            for el in elements:

                try:

                    text = el.text.strip()

                    if not text:
                        continue

                    # Remove duplicate values
                    if text in seen:
                        continue

                    seen.add(text)
                    all_values.append(text)

                except Exception:
                    continue

        except Exception:
            continue

    return all_values


# ============================================================
# GET SPECIFIC TRADINGVIEW VALUES
# ============================================================

def get_tradingview_values(drv):

    values = []

    # --------------------------------------------------------
    # METHOD 1
    # Current valueValue selector
    # --------------------------------------------------------

    selectors = [

        "[class*='valueValue']",

        "[class*='valueValue-']",

        "[data-name='legend-source-item']",

        "[data-name='legend-source-item-value']",

        "[data-name='legend-series-item']",

        "[data-name='legend-series-item-value']",

    ]

    for selector in selectors:

        try:

            elements = drv.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            for element in elements:

                try:

                    text = element.text.strip()

                    if text:
                        values.append(text)

                except Exception:
                    pass

        except Exception:
            pass

    # Remove duplicates while preserving order

    final_values = []

    seen = set()

    for value in values:

        if value not in seen:

            seen.add(value)
            final_values.append(value)

    return final_values


# ============================================================
# PAGE CHECK
# ============================================================

def check_page(drv):

    try:

        title = drv.title

        current_url = drv.current_url

        html = drv.page_source.lower()

        log(f"📄 Title: {title}")
        log(f"🌐 Current URL: {current_url}")

        # CAPTCHA / Cloudflare detection

        blocked_words = [
            "captcha",
            "verify you are human",
            "checking your browser",
            "access denied",
            "unusual traffic",
        ]

        for word in blocked_words:

            if word in html:

                log(
                    f"🚨 Possible TradingView "
                    f"block detected: {word}"
                )

                return False

        return True

    except Exception:

        return True


# ============================================================
# WAIT FOR VALUES
# ============================================================

def wait_for_values(drv, timeout=30):

    start = time.time()

    best_values = []

    while time.time() - start < timeout:

        try:

            values = get_tradingview_values(drv)

            if len(values) > len(best_values):

                best_values = values

                log(
                    f"📊 Values currently found: "
                    f"{len(best_values)}"
                )

            if len(best_values) >= EXPECTED_COUNT:

                return best_values

        except Exception:
            pass

        # Small scroll helps TradingView lazy rendering

        try:

            drv.execute_script(
                "window.scrollTo(0, 500);"
            )

        except Exception:
            pass

        time.sleep(1)

    return best_values


# ============================================================
# SCRAPE DAY
# ============================================================

def scrape_day(url):

    if not url:

        return (
            [""] * EXPECTED_COUNT,
            "NOT OK",
            "",
            ""
        )

    for attempt in range(3):

        try:

            drv = ensure_driver()

            log(
                f"🌐 Opening TradingView "
                f"(attempt {attempt + 1}/3)"
            )

            drv.get(url)

            # Give TradingView enough time to initialize

            time.sleep(5)

            check_page(drv)

            # ------------------------------------------------
            # Wait for document ready
            # ------------------------------------------------

            try:

                WebDriverWait(
                    drv,
                    20
                ).until(
                    lambda d:
                    d.execute_script(
                        "return document.readyState"
                    ) == "complete"
                )

            except Exception:
                pass

            # ------------------------------------------------
            # Wait for TradingView values
            # ------------------------------------------------

            vals = wait_for_values(
                drv,
                timeout=30
            )

            # ------------------------------------------------
            # Additional scrolling
            # ------------------------------------------------

            if len(vals) < EXPECTED_COUNT:

                log(
                    f"🔄 Only {len(vals)}/"
                    f"{EXPECTED_COUNT}. "
                    f"Trying additional rendering..."
                )

                scroll_positions = [
                    0,
                    400,
                    800,
                    1200,
                    1600,
                    2000,
                    2500,
                ]

                for scroll_y in scroll_positions:

                    try:

                        drv.execute_script(
                            f"window.scrollTo("
                            f"0, {scroll_y});"
                        )

                    except Exception:
                        pass

                    time.sleep(2)

                    new_vals = get_tradingview_values(
                        drv
                    )

                    if len(new_vals) > len(vals):

                        vals = new_vals

                        log(
                            f"📈 Values increased to "
                            f"{len(vals)}"
                        )

                    if len(vals) >= EXPECTED_COUNT:
                        break

            # ------------------------------------------------
            # Final attempt with generic selector
            # ------------------------------------------------

            if len(vals) < EXPECTED_COUNT:

                log(
                    "🔎 Trying fallback value "
                    "selector..."
                )

                fallback = get_values(drv)

                if len(fallback) > len(vals):

                    vals = fallback

                    log(
                        f"📈 Fallback found "
                        f"{len(vals)} values"
                    )

            browser_url = drv.current_url

            found_count = len(vals)

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if found_count >= EXPECTED_COUNT:

                log(
                    f"✅ Found "
                    f"{found_count}/{EXPECTED_COUNT}"
                )

                return (
                    vals[:EXPECTED_COUNT],
                    "OK",
                    url,
                    browser_url
                )

            # ------------------------------------------------
            # FAILED
            # ------------------------------------------------

            log(
                f"⚠️ Found "
                f"{found_count}/{EXPECTED_COUNT}"
            )

            # Print values for debugging
            if vals:

                log(
                    "📋 Values detected: "
                    + " | ".join(
                        vals[:EXPECTED_COUNT]
                    )
                )

            padded = (
                vals +
                [""] * EXPECTED_COUNT
            )[:EXPECTED_COUNT]

            # Restart browser before next attempt

            restart_driver()

            if attempt < 2:

                wait_time = 3 + attempt * 2

                log(
                    f"🔄 Retrying after "
                    f"{wait_time}s..."
                )

                time.sleep(wait_time)

                continue

            return (
                padded,
                "NOT OK",
                url,
                browser_url
            )

        except Exception as e:

            log(
                f"❌ Attempt {attempt + 1} "
                f"failed: {str(e)[:150]}"
            )

            restart_driver()

            if attempt < 2:

                time.sleep(3)

    return (
        [""] * EXPECTED_COUNT,
        "NOT OK",
        url,
        ""
    )


# ============================================================
# GOOGLE SHEETS
# ============================================================

def connect_sheets():

    gc = gspread.service_account(
        "credentials.json"
    )

    sh_main = gc.open(
        "STOCKLIST 2"
    ).worksheet(
        "Sheet1"
    )

    sh_data = gc.open(
        "MV2 DAY"
    ).worksheet(
        "Sheet1"
    )

    return sh_main, sh_data


# ============================================================
# PROCESS ROW
# ============================================================

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

    vals, status, sheet_url_used, browser_url_used = (
        scrape_day(url)
    )

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
            "range":
                f"{DAY_START_COL_LETTER}{row_idx}:"
                f"{DAY_END_COL_LETTER}{row_idx}",

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

    return row_payload, status == "OK"


# ============================================================
# CONNECT
# ============================================================

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


# ============================================================
# MAIN VARIABLES
# ============================================================

retry_indices = []
batch_list = []

current_date = date.today().strftime(
    "%m/%d/%Y"
)

loop_end = min(
    END_ROW,
    len(company_list)
)


# ============================================================
# FIRST PASS
# ============================================================

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

        f.write(
            str(i + 1)
        )

    # Restart periodically

    if (
        (i + 1) % RESTART_EVERY_ROWS
        == 0
    ):

        log(
            "♻️ Scheduled browser restart..."
        )

        restart_driver()

    # Upload batch

    if (
        len(batch_list) // 6
        >= BATCH_SIZE
    ):

        log(
            "🚀 Uploading batch..."
        )

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )

        batch_list = []


# ============================================================
# UPLOAD REMAINING FIRST PASS
# ============================================================

if batch_list:

    log(
        "🚀 Uploading final first-pass batch..."
    )

    api_retry(
        sheet_data.batch_update,
        batch_list,
        value_input_option="RAW"
    )

    batch_list = []


# ============================================================
# RETRY PASS
# ============================================================

if retry_indices:

    log(
        f"🔁 Retrying "
        f"{len(retry_indices)} "
        f"symbols labeled NOT OK..."
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

        # Restart every 10 retry rows

        if (
            (idx + 1) % 10
            == 0
        ):

            log(
                "♻️ Restarting browser "
                "during retry pass..."
            )

            restart_driver()

            if batch_list:

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


# ============================================================
# CLEANUP
# ============================================================

restart_driver()

log(
    "🏁 SCRAPING COMPLETED."
)
