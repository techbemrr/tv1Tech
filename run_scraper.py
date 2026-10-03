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

import gspread
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# LOG
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

COOKIE_FILE = os.getenv(
    "COOKIE_FILE",
    "cookies.json"
)

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# C column
DAY_OUTPUT_START_COL = 3


# ============================================================
# GOOGLE SHEET COLUMN HELPERS
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

# 18 values:
# C:T
DAY_END_COL_LETTER = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1
)

# U
STATUS_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT
)

# V
SHEET_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1
)

# W
BROWSER_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2
)


log(
    f"📌 Output columns: "
    f"{DAY_START_COL_LETTER}:{DAY_END_COL_LETTER}"
)

log(
    f"📌 Status column: {STATUS_COL}"
)

log(
    f"📌 Sheet URL column: {SHEET_URL_COL}"
)

log(
    f"📌 Browser URL column: {BROWSER_URL_COL}"
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
                f"⚠️ API Issue: {str(e)[:120]}"
            )

            log(
                f"🔄 Retrying in {wait:.1f}s..."
            )

            time.sleep(wait)

    return func(*args, **kwargs)


# ============================================================
# CHECKPOINT
# ============================================================

if os.path.exists(checkpoint_file):

    try:

        with open(
            checkpoint_file,
            "r"
        ) as f:

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
        f"Initializing Chrome..."
    )

    opts = Options()

    # GitHub Actions
    opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")

    opts.add_argument(
        "--disable-dev-shm-usage"
    )

    opts.add_argument(
        "--window-size=1920,1080"
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
        "--disable-blink-features=AutomationControlled"
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

    # --------------------------------------------------------
    # Hide webdriver property
    # --------------------------------------------------------

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
                        'languages',
                        {
                            get: () => ['en-US', 'en']
                        }
                    );

                    Object.defineProperty(
                        navigator,
                        'platform',
                        {
                            get: () => 'Win32'
                        }
                    );
                """
            }
        )

    except Exception as e:

        log(
            f"⚠️ Browser setup warning: "
            f"{str(e)[:100]}"
        )

    # --------------------------------------------------------
    # Cookies
    # --------------------------------------------------------

    if os.path.exists(COOKIE_FILE):

        try:

            log("🍪 Loading TradingView cookies...")

            drv.get(
                "https://in.tradingview.com/"
            )

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

                    if "domain" in cookie:

                        if (
                            "tradingview.com"
                            not in cookie["domain"]
                        ):

                            cookie.pop(
                                "domain",
                                None
                            )

                    drv.add_cookie(cookie)

                    loaded += 1

                except Exception:
                    pass

            log(
                f"🍪 Loaded {loaded} cookies"
            )

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

    if driver is not None:

        try:

            driver.quit()

        except Exception:

            pass

    driver = None


# ============================================================
# POPUP HANDLER
# ============================================================

def close_normal_popups(drv):

    """
    Attempts to close ordinary TradingView UI popups.
    This does NOT attempt to bypass CAPTCHA/human verification.
    """

    popup_selectors = [

        # Common close buttons
        "button[aria-label='Close']",

        "button[aria-label='close']",

        "[data-name='close']",

        "[data-name='close-button']",

        # Dialog close buttons
        "[role='dialog'] button[aria-label='Close']",

        # Common modal close
        "[class*='dialog'] button[aria-label='Close']",

        "[class*='modal'] button[aria-label='Close']",

    ]

    closed = 0

    for selector in popup_selectors:

        try:

            elements = drv.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            for element in elements:

                try:

                    if element.is_displayed():

                        drv.execute_script(
                            "arguments[0].click();",
                            element
                        )

                        closed += 1

                        time.sleep(0.3)

                except Exception:

                    pass

        except Exception:

            pass

    if closed:

        log(
            f"🧹 Closed {closed} normal popup(s)"
        )


# ============================================================
# PAGE BLOCK CHECK
# ============================================================

def check_for_block_page(drv):

    try:

        html = drv.page_source.lower()

        block_words = [

            "verify you are human",

            "checking your browser",

            "captcha",

            "access denied",

            "unusual traffic",

        ]

        for word in block_words:

            if word in html:

                log(
                    f"🚨 TradingView verification/block "
                    f"page detected: {word}"
                )

                return True

    except Exception:

        pass

    return False


# ============================================================
# IMPORTANT:
# EXTRACT ONLY LEAF VALUE ELEMENTS
# ============================================================

def get_leaf_value_elements(
    drv,
    selector
):

    """
    TradingView can have a parent element and child
    elements that both contain the valueValue class.

    Example:

        parent valueValue
            child valueValue -> 1
            child valueValue -> 10
            child valueValue -> 207.26

    The old code could read the parent as one giant
    multiline value.

    This function keeps only elements that DO NOT
    contain another matching value element.
    """

    try:

        elements = drv.find_elements(
            By.CSS_SELECTOR,
            selector
        )

    except Exception:

        return []

    if not elements:

        return []

    leaf_elements = []

    for element in elements:

        try:

            children = element.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            # If the element has matching descendants,
            # it is a parent/container.
            if len(children) > 0:

                continue

            leaf_elements.append(element)

        except Exception:

            # If descendant detection fails,
            # keep the element rather than losing data.
            leaf_elements.append(element)

    return leaf_elements


# ============================================================
# EXTRACT 18 VALUES
# ============================================================

def get_tradingview_values(drv):

    """
    Extract TradingView values without:
      - parent containers
      - generic TD elements
      - generic [class*=value]
      - duplicate removal by text

    Duplicate TEXT values are intentionally preserved.

    Example:

        1
        10
        1
        207.26
        ...
        1.07
        1.07

    Both 1.07 values are valid and must remain.
    """

    selectors = [

        "[class*='valueValue']",

        "[class*='valueValue-']",

        "[data-name='legend-source-item-value']",

        "[data-name='legend-series-item-value']",

    ]

    # We will try selectors one at a time.
    # This avoids combining parent/child results
    # from different selector types.

    for selector in selectors:

        elements = get_leaf_value_elements(
            drv,
            selector
        )

        if not elements:

            continue

        extracted = []

        seen_element_ids = set()

        for element in elements:

            try:

                # Selenium's internal element ID lets us
                # prevent the SAME DOM element from being
                # processed twice.
                element_id = element.id

                if element_id in seen_element_ids:

                    continue

                seen_element_ids.add(
                    element_id
                )

                text = element.text.strip()

                if not text:

                    continue

                # ------------------------------------------------
                # IMPORTANT
                #
                # If a leaf element itself contains multiple
                # lines, split those lines.
                #
                # DO NOT remove duplicates.
                # ------------------------------------------------

                lines = [

                    line.strip()

                    for line in text.splitlines()

                    if line.strip()

                ]

                for line in lines:

                    extracted.append(line)

            except Exception:

                continue

        if extracted:

            log(
                f"🔎 Selector {selector} "
                f"returned {len(extracted)} values"
            )

            # If we have enough, this is our result.
            if len(extracted) >= EXPECTED_COUNT:

                return extracted[
                    :EXPECTED_COUNT
                ]

            # Keep searching another selector if
            # current selector did not provide enough.
            best = extracted

        else:

            best = []

    return best if 'best' in locals() else []


# ============================================================
# WAIT FOR 18 VALUES
# ============================================================

def wait_for_values(
    drv,
    timeout=40
):

    start_time = time.time()

    best_values = []

    while (
        time.time() - start_time
        < timeout
    ):

        try:

            close_normal_popups(drv)

            values = get_tradingview_values(
                drv
            )

            if len(values) > len(best_values):

                best_values = values

                log(
                    f"📊 Found "
                    f"{len(best_values)}/"
                    f"{EXPECTED_COUNT} values"
                )

            if (
                len(best_values)
                >= EXPECTED_COUNT
            ):

                return best_values[
                    :EXPECTED_COUNT
                ]

        except Exception as e:

            log(
                f"⚠️ Extraction error: "
                f"{str(e)[:100]}"
            )

        # Give TradingView time to render
        time.sleep(1)

    return best_values[
        :EXPECTED_COUNT
    ]


# ============================================================
# SCRAPE ONE DAY
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
                f"🌐 Opening URL "
                f"(attempt {attempt + 1}/3)"
            )

            log(
                f"🔗 {url}"
            )

            drv.get(url)

            # Initial page render
            time.sleep(5)

            # ------------------------------------------------
            # Browser information
            # ------------------------------------------------

            try:

                log(
                    f"📄 Title: {drv.title}"
                )

                log(
                    f"🌐 Current URL: "
                    f"{drv.current_url}"
                )

            except Exception:

                pass

            # ------------------------------------------------
            # Detect verification page
            # ------------------------------------------------

            if check_for_block_page(drv):

                log(
                    "⚠️ TradingView returned "
                    "a verification/block page."
                )

                restart_driver()

                time.sleep(
                    3 + attempt
                )

                continue

            # ------------------------------------------------
            # Close ordinary popups
            # ------------------------------------------------

            close_normal_popups(drv)

            # ------------------------------------------------
            # Wait for document
            # ------------------------------------------------

            try:

                start_wait = time.time()

                while (
                    time.time() - start_wait
                    < 20
                ):

                    state = drv.execute_script(
                        "return document.readyState"
                    )

                    if state == "complete":

                        break

                    time.sleep(0.5)

            except Exception:

                pass

            # ------------------------------------------------
            # Wait for values
            # ------------------------------------------------

            vals = wait_for_values(
                drv,
                timeout=40
            )

            # ------------------------------------------------
            # Additional scrolling
            # ------------------------------------------------

            if len(vals) < EXPECTED_COUNT:

                log(
                    f"🔄 Only {len(vals)}/"
                    f"{EXPECTED_COUNT} found."
                    f" Trying additional rendering..."
                )

                scroll_positions = [

                    0,

                    300,

                    600,

                    900,

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

                    time.sleep(1.5)

                    new_vals = (
                        get_tradingview_values(
                            drv
                        )
                    )

                    if len(new_vals) > len(vals):

                        vals = new_vals

                        log(
                            f"📈 Now found "
                            f"{len(vals)}/"
                            f"{EXPECTED_COUNT}"
                        )

                    if (
                        len(vals)
                        >= EXPECTED_COUNT
                    ):

                        break

            # ------------------------------------------------
            # Final extraction
            # ------------------------------------------------

            found_count = len(vals)

            browser_url = ""

            try:

                browser_url = drv.current_url

            except Exception:

                pass

            # ------------------------------------------------
            # EXACTLY 18 VALUES
            # ------------------------------------------------

            if found_count >= EXPECTED_COUNT:

                vals = vals[
                    :EXPECTED_COUNT
                ]

                log(
                    f"✅ SUCCESS: "
                    f"{len(vals)}/"
                    f"{EXPECTED_COUNT}"
                )

                log(
                    "📋 Values:"
                )

                for n, value in enumerate(
                    vals,
                    start=1
                ):

                    log(
                        f"   {n}. {value}"
                    )

                return (
                    vals,
                    "OK",
                    url,
                    browser_url
                )

            # ------------------------------------------------
            # NOT ENOUGH
            # ------------------------------------------------

            log(
                f"⚠️ Only "
                f"{found_count}/"
                f"{EXPECTED_COUNT} values found"
            )

            if vals:

                log(
                    "📋 Partial values:"
                )

                for n, value in enumerate(
                    vals,
                    start=1
                ):

                    log(
                        f"   {n}. {value}"
                    )

            # Pad to exactly 18 columns

            padded = (
                vals
                + [""] * (
                    EXPECTED_COUNT
                    - len(vals)
                )
            )

            # Restart browser before retry

            restart_driver()

            if attempt < 2:

                wait_time = 3 + (
                    attempt * 2
                )

                log(
                    f"🔄 Retrying in "
                    f"{wait_time}s..."
                )

                time.sleep(
                    wait_time
                )

                continue

            return (
                padded,
                "NOT OK",
                url,
                browser_url
            )

        except Exception as e:

            log(
                f"❌ Attempt "
                f"{attempt + 1} failed: "
                f"{str(e)[:150]}"
            )

            restart_driver()

            if attempt < 2:

                time.sleep(
                    3 + attempt
                )

    return (
        [""] * EXPECTED_COUNT,
        "NOT OK",
        url,
        ""
    )


# ============================================================
# GOOGLE SHEETS CONNECTION
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
# PROCESS ONE ROW
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

        if (
            i < len(url_list)
            and "http" in url_list[i]
        )

        else None

    )

    log(
        f"🔍 [{i + 1}] "
        f"{name} | "
        f"URL: "
        f"{url if url else 'No URL'}"
    )

    vals, status, sheet_url_used, browser_url_used = (
        scrape_day(url)
    )

    row_idx = i + 1

    # --------------------------------------------------------
    # Make absolutely sure Google Sheets receives
    # exactly 18 individual cells.
    # --------------------------------------------------------

    vals = (
        vals[:EXPECTED_COUNT]
        + [""] * (
            EXPECTED_COUNT
            - len(vals[:EXPECTED_COUNT])
        )
    )

    # This will always contain exactly 18 values
    assert len(vals) == EXPECTED_COUNT

    row_payload = [

        {
            "range": f"A{row_idx}",
            "values": [
                [name]
            ]
        },

        {
            "range": f"B{row_idx}",
            "values": [
                [current_date]
            ]
        },

        {
            "range":
                f"{DAY_START_COL_LETTER}{row_idx}:"
                f"{DAY_END_COL_LETTER}{row_idx}",

            "values": [
                vals
            ]
        },

        {
            "range": f"{STATUS_COL}{row_idx}",
            "values": [
                [status]
            ]
        },

        {
            "range": f"{SHEET_URL_COL}{row_idx}",
            "values": [
                [sheet_url_used]
            ]
        },

        {
            "range": f"{BROWSER_URL_COL}{row_idx}",
            "values": [
                [browser_url_used]
            ]
        }

    ]

    return (
        row_payload,
        status == "OK"
    )


# ============================================================
# CONNECT TO GOOGLE
# ============================================================

try:

    sheet_main, sheet_data = (
        connect_sheets()
    )

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
# VARIABLES
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

    batch_list.extend(
        payload
    )

    if not success:

        retry_indices.append(i)

    # --------------------------------------------------------
    # Save checkpoint
    # --------------------------------------------------------

    with open(
        checkpoint_file,
        "w"
    ) as f:

        f.write(
            str(i + 1)
        )

    # --------------------------------------------------------
    # Restart browser periodically
    # --------------------------------------------------------

    if (
        (i + 1)
        % RESTART_EVERY_ROWS
        == 0
    ):

        log(
            "♻️ Scheduled browser restart..."
        )

        restart_driver()

    # --------------------------------------------------------
    # Upload batch
    # --------------------------------------------------------

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
# FINAL FIRST-PASS UPLOAD
# ============================================================

if batch_list:

    log(
        "🚀 Uploading final batch..."
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
        f"NOT OK symbols..."
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

        batch_list.extend(
            payload
        )

        # Restart every 10 retries

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

    # Remaining retry batch

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
