// ✅ Updated safeGoto: More aggressive timing and failure handling
async function safeGoto(page, url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      // Use 'commit' to get in as soon as the network responds, then handle the rest manually
      await page.goto(url, { waitUntil: "commit", timeout: 30000 });

      // 1. Initial burst to kill early blockers
      await killPopups(page);
      
      // 2. Wait for the main chart container instead of just the legend (more reliable)
      await page.waitForSelector('.chart-container-border', { timeout: 15000 }).catch(() => {
        console.warn("Chart container not found, but continuing...");
      });

      // 3. Final popup sweep
      await page.waitForTimeout(2000);
      await killPopups(page);

      // 4. Verify legend exists before finishing
      await page.waitForSelector('[data-qa-id="legend"]', { timeout: 10000 });

      return true;
    } catch (err) {
      console.warn(`Retry ${i + 1} for ${url} – ${err.message}`);
      if (i === retries - 1) return false;
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

// ✅ Updated killPopups: Force-removes known TradingView blocking IDs
async function killPopups(page) {
  try {
    // Escape spam
    for (let k = 0; k < 3; k++) {
      await page.keyboard.press("Escape");
    }

    await page.evaluate(() => {
      // 1. Unlock Scrolling
      document.documentElement.style.setProperty("overflow", "auto", "important");
      document.body.style.setProperty("overflow", "auto", "important");

      // 2. Target specific TradingView popup/modal containers
      const selectors = [
        '[class*="overlap-manager"]', // Main container for TV modals
        '[class*="dialog-"]',          // Any TV dialog
        '.tv-dialog__close',
        '.js-dialog__close',
        '[data-name="close"]',
        '#overlap-manager-root',       // Common TV overlay root
        '[data-role="toast-container"]',
        '.modal-backdrop'
      ];

      selectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => el.remove());
      });

      // 3. Force remove any fixed element with high z-index (except the chart/header)
      const all = document.querySelectorAll('*');
      for (const el of all) {
        const style = window.getComputedStyle(el);
        const zIndex = parseInt(style.zIndex);
        if (style.position === 'fixed' && zIndex > 50) {
            // Keep the main layout, kill the rest
            if (!el.innerText.includes('Symbol Search') && !el.querySelector('canvas')) {
                el.remove();
            }
        }
      }
    });
  } catch (e) {
    // Silently fail if page is closed
  }
}

// ✅ Rest of your scraping logic remains mostly same, added a small delay
export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    // Final clean
    await killPopups(page);
    await page.waitForTimeout(500);

    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        const clubbed = [...sections].find((section) => {
          const title = section.querySelector(
            '[data-qa-id="legend-source-title"] .title-l31H9iuA'
          );
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!clubbed) return ["CLUBBED NOT FOUND"];

        const valueSpans = clubbed.querySelectorAll(".valueValue-l31H9iuA");
        return [...valueSpans].map((el) => {
          const t = el.innerText.trim();
          return t === "∅" ? "None" : t;
        });
      }
    );

    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];
  } catch (err) {
    console.error(`Error scraping ${url}:`, err.message);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}

// Helper functions (required to keep code complete)
function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

function buildDate(day, month, year) {
  if (!year) return "";
  if (!day && !month) return `${year}`;
  if (!day) day = "01";
  if (!month) month = "01";
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${year}`;
}
