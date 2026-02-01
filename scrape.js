// ✅ Complete Drop-in Replacement
// Fixes "Navigation Failed" by ignoring heavy background assets and forcing a desktop view

async function safeGoto(page, url, retries = 3) {
  // Set a standard desktop User-Agent to avoid bot detection
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
  
  for (let i = 0; i < retries; i++) {
    try {
      // 1. "domcontentloaded" is safer than "networkidle" for TradingView
      // Reduced timeout to 30s to fail fast and retry rather than hanging
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

      // 2. Kill popups immediately
      await killPopups(page);

      // 3. Wait for the chart engine to actually start (the Canvas)
      // This is more reliable than waiting for the legend
      await page.waitForSelector('canvas.chart-gui-wrapper', { timeout: 15000 });

      // 4. Brief pause for SPA elements to settle
      await page.waitForTimeout(2000);
      await killPopups(page);

      return true;
    } catch (err) {
      console.warn(`Attempt ${i + 1} failed for ${url}: ${err.message}`);
      if (i === retries - 1) return false;
      // Wait before retry
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

async function killPopups(page) {
  try {
    // Spam Escape key to close native TV dialogs
    for (let k = 0; k < 3; k++) {
      await page.keyboard.press("Escape");
    }

    await page.evaluate(() => {
      // Force scrollability
      document.documentElement.style.setProperty("overflow", "auto", "important");
      document.body.style.setProperty("overflow", "auto", "important");

      // Specific TradingView Overlay IDs/Classes
      const blockers = [
        '#overlap-manager-root', 
        '.tv-dialog__close', 
        '.js-dialog__close',
        '[data-name="close"]',
        '[class*="overlap-manager"]',
        '[class*="dialog-"]',
        '.modal-backdrop'
      ];

      blockers.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => el.remove());
      });

      // Remove any fixed full-screen divs that aren't the chart
      document.querySelectorAll('div').forEach(div => {
        const style = window.getComputedStyle(div);
        if (style.position === 'fixed' && parseInt(style.zIndex) > 100) {
          if (!div.querySelector('canvas')) div.remove();
        }
      });
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    // Ensure viewport is large enough to render the legend
    await page.setViewport({ width: 1920, height: 1080 });
    
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    // Final clean before extraction
    await killPopups(page);

    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    // Wait for the specific legend item to be visible
    await page.waitForSelector('[data-qa-id="legend"]', { timeout: 10000 });

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        const target = [...sections].find((section) => {
          const title = section.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA');
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!target) return ["CLUBBED NOT FOUND"];

        const valueSpans = target.querySelectorAll(".valueValue-l31H9iuA");
        return [...valueSpans].map((el) => {
          const t = el.innerText.trim();
          return t === "∅" || t === "" ? "None" : t;
        });
      }
    );

    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];
  } catch (err) {
    console.error(`Scrape Error: ${err.message}`);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}

// Support functions
function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

function buildDate(day, month, year) {
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${year}`;
}
