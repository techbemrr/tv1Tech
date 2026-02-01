// ✅ FINAL UPDATED VERSION
// - Fixes "page.waitForTimeout is not a function"
// - Force-clears TradingView blocking overlays
// - Optimized for GitHub Actions / Headless environments

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  // Use a modern User-Agent to prevent bot-blocking
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");
  
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);
      
      // 'load' ensures the chart script starts execution
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // Immediate burst to kill early blockers
      await killPopups(page);

      // Wait for the Chart Engine (Canvas) to actually render
      await page.waitForFunction(() => {
        const canvas = document.querySelector('canvas');
        return canvas && canvas.offsetWidth > 0;
      }, { timeout: 25000 });

      // Small buffer for technical indicators to calculate and display values
      await delay(4000);
      
      // Final sweep to catch delayed "Sign in" or "Black Friday" popups
      await killPopups(page);

      return true;
    } catch (err) {
      console.warn(`[Warning] Attempt ${i + 1} failed: ${err.message}`);
      // Attempt to clear blockers even on failure before retry
      await killPopups(page).catch(() => {});
      if (i === retries - 1) return false;
      await delay(5000);
    }
  }
}

async function killPopups(page) {
  try {
    // Escape key closes many TradingView native modals
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");

    await page.evaluate(() => {
      // 1. Force Scrollability
      document.documentElement.style.setProperty("overflow", "auto", "important");
      document.body.style.setProperty("overflow", "auto", "important");

      // 2. Targeted removal of TradingView's popup root and common dialogs
      const selectors = [
        '#overlap-manager-root',        // This handles 90% of TV popups
        '[class*="overlap-manager"]',
        '[class*="dialog-"]',
        '.tv-dialog__close',
        '.js-dialog__close',
        'button[name="close"]',
        '[data-role="toast-container"]',
        '.modal-backdrop'
      ];

      selectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => el.remove());
      });

      // 3. Automated "Accept Cookies" Clicker
      const buttons = Array.from(document.querySelectorAll('button'));
      const consentBtn = buttons.find(b => {
        const t = b.innerText.toLowerCase();
        return t.includes('accept') || t.includes('agree') || t.includes('got it');
      });
      if (consentBtn) consentBtn.click();
    });
  } catch (e) {
    // Fail silently during popup removal to prevent script crash
  }
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    // Set a large viewport so the legend is not hidden by the UI
    await page.setViewport({ width: 1920, height: 1080 });
    
    const success = await safeGoto(page, url);

    if (!success) {
      console.error(`[Error] Navigation failed permanently for: ${url}`);
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    // Wait for the specific legend element to appear in the DOM
    await page.waitForSelector('[data-qa-id="legend"]', { timeout: 15000 });
    
    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        // Look for your specific indicator section
        const target = [...sections].find((section) => {
          const title = section.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA');
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!target) return ["INDICATOR NOT FOUND"];

        const spans = target.querySelectorAll(".valueValue-l31H9iuA");
        const results = [...spans].map(s => s.innerText.trim());
        
        return results.length > 0 ? results : ["NO VALUES"];
      }
    );

    console.log(`[Success] Scraped ${values.length} values from ${url}`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];

  } catch (err) {
    console.error(`[Fatal] Scrape Error on ${url}:`, err.message);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}

// Fixed length helper to keep your CSV/Sheets columns aligned
function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

// DD/MM/YYYY date builder
function buildDate(day, month, year) {
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${year}`;
}
