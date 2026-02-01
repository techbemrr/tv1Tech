// ✅ COMPLETE UPDATE: High-Resilience Scraping for TradingView
// This version includes a "Page Ready" check and handles the canvas rendering delay.

async function safeGoto(page, url, retries = 3) {
  // 1. Force a real-world User Agent
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");
  
  // 2. Extra Headers to look more like a browser
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
  });

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`--- Navigating to: ${url} (Attempt ${i+1}) ---`);
      
      // Use 'load' instead of 'domcontentloaded' to ensure the JS engine starts
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      // Immediate kill of common blocking overlays
      await killPopups(page);

      // ✅ CHANGE: Wait for ANY canvas first, then specifically the chart wrapper
      // Some TV layouts load different canvas classes
      await page.waitForFunction(() => {
        const c = document.querySelector('canvas');
        return c && c.offsetWidth > 0;
      }, { timeout: 20000 });

      // Small buffer for the legend to populate values
      await page.waitForTimeout(3000);
      
      // Final sweep
      await killPopups(page);

      return true;
    } catch (err) {
      console.warn(`Attempt ${i + 1} failed: ${err.message}`);
      // If we see a popup, try to kill it even during failure
      await killPopups(page).catch(() => {});
      if (i === retries - 1) return false;
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

async function killPopups(page) {
  try {
    // Escape key is the most effective tool against TradingView modals
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");

    await page.evaluate(() => {
      // 1. Unlock Scroll
      document.documentElement.style.overflow = "auto";
      document.body.style.overflow = "auto";

      // 2. List of elements that TradingView uses to block the UI
      const blockerSelectors = [
        'div[id^="overlap-manager-root"]',
        '.tv-dialog__close',
        '.js-dialog__close',
        'button[name="close"]',
        '[data-role="toast-container"]',
        '.modal-backdrop',
        '#overlap-manager-root',
        '[class*="overlap-manager"]',
        '[class*="dialog-"]',
        '[class*="overlay-"]'
      ];

      blockerSelectors.forEach(selector => {
        document.querySelectorAll(selector).forEach(el => {
          el.style.display = 'none'; // Hide it
          el.remove();               // Then delete it
        });
      });

      // 3. Click "Accept" if a cookie banner is found
      const buttons = Array.from(document.querySelectorAll('button'));
      const acceptBtn = buttons.find(b => b.innerText.includes('Accept') || b.innerText.includes('Agree'));
      if (acceptBtn) acceptBtn.click();
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    // TradingView needs a wide screen to show the legend values
    await page.setViewport({ width: 1920, height: 1080 });
    
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    // Extracting the data
    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        // Find the specific section with title 'clubbed' or 'l'
        const targetSection = [...sections].find((section) => {
          const titleEl = section.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA');
          const titleText = titleEl?.innerText?.trim().toLowerCase();
          return titleText === "clubbed" || titleText === "l";
        });

        if (!targetSection) return ["INDICATOR NOT FOUND"];

        const spans = targetSection.querySelectorAll(".valueValue-l31H9iuA");
        const data = [...spans].map(s => s.innerText.trim());
        return data.length > 0 ? data : ["NO VALUES"];
      }
    );

    console.log(`Successfully scraped: ${url}`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];

  } catch (err) {
    console.error(`Error on ${url}:`, err.message);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}

// Helpers
function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

function buildDate(day, month, year) {
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${year}`;
}
