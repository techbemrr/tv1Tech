// ✅ FINAL OPTIMIZED VERSION: Fixes "∅" symbol issue
// This version waits specifically for numbers to appear in the legend.

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");
  
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      await killPopups(page);

      // Wait for the Chart Engine
      await page.waitForFunction(() => {
        const canvas = document.querySelector('canvas');
        return canvas && canvas.offsetWidth > 0;
      }, { timeout: 25000 });

      // ✅ KEY FIX: Wait for the legend values to change from '∅' to actual numbers
      // We check the 'clubbed' or 'l' section specifically
      await page.waitForFunction(() => {
        const items = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const target = Array.from(items).find(el => {
            const t = el.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
            return t === "clubbed" || t === "l";
        });
        if (!target) return false;
        const firstVal = target.querySelector(".valueValue-l31H9iuA")?.innerText;
        return firstVal && firstVal !== "∅" && firstVal !== "";
      }, { timeout: 15000 }).catch(() => console.log("Values still ∅ after wait, attempting extraction anyway."));

      await delay(2000); // Final settle time
      return true;
    } catch (err) {
      console.warn(`[Warning] Attempt ${i + 1} failed: ${err.message}`);
      await killPopups(page).catch(() => {});
      if (i === retries - 1) return false;
      await delay(5000);
    }
  }
}

async function killPopups(page) {
  try {
    await page.keyboard.press("Escape");
    await page.evaluate(() => {
      document.documentElement.style.setProperty("overflow", "auto", "important");
      document.body.style.setProperty("overflow", "auto", "important");
      const selectors = ['#overlap-manager-root', '[class*="overlap-manager"]', '[class*="dialog-"]', '.tv-dialog__close', '.js-dialog__close', 'button[name="close"]', '.modal-backdrop'];
      selectors.forEach(sel => document.querySelectorAll(sel).forEach(el => el.remove()));
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    await page.setViewport({ width: 1920, height: 1080 });
    const success = await safeGoto(page, url);

    if (!success) {
        return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    const now = new Date();
    const dateString = buildDate(now.getDate(), now.getMonth() + 1, now.getFullYear());

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        const target = [...sections].find((section) => {
          const title = section.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA');
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!target) return ["INDICATOR NOT FOUND"];

        const spans = target.querySelectorAll(".valueValue-l31H9iuA");
        // Convert to array and filter out the '∅' if it somehow still exists
        return [...spans].map(s => {
            const val = s.innerText.trim();
            return val === "∅" ? "None" : val;
        });
      }
    );

    console.log(`[Success] Scraped: ${values.join(' | ')}`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];

  } catch (err) {
    console.error(`[Fatal] Scrape Error on ${url}:`, err.message);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}

function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

function buildDate(day, month, year) {
  return `${String(day).padStart(2, "0")}/${String(month).padStart(2, "0")}/${year}`;
}
