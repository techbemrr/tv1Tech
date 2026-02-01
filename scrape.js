// ✅ FINAL REVISION: Force-Triggering Legend Values
// This version moves the mouse and clicks to "wake up" the TradingView data engine.

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");
  
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      await killPopups(page);

      // Wait for Chart Engine
      await page.waitForSelector('canvas', { timeout: 20000 });

      // ✅ ACTION: Move mouse to center and click to "activate" the chart
      const { width, height } = page.viewport();
      await page.mouse.move(width / 2, height / 2);
      await page.mouse.click(width / 2, height / 2);
      
      // ✅ ACTION: Specifically click near the legend to force calculation
      await page.click('[data-qa-id="legend"]').catch(() => {});

      // Wait for the legend values to change from '∅' to actual numbers
      console.log("Waiting for data to calculate...");
      await page.waitForFunction(() => {
        const items = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const target = Array.from(items).find(el => {
            const t = el.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
            return t === "clubbed" || t === "l";
        });
        if (!target) return false;
        const firstVal = target.querySelector(".valueValue-l31H9iuA")?.innerText;
        // Success if value is a number, has a decimal, or is not the null symbol
        return firstVal && firstVal !== "∅" && firstVal !== "" && /[0-9]/.test(firstVal);
      }, { timeout: 20000 }).catch(() => console.log("Values still loading, proceeding to grab best available..."));

      await delay(2000); 
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

    // Final check for popups that might have appeared after clicking the chart
    await killPopups(page);

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
        return [...spans].map(s => {
            const val = s.innerText.trim();
            return val === "∅" ? "None" : val;
        });
      }
    );

    console.log(`[Success] Scraped: ${values.slice(0, 5).join(' | ')} ...`);
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
