// ✅ ULTIMATE REVISION: Handling GitHub Actions Throttling & "None" Values

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");
  
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);
      
      // Navigate and wait for the base layer
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

      // Kill blockers early
      await killPopups(page);

      // 1. Wait for any chart canvas to load
      await page.waitForSelector('canvas', { timeout: 30000 });

      // 2. Interaction: Simulate a user "activating" the chart
      const view = page.viewport();
      await page.mouse.move(view.width / 2, view.height / 2);
      await page.mouse.click(view.width / 2, view.height / 2);
      await delay(1000);

      // 3. Hover over the legend to trigger the data-ready event
      const legend = await page.$('[data-qa-id="legend"]');
      if (legend) {
        await legend.hover().catch(() => {});
      }

      console.log("Waiting for values to calculate (Avoid ∅)...");
      
      // 4. Heavy Wait: Wait specifically for digits to appear in the 'clubbed' study
      const dataLoaded = await page.waitForFunction(() => {
        const studies = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const clubbed = Array.from(studies).find(s => {
          const title = s.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
          return title === "clubbed" || title === "l";
        });
        
        if (!clubbed) return false;
        
        const firstVal = clubbed.querySelector(".valueValue-l31H9iuA")?.innerText || "";
        // Returns true only if it contains a number (0-9)
        return /\d/.test(firstVal);
      }, { timeout: 35000, polling: 1000 }).catch(() => false);

      if (!dataLoaded && i < retries - 1) {
        console.warn("Data didn't calculate. Retrying page load...");
        continue; 
      }

      await delay(2000); // Final settle
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
      const blockers = ['#overlap-manager-root', '[class*="overlap-manager"]', '[class*="dialog-"]', '.tv-dialog__close', '.js-dialog__close', 'button[name="close"]', '.modal-backdrop'];
      blockerSelectors = blockers.join(',');
      document.querySelectorAll(blockerSelectors).forEach(el => el.remove());
    });
  } catch (e) {}
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25;

  try {
    // Desktop Viewport
    await page.setViewport({ width: 1920, height: 1080 });
    
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    // Refresh popups check
    await killPopups(page);

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
        return [...spans].map(s => {
          const val = s.innerText.trim();
          // Return 'None' only if strictly ∅ or empty
          return (val === "∅" || val === "") ? "None" : val;
        });
      }
    );

    // If still None, log it for debugging
    if (values.every(v => v === "None")) {
        console.log(`[Alert] Row resulted in all None values.`);
    } else {
        console.log(`[Success] Scraped: ${values[0]}...`);
    }

    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];

  } catch (err) {
    console.error(`[Fatal] Error on ${url}:`, err.message);
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
