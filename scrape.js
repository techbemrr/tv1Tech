// ✅ FINAL CALCULATION FIX: Forcing Technical Analysis Engine to Run

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function safeGoto(page, url, retries = 3) {
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36");
  
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[Navigation] Attempt ${i + 1}: ${url}`);
      await page.goto(url, { waitUntil: "load", timeout: 60000 });

      await killPopups(page);

      // 1. Wait for Chart
      await page.waitForSelector('canvas', { timeout: 30000 });

      // 2. FORCE RE-RENDER: TradingView often freezes studies in headless mode.
      // We simulate a window resize and a mouse "scrub" across the chart.
      await page.setViewport({ width: 1921, height: 1081 }); // Trigger resize
      await delay(500);
      await page.setViewport({ width: 1920, height: 1080 });
      
      const view = page.viewport();
      await page.mouse.move(view.width / 2, view.height / 2);
      await page.mouse.down();
      await page.mouse.move(view.width / 2 + 50, view.height / 2); // Small drag
      await page.mouse.up();

      console.log("Waiting for values to calculate (Avoid ∅)...");
      
      // 3. Wait specifically for a number to appear in the study legend
      const dataLoaded = await page.waitForFunction(() => {
        const studies = document.querySelectorAll('[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA');
        const clubbed = Array.from(studies).find(s => {
          const title = s.querySelector('[data-qa-id="legend-source-title"] .title-l31H9iuA')?.innerText?.toLowerCase();
          return title === "clubbed" || title === "l";
        });
        
        if (!clubbed) return false;
        
        const firstVal = clubbed.querySelector(".valueValue-l31H9iuA")?.innerText || "";
        // Check if it's a number, a decimal point, or a minus sign (ignores ∅ and n/a)
        return /[0-9.-]/.test(firstVal);
      }, { timeout: 35000, polling: 1000 }).catch(() => false);

      if (!dataLoaded && i < retries - 1) {
        console.warn("Values stayed ∅. Retrying with full reload...");
        continue; 
      }

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
      document.querySelectorAll(blockers.join(',')).forEach(el => el.remove());
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

    await delay(1000); // Tiny wait to let all 25 values sync
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
          return (val === "∅" || val === "" || val === "n/a") ? "None" : val;
        });
      }
    );

    console.log(`[Success] First 3 values: ${values.slice(0, 3).join(', ')}`);
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];

  } catch (err) {
    console.error(`[Fatal] Error:`, err.message);
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
