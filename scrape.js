// scrape.js
export async function scrapeChart(page, url) {
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

    await page.waitForSelector('[data-name="legend"]', { timeout: 15000 });

    await page.waitForFunction(
      () => {
        const sections = document.querySelectorAll(
          '[data-name="legend"] .item-l31H9iuA.study-l31H9iuA'
        );
        for (const section of sections) {
          const title = section.querySelector(
            '[data-name="legend-source-title"] .title-l31H9iuA'
          );
          if (title?.innerText?.toLowerCase() === "clubbed") {
            const values = section.querySelectorAll(".valueValue-l31H9iuA");
            return Array.from(values).some(
              (el) => el.innerText.trim() && el.innerText.trim() !== "∅"
            );
          }
        }
        return false;
      },
      { timeout: 15000 }
    );

    // Filter and extract values ONLY under the CLUBBED title
    const values = await page.$$eval(
      '[data-name="legend"] .item-l31H9iuA.study-l31H9iuA', // studies like CLUBBED
      (studySections) => {
        const clubbed = [...studySections].find((section) => {
          const titleDiv = section.querySelector(
            '[data-name="legend-source-title"] .title-l31H9iuA'
          );
          return titleDiv?.innerText?.toLowerCase() === "clubbed";
        });

        if (!clubbed) return ["CLUBBED NOT FOUND"];

        const valueSpans = clubbed.querySelectorAll(".valueValue-l31H9iuA");
        return [...valueSpans].map((el) => el.innerText.trim());
      }
    );

    return values;
  } catch (err) {
    console.error(`❌ Error scraping ${url}:`, err.message);
    return ["ERROR"];
  }
}
