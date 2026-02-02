import puppeteer from "puppeteer";
import fs from "fs";
import { login } from "./login.js";
import { scrapeChart } from "./scrape.js";
import { getChartLinks, writeBulkWithRetry } from "./sheets.js";

const COOKIE_PATH = "./cookies.json";
const TV_HOME = "https://www.tradingview.com/";

function normalizeTvUrl(url) {
  if (!url) return url;
  return url.replace("https://in.tradingview.com", "https://www.tradingview.com");
}

if (process.env.COOKIES_BASE64 && !fs.existsSync(COOKIE_PATH)) {
  const decoded = Buffer.from(process.env.COOKIES_BASE64, "base64").toString("utf-8");
  fs.writeFileSync(COOKIE_PATH, decoded);
  console.log("cookies.json restored from Base64");
}

async function saveCookies(cookies) {
  fs.writeFileSync(COOKIE_PATH, JSON.stringify(cookies, null, 2));
}

async function loadCookies(page) {
  if (!fs.existsSync(COOKIE_PATH)) return false;

  // ✅ MUST visit domain before setCookie
  await page.goto(TV_HOME, { waitUntil: "domcontentloaded", timeout: 60000 });

  const cookies = JSON.parse(fs.readFileSync(COOKIE_PATH, "utf-8"));
  await page.setCookie(...cookies);

  // ✅ reload once so cookies take effect
  await page.goto(TV_HOME, { waitUntil: "domcontentloaded", timeout: 60000 });
  return true;
}

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

(async () => {
  const chartLinks = await getChartLinks();

  const BATCH_INDEX = parseInt(process.argv[2] || process.env.BATCH_INDEX || "0", 10);
  const ACCOUNT_START = parseInt(process.env.ACCOUNT_START || "0", 10);
  const ACCOUNT_END = parseInt(process.env.ACCOUNT_END || chartLinks.length.toString(), 10);
  const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);

  const accountLinks = chartLinks.slice(ACCOUNT_START, ACCOUNT_END);
  const start = BATCH_INDEX * BATCH_SIZE;
  const end = start + BATCH_SIZE;
  const batchLinks = accountLinks.slice(start, end);

  console.log(`Account range: ${ACCOUNT_START}–${ACCOUNT_END}`);
  console.log(`Processing batch ${BATCH_INDEX}: ${start} to ${end}`);

  const browser = await puppeteer.launch({
    headless: "new",
    slowMo: 0,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });

  let page = await browser.newPage();

  // ✅ set UA once
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
  );
  await page.setExtraHTTPHeaders({ "accept-language": "en-US,en;q=0.9" });

  // ✅ apply cookies OR login
  const hasCookies = await loadCookies(page);
  if (!hasCookies) {
    try {
      const cookies = await login(page);
      await saveCookies(cookies);
    } catch (err) {
      console.error("Login failed:", err.message);
      await browser.close();
      process.exit(1);
    }
  }

  let rowBuffer = [];
  let startRow = -1;

  for (let i = 0; i < batchLinks.length; i++) {
    let url = normalizeTvUrl(batchLinks[i]);
    if (!url) continue;

    const globalIndex = ACCOUNT_START + BATCH_INDEX * BATCH_SIZE + i;
    console.log(`Scraping Row ${globalIndex + 2}: ${url}`);

    try {
      // scrapeChart returns: [month, day, ...indicatorValues]
      const scraped = await scrapeChart(page, url);

      const month = scraped[0];
      const day = scraped[1];
      const indicatorValues = scraped.slice(2);

      // ✅ your fixed year
      const date = `${day}/${month}/2025`;

      const rowData = [date, ...indicatorValues];

      if (rowBuffer.length === 0) startRow = globalIndex;
      rowBuffer.push(rowData);

      // ✅ Write in bulk every 10 rows
      if (rowBuffer.length === 10) {
        await writeBulkWithRetry(startRow, rowBuffer);
        rowBuffer = [];
        startRow = -1;
      }
    } catch (err) {
      console.error(`Error scraping ${url}:`, err.message);
    }

    // restart page every 100 rows
    if ((i + 1) % 100 === 0) {
      console.log("Restarting page to clear memory...");
      await page.close();
      page = await browser.newPage();
      await page.setUserAgent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
      );
      await page.setExtraHTTPHeaders({ "accept-language": "en-US,en;q=0.9" });

      // ✅ MUST reapply cookies correctly on new page
      await loadCookies(page);
    }

    await delay(700);
  }

  // write remaining
  if (rowBuffer.length > 0) {
    console.log(`🧹 Writing remaining ${rowBuffer.length} rows starting from ${startRow + 2}`);
    await writeBulkWithRetry(startRow, rowBuffer);
  }

  await browser.close();
})();
