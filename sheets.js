// sheets.js
import { google } from "googleapis";
import dotenv from "dotenv";
dotenv.config();

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    private_key: (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  },
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

// ✅ RAW sheet name (create this sheet in Google Sheets)
const RAW_SHEET_NAME = process.env.RAW_SHEET_NAME || "TV_RAW";

// ✅ where to write in RAW sheet
const RAW_START_COL = "A"; // A = first column
const RAW_START_ROW_OFFSET = 2; // start from row 2 (A2)

function cleanCell(v) {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  return s.replace(/\u200B|\u200C|\u200D|\uFEFF/g, "");
}

// 1->A, 2->B, 27->AA
function colToLetter(n) {
  let s = "";
  while (n > 0) {
    const mod = (n - 1) % 26;
    s = String.fromCharCode(65 + mod) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function buildRange(sheetName, startColLetter, startRowNumber, numRows, numCols) {
  const startColNum = startColLetter.charCodeAt(0) - 64;
  const endColNum = startColNum + numCols - 1;
  const endColLetter = colToLetter(endColNum);
  const endRow = startRowNumber + numRows - 1;
  return `${sheetName}!${startColLetter}${startRowNumber}:${endColLetter}${endRow}`;
}

export async function getChartLinks() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.SHEET_ID,
    range: `${process.env.SOURCE_SHEET}!D2:D`,
  });

  const values = res.data.values || [];
  return values
    .map((row) => (row?.[0] || ""))
    .map((url) => String(url).replace(/"/g, "").trim())
    .filter(Boolean);
}

/**
 * ✅ Writes RAW scraper output to a separate RAW sheet
 * This avoids formulas in your main output sheet (which show Ø/0).
 *
 * startRow is 0-based index from your script.
 */
export async function writeBulkValuesToSheet(startRow, rows) {
  if (!rows || rows.length === 0) return;

  // sanitize
  const safeRows = rows.map((r) => (Array.isArray(r) ? r : []).map(cleanCell));

  const startRowNumber = startRow + RAW_START_ROW_OFFSET; // A2 corresponds to startRow=0
  const numRows = safeRows.length;
  const numCols = Math.max(...safeRows.map((r) => r.length), 1);

  const writeRange = buildRange(
    RAW_SHEET_NAME,
    RAW_START_COL,
    startRowNumber,
    numRows,
    numCols
  );

  // clear then write (clean overwrite)
  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.OUTPUT_SHEET_ID,
    range: writeRange,
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.OUTPUT_SHEET_ID,
    range: writeRange,
    valueInputOption: "RAW",
    requestBody: {
      majorDimension: "ROWS",
      values: safeRows,
    },
  });
}

export async function writeBulkWithRetry(startRow, rows, retries = 5) {
  console.log(
    `📝 Attempting to write ${rows.length} rows starting from RAW row ${startRow + RAW_START_ROW_OFFSET}`
  );

  const delay = (ms) => new Promise((res) => setTimeout(res, ms));

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await writeBulkValuesToSheet(startRow, rows);
      return;
    } catch (err) {
      const msg = err?.message || JSON.stringify(err);
      if (msg.includes("Quota exceeded") || msg.includes("USER_RATE_LIMIT_EXCEEDED")) {
        const wait = 1000 * Math.pow(2, attempt);
        console.warn(`Bulk quota exceeded. Retrying in ${wait / 1000}s...`);
        await delay(wait);
      } else {
        throw err;
      }
    }
  }

  console.error(`Failed to write bulk rows starting at RAW row ${startRow + RAW_START_ROW_OFFSET}`);
}
