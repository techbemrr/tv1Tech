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

function cleanCell(v) {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  return s.replace(/\u200B|\u200C|\u200D|\uFEFF/g, "");
}

// Convert 1 -> A, 2 -> B, 27 -> AA ...
function colToLetter(n) {
  let s = "";
  while (n > 0) {
    const mod = (n - 1) % 26;
    s = String.fromCharCode(65 + mod) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// Build explicit range from startCol + row/col sizes
function buildRange(sheetName, startColLetter, startRowNumber, numRows, numCols) {
  const startColNum = startColLetter.charCodeAt(0) - 64; // A=1, B=2...
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
 * ✅ Clears + writes rows with an explicit range (most reliable).
 * startRow is 0-based index for your data row (same as your index.js globalIndex)
 */
export async function writeBulkValuesToSheet(startRow, rows) {
  if (!rows || rows.length === 0) return;

  // sanitize + ensure no sparse arrays
  const safeRows = rows.map((r) => (Array.isArray(r) ? r : []).map(cleanCell));

  const startRowNumber = startRow + 2; // because your data starts from row 2 in sheet
  const numRows = safeRows.length;
  const numCols = Math.max(...safeRows.map((r) => r.length), 1);

  // IMPORTANT: You are writing starting at column C
  const startCol = "C";

  // ✅ explicit overwrite range
  const writeRange = buildRange(
    process.env.OUTPUT_SHEET,
    startCol,
    startRowNumber,
    numRows,
    numCols
  );

  // ✅ clear first (removes old formulas/values causing weird ∅ displays)
  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.OUTPUT_SHEET_ID,
    range: writeRange,
  });

  // ✅ write data
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
  console.log(`📝 Attempting to write ${rows.length} rows starting from row ${startRow + 2}`);

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

  console.error(`Failed to write bulk rows starting at ${startRow + 2}`);
}
