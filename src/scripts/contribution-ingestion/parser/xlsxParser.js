import fs from "fs/promises";
import path from "path";
import ExcelJS from "exceljs";

const MONTH_ALIASES = new Map([
  ["jan", 1],
  ["january", 1],
  ["feb", 2],
  ["february", 2],
  ["mar", 3],
  ["march", 3],
  ["apr", 4],
  ["april", 4],
  ["may", 5],
  ["jun", 6],
  ["june", 6],
  ["jul", 7],
  ["july", 7],
  ["aug", 8],
  ["august", 8],
  ["sep", 9],
  ["sept", 9],
  ["september", 9],
  ["oct", 10],
  ["october", 10],
  ["nov", 11],
  ["november", 11],
  ["dec", 12],
  ["december", 12],
]);

const normalizeHeader = (value) => {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) {
    return value.toISOString();
  }
  return String(value).trim().toLowerCase();
};

const normalizeCellValue = (value) => {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value;
  if (typeof value !== "object") return value;

  if (Object.prototype.hasOwnProperty.call(value, "result")) {
    return value.result ?? null;
  }
  if (Object.prototype.hasOwnProperty.call(value, "text")) {
    return value.text ?? null;
  }
  if (Array.isArray(value.richText)) {
    return value.richText.map((part) => part.text || "").join("");
  }
  if (Object.prototype.hasOwnProperty.call(value, "hyperlink")) {
    return value.text ?? value.hyperlink ?? null;
  }
  if (Object.prototype.hasOwnProperty.call(value, "error")) {
    return null;
  }

  return value;
};

const resolveMonthIndex = (value) => {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) {
    return value.getUTCMonth() + 1;
  }
  const raw = String(value).trim();
  if (!raw) return null;
  const text = raw.toLowerCase();

  for (const [alias, month] of MONTH_ALIASES.entries()) {
    if (text === alias || text.startsWith(`${alias} `) || text.includes(alias)) {
      return month;
    }
  }

  const numericMatch = text.match(/\b(1[0-2]|0?[1-9])\b/);
  if (numericMatch && (text === numericMatch[1] || /month/.test(text))) {
    const month = Number(numericMatch[1]);
    if (month >= 1 && month <= 12) return month;
  }

  const dateLike = new Date(raw);
  if (!Number.isNaN(dateLike.getTime())) {
    return dateLike.getUTCMonth() + 1;
  }

  return null;
};

const isMemberHeader = (value) => {
  const text = normalizeHeader(value);
  if (!text) return false;
  return /member|name|full name|participant/.test(text);
};

const isUnitsHeader = (value) => {
  const text = normalizeHeader(value);
  if (!text) return false;
  return /unit|units|share|slot/.test(text);
};

const detectHeaderRowIndex = (rows) => {
  const maxScan = Math.min(rows.length, 10);
  let bestIndex = 0;
  let bestScore = -1;

  for (let i = 0; i < maxScan; i += 1) {
    const row = rows[i] || [];
    let score = 0;
    row.forEach((cell) => {
      if (isMemberHeader(cell)) score += 3;
      if (isUnitsHeader(cell)) score += 2;
      if (resolveMonthIndex(cell)) score += 1;
    });

    if (score > bestScore) {
      bestScore = score;
      bestIndex = i;
    }
  }

  return bestIndex;
};

const mapHeaders = (headerRow) => {
  let memberIndex = null;
  let unitsIndex = null;
  const monthIndexes = new Map();
  const unmapped = [];

  headerRow.forEach((cell, index) => {
    if (memberIndex === null && isMemberHeader(cell)) {
      memberIndex = index;
      return;
    }
    if (unitsIndex === null && isUnitsHeader(cell)) {
      unitsIndex = index;
      return;
    }
    const monthIndex = resolveMonthIndex(cell);
    if (monthIndex) {
      monthIndexes.set(monthIndex, index);
      return;
    }
    unmapped.push({ index, value: cell });
  });

  return { memberIndex, unitsIndex, monthIndexes, unmapped };
};

const parseNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  const cleaned = String(value)
    .replace(/[,\s]/g, "")
    .replace(/[^0-9.-]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
};

const normalizeText = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).replace(/\s+/g, " ").trim();
};

export async function parseContributionWorkbook({
  inputPath,
  sheetName,
  headerRowIndex,
}) {
  const stat = await fs.stat(inputPath);
  if (!stat.isFile()) {
    throw new Error(`Input path is not a file: ${inputPath}`);
  }

  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(inputPath);

  const resolvedSheetName =
    sheetName || workbook.worksheets[0]?.name || null;
  if (!resolvedSheetName) {
    throw new Error("No worksheets found in workbook.");
  }

  const worksheet = workbook.getWorksheet(resolvedSheetName);
  if (!worksheet) {
    throw new Error(`Worksheet not found: ${resolvedSheetName}`);
  }

  const sheetValues = worksheet.getSheetValues();
  const rows = sheetValues.slice(1).map((row) => {
    if (!row) return [];
    if (!Array.isArray(row)) return [];
    return row.slice(1).map(normalizeCellValue);
  });

  const detectedHeaderIndex =
    Number.isFinite(headerRowIndex) && headerRowIndex >= 0
      ? headerRowIndex
      : detectHeaderRowIndex(rows);

  const headerRow = rows[detectedHeaderIndex] || [];
  const mapping = mapHeaders(headerRow);
  const warnings = [];

  if (mapping.memberIndex === null) {
    warnings.push("Member name column not detected; rows may be skipped.");
  }
  if (mapping.monthIndexes.size === 0) {
    warnings.push("No month columns detected; contributions will be empty.");
  }

  const dataRows = rows.slice(detectedHeaderIndex + 1);
  const parsedRows = [];
  let emptyRows = 0;

  dataRows.forEach((row, rowOffset) => {
    const trimmedRow = row.map(normalizeText);
    const isEmpty = trimmedRow.every((cell) => cell === "");
    if (isEmpty) {
      emptyRows += 1;
      return;
    }

    const nameValue = mapping.memberIndex !== null ? row[mapping.memberIndex] : null;
    const unitsValue = mapping.unitsIndex !== null ? row[mapping.unitsIndex] : null;

    const contributions = {};
    mapping.monthIndexes.forEach((colIndex, month) => {
      contributions[month] = parseNumber(row[colIndex]);
    });

    parsedRows.push({
      rowIndex: detectedHeaderIndex + 1 + rowOffset,
      rawRow: row,
      name: normalizeText(nameValue),
      units: parseNumber(unitsValue),
      contributions,
    });
  });

  const monthsDetected = Array.from(mapping.monthIndexes.keys()).sort((a, b) => a - b);

  return {
    meta: {
      inputPath: path.resolve(inputPath),
      sheetName: resolvedSheetName,
      fileName: path.basename(inputPath),
      totalRows: rows.length,
      headerRowIndex: detectedHeaderIndex,
      emptyRowsSkipped: emptyRows,
      monthsDetected,
      warnings,
    },
    mapping,
    rows: parsedRows,
  };
}
