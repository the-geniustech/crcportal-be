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
  return value;
};

const normalizeText = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).replace(/\s+/g, " ").trim();
};

const normalizeHeader = (value) => normalizeText(value).toLowerCase();

const parseNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const cleaned = String(value)
    .replace(/[,\s]/g, "")
    .replace(/[^0-9.-]/g, "");
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const resolveMonthIndex = (value) => {
  const normalized = normalizeHeader(value);
  if (!normalized) return null;
  for (const [alias, month] of MONTH_ALIASES.entries()) {
    if (normalized === alias) return month;
  }
  return null;
};

const isNameHeader = (value) => /^(name|member|member name|full name)$/i.test(
  normalizeText(value),
);
const isSerialHeader = (value) =>
  /^(serial|serial number|serialnumber|member serial)$/i.test(
    normalizeText(value),
  );
const isUnitHeader = (value) => /^(unit|units)$/i.test(normalizeText(value));
const isTotalHeader = (value) => /^total$/i.test(normalizeText(value));
const isGrandTotalHeader = (value) =>
  /^grand total$/i.test(normalizeText(value));

const detectHeaderRowIndex = (rows) => {
  const maxRows = Math.min(rows.length, 10);
  let bestIndex = 0;
  let bestScore = -1;

  for (let index = 0; index < maxRows; index += 1) {
    const row = rows[index] || [];
    let score = 0;
    row.forEach((cell) => {
      if (isNameHeader(cell)) score += 3;
      if (isSerialHeader(cell)) score += 4;
      if (isUnitHeader(cell)) score += 2;
      if (resolveMonthIndex(cell)) score += 1;
      if (isTotalHeader(cell) || isGrandTotalHeader(cell)) score += 1;
    });

    if (score > bestScore) {
      bestScore = score;
      bestIndex = index;
    }
  }

  return bestIndex;
};

const mapHeaders = (headerRow) => {
  let nameIndex = null;
  let serialIndex = null;
  let unitIndex = null;
  let totalIndex = null;
  let grandTotalIndex = null;
  const monthIndexes = new Map();

  headerRow.forEach((cell, index) => {
    if (nameIndex === null && isNameHeader(cell)) {
      nameIndex = index;
      return;
    }
    if (serialIndex === null && isSerialHeader(cell)) {
      serialIndex = index;
      return;
    }
    if (unitIndex === null && isUnitHeader(cell)) {
      unitIndex = index;
      return;
    }
    if (totalIndex === null && isTotalHeader(cell)) {
      totalIndex = index;
      return;
    }
    if (grandTotalIndex === null && isGrandTotalHeader(cell)) {
      grandTotalIndex = index;
      return;
    }
    const month = resolveMonthIndex(cell);
    if (month && !monthIndexes.has(month)) {
      monthIndexes.set(month, index);
    }
  });

  return {
    nameIndex,
    serialIndex,
    unitIndex,
    totalIndex,
    grandTotalIndex,
    monthIndexes,
  };
};

const isSkippableRow = (row, mapping) => {
  const nameValue =
    mapping.nameIndex !== null ? normalizeText(row[mapping.nameIndex]) : "";
  const serialValue =
    mapping.serialIndex !== null ? normalizeText(row[mapping.serialIndex]) : "";
  const hasMonthValue = Array.from(mapping.monthIndexes.values()).some(
    (columnIndex) => parseNumber(row[columnIndex]) !== null,
  );

  if (serialValue) return false;
  if (!nameValue && !hasMonthValue) return true;
  return /^total$/i.test(nameValue);
};

export async function parseSpecialContributionWorkbook({
  inputPath,
  sheetName = null,
  headerRowIndex = null,
}) {
  const stat = await fs.stat(inputPath);
  if (!stat.isFile()) {
    throw new Error(`Input path is not a file: ${inputPath}`);
  }

  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(inputPath);

  const worksheet =
    (sheetName && workbook.getWorksheet(sheetName)) || workbook.worksheets[0];
  if (!worksheet) {
    throw new Error(`No worksheet found in workbook: ${inputPath}`);
  }

  const sheetValues = worksheet.getSheetValues();
  const rows = sheetValues.slice(1).map((row) => {
    if (!Array.isArray(row)) return [];
    return row.slice(1).map(normalizeCellValue);
  });

  const detectedHeaderRowIndex =
    Number.isFinite(headerRowIndex) && headerRowIndex >= 0
      ? headerRowIndex
      : detectHeaderRowIndex(rows);

  const headerRow = rows[detectedHeaderRowIndex] || [];
  const mapping = mapHeaders(headerRow);
  const warnings = [];

  if (mapping.serialIndex === null) {
    warnings.push("Serial column was not detected.");
  }
  if (mapping.unitIndex === null) {
    warnings.push("Unit column was not detected.");
  }
  if (mapping.monthIndexes.size === 0) {
    warnings.push("No monthly contribution columns were detected.");
  }

  const dataRows = [];
  const rawRows = rows.slice(detectedHeaderRowIndex + 1);

  rawRows.forEach((row, index) => {
    if (isSkippableRow(row, mapping)) return;

    const contributions = {};
    mapping.monthIndexes.forEach((columnIndex, month) => {
      contributions[month] = parseNumber(row[columnIndex]);
    });

    dataRows.push({
      rowIndex: detectedHeaderRowIndex + 2 + index,
      name: mapping.nameIndex !== null ? normalizeText(row[mapping.nameIndex]) : "",
      serial:
        mapping.serialIndex !== null ? normalizeText(row[mapping.serialIndex]) : "",
      units: mapping.unitIndex !== null ? parseNumber(row[mapping.unitIndex]) : null,
      total: mapping.totalIndex !== null ? parseNumber(row[mapping.totalIndex]) : null,
      grandTotal:
        mapping.grandTotalIndex !== null
          ? parseNumber(row[mapping.grandTotalIndex])
          : null,
      contributions,
      rawRow: row,
    });
  });

  return {
    meta: {
      inputPath: path.resolve(inputPath),
      fileName: path.basename(inputPath),
      sheetName: worksheet.name,
      headerRowIndex: detectedHeaderRowIndex,
      monthsDetected: Array.from(mapping.monthIndexes.keys()).sort((a, b) => a - b),
      totalRows: rows.length,
      parsedRows: dataRows.length,
      warnings,
    },
    mapping,
    rows: dataRows,
  };
}
