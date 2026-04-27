import ExcelJS from "exceljs";

function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
  }).format(date);
}

export async function generateGroupContributionLedgerWorkbookBuffer({
  groupName,
  contributionTypeLabel,
  year,
  generatedAt,
  monthsToDate,
  rows,
  totals,
}) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "CRC";
  workbook.lastModifiedBy = "CRC";
  workbook.created = new Date();
  workbook.modified = new Date();

  const worksheet = workbook.addWorksheet("Contribution Ledger", {
    views: [{ state: "frozen", ySplit: 5 }],
  });

  worksheet.mergeCells("A1:Q1");
  worksheet.getCell("A1").value = "Contribution Ledger";
  worksheet.getCell("A1").font = {
    bold: true,
    color: { argb: "FFFFFFFF" },
    size: 16,
  };
  worksheet.getCell("A1").alignment = {
    vertical: "middle",
    horizontal: "center",
  };
  worksheet.getCell("A1").fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FF0F766E" },
  };
  worksheet.getRow(1).height = 26;

  worksheet.mergeCells("A2:Q2");
  worksheet.getCell("A2").value = `${groupName} | ${contributionTypeLabel} | ${year}`;
  worksheet.getCell("A2").font = { bold: true, size: 11, color: { argb: "FF111827" } };
  worksheet.getCell("A2").alignment = { vertical: "middle", horizontal: "left" };

  worksheet.mergeCells("A3:Q3");
  worksheet.getCell("A3").value = `Generated ${formatDate(generatedAt)} | YTD window: ${monthsToDate} month(s)`;
  worksheet.getCell("A3").font = { size: 10, color: { argb: "FF6B7280" } };

  const summaryRow = worksheet.getRow(4);
  summaryRow.values = [
    "Members",
    rows.length,
    "YTD Total",
    Number(totals?.ytdTotal || 0),
    "Expected YTD",
    Number(totals?.expectedYtd || 0),
    "Arrears",
    Number(totals?.arrears || 0),
    "Status",
    totals?.status || "-",
  ];
  summaryRow.font = { bold: true };
  summaryRow.height = 22;
  [1, 3, 5, 7, 9].forEach((cellIndex) => {
    const cell = summaryRow.getCell(cellIndex);
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFF8FAFC" },
    };
  });
  [4, 6, 8].forEach((cellIndex) => {
    summaryRow.getCell(cellIndex).numFmt = `"NGN" #,##0`;
  });

  const columns = [
    { header: "S/N", key: "sn", width: 10 },
    { header: "Member Serial", key: "memberSerial", width: 18 },
    { header: "Member Name", key: "memberName", width: 28 },
    { header: "Units", key: "units", width: 12 },
    { header: "Jan", key: "jan", width: 12 },
    { header: "Feb", key: "feb", width: 12 },
    { header: "Mar", key: "mar", width: 12 },
    { header: "Apr", key: "apr", width: 12 },
    { header: "May", key: "may", width: 12 },
    { header: "Jun", key: "jun", width: 12 },
    { header: "Jul", key: "jul", width: 12 },
    { header: "Aug", key: "aug", width: 12 },
    { header: "Sep", key: "sep", width: 12 },
    { header: "Oct", key: "oct", width: 12 },
    { header: "Nov", key: "nov", width: 12 },
    { header: "Dec", key: "dec", width: 12 },
    { header: "YTD Total", key: "ytdTotal", width: 14 },
    { header: "Expected YTD", key: "expectedYtd", width: 15 },
    { header: "Arrears", key: "arrears", width: 14 },
    { header: "Status", key: "status", width: 14 },
  ];
  worksheet.columns = columns.map(({ key, width }) => ({ key, width }));

  const headerRow = worksheet.getRow(5);
  headerRow.values = columns.map((column) => column.header);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FF0F172A" },
  };
  headerRow.alignment = { vertical: "middle", horizontal: "center" };
  headerRow.height = 22;

  rows.forEach((row, index) => {
    worksheet.addRow({
      sn: index + 1,
      memberSerial: row.memberSerial || "-",
      memberName: row.memberName || "Member",
      units: row.units ?? "-",
      jan: Number(row.months?.[0] || 0),
      feb: Number(row.months?.[1] || 0),
      mar: Number(row.months?.[2] || 0),
      apr: Number(row.months?.[3] || 0),
      may: Number(row.months?.[4] || 0),
      jun: Number(row.months?.[5] || 0),
      jul: Number(row.months?.[6] || 0),
      aug: Number(row.months?.[7] || 0),
      sep: Number(row.months?.[8] || 0),
      oct: Number(row.months?.[9] || 0),
      nov: Number(row.months?.[10] || 0),
      dec: Number(row.months?.[11] || 0),
      ytdTotal: Number(row.ytdTotal || 0),
      expectedYtd: Number(row.expectedYtd || 0),
      arrears: Number(row.arrears || 0),
      status: row.status || "-",
    });
  });

  const totalsRow = worksheet.addRow({
    sn: "Totals",
    memberSerial: "-",
    memberName: `${rows.length} members`,
    units: Number(totals?.unitsTotal || 0) || "-",
    jan: Number(totals?.monthTotals?.[0] || 0),
    feb: Number(totals?.monthTotals?.[1] || 0),
    mar: Number(totals?.monthTotals?.[2] || 0),
    apr: Number(totals?.monthTotals?.[3] || 0),
    may: Number(totals?.monthTotals?.[4] || 0),
    jun: Number(totals?.monthTotals?.[5] || 0),
    jul: Number(totals?.monthTotals?.[6] || 0),
    aug: Number(totals?.monthTotals?.[7] || 0),
    sep: Number(totals?.monthTotals?.[8] || 0),
    oct: Number(totals?.monthTotals?.[9] || 0),
    nov: Number(totals?.monthTotals?.[10] || 0),
    dec: Number(totals?.monthTotals?.[11] || 0),
    ytdTotal: Number(totals?.ytdTotal || 0),
    expectedYtd: Number(totals?.expectedYtd || 0),
    arrears: Number(totals?.arrears || 0),
    status: totals?.status || "-",
  });
  totalsRow.font = { bold: true };
  totalsRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFE2E8F0" },
  };

  worksheet.autoFilter = {
    from: "A5",
    to: "T5",
  };

  worksheet.eachRow((row, rowNumber) => {
    row.alignment = { vertical: "middle" };
    if (rowNumber >= 6) {
      row.height = 20;
    }
  });

  const currencyColumns = [
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
  ];
  currencyColumns.forEach((columnKey) => {
    worksheet.getColumn(columnKey).numFmt = `"NGN" #,##0`;
  });

  worksheet.getColumn("D").alignment = { horizontal: "right" };
  worksheet.getColumn("A").alignment = { horizontal: "right" };
  currencyColumns.forEach((columnKey) => {
    worksheet.getColumn(columnKey).alignment = { horizontal: "right" };
  });

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}
