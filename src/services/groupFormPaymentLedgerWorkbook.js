import ExcelJS from "exceljs";

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

function getColumnLetter(index) {
  let value = index;
  let letters = "";

  while (value > 0) {
    const remainder = (value - 1) % 26;
    letters = String.fromCharCode(65 + remainder) + letters;
    value = Math.floor((value - 1) / 26);
  }

  return letters;
}

export async function generateGroupFormPaymentLedgerWorkbookBuffer({
  groupName,
  scopeLabel,
  generatedAt,
  rows,
  summary,
  includeGroupName = false,
}) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "CRC";
  workbook.lastModifiedBy = "CRC";
  workbook.created = new Date();
  workbook.modified = new Date();

  const worksheet = workbook.addWorksheet("Form Payment Ledger", {
    views: [{ state: "frozen", ySplit: 5 }],
  });

  const columns = [
    { header: "Member Name", key: "memberName", width: 30 },
    ...(includeGroupName
      ? [{ header: "Group Name", key: "groupName", width: 30 }]
      : []),
    { header: "Email", key: "memberEmail", width: 34 },
    { header: "Phone", key: "memberPhone", width: 19 },
    { header: "Form Type", key: "formLabel", width: 40 },
    { header: "Amount", key: "amount", width: 16 },
    { header: "Status", key: "paymentStatus", width: 15 },
    { header: "Submitted", key: "submittedAt", width: 18 },
    { header: "Reviewed", key: "reviewedAt", width: 18 },
    { header: "Source Ref", key: "sourceReference", width: 30 },
    { header: "Transaction Ref", key: "transactionReference", width: 52 },
  ];
  const lastColumn = getColumnLetter(columns.length);
  const amountColumnIndex =
    columns.findIndex((column) => column.key === "amount") + 1;

  worksheet.mergeCells(`A1:${lastColumn}1`);
  worksheet.getCell("A1").value = "Form Payment Ledger";
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
  worksheet.getRow(1).height = 28;

  worksheet.mergeCells(`A2:${lastColumn}2`);
  worksheet.getCell("A2").value = `${groupName} | ${scopeLabel}`;
  worksheet.getCell("A2").font = {
    bold: true,
    size: 11,
    color: { argb: "FF111827" },
  };

  worksheet.mergeCells(`A3:${lastColumn}3`);
  worksheet.getCell("A3").value =
    `Generated ${formatDate(generatedAt)} | ${rows.length} form payment record(s)`;
  worksheet.getCell("A3").font = { size: 10, color: { argb: "FF6B7280" } };

  const summaryRow = worksheet.getRow(4);
  summaryRow.values = [
    "Records",
    Number(summary?.totalRecords || 0),
    "Total Expected",
    Number(summary?.totalAmount || 0),
    "Paid",
    Number(summary?.paidAmount || 0),
    "Pending",
    Number(summary?.pendingAmount || 0),
    "Defaulted",
    Number(summary?.defaultedAmount || 0),
  ];
  summaryRow.font = { bold: true };
  summaryRow.height = 22;
  [1, 3, 5, 7, 9].forEach((cellIndex) => {
    summaryRow.getCell(cellIndex).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFF8FAFC" },
    };
  });
  [4, 6, 8, 10].forEach((cellIndex) => {
    summaryRow.getCell(cellIndex).numFmt = `"NGN" #,##0`;
  });

  worksheet.columns = columns.map(({ key, width }) => ({ key, width }));

  const headerRow = worksheet.getRow(5);
  headerRow.values = columns.map((column) => column.header);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FF0F172A" },
  };
  headerRow.alignment = {
    vertical: "middle",
    horizontal: "center",
    wrapText: false,
  };
  headerRow.height = 26;

  rows.forEach((row) => {
    worksheet.addRow({
      memberName: row.memberName || "Unknown member",
      groupName: row.groupName || "-",
      memberEmail: row.memberEmail || "-",
      memberPhone: row.memberPhone || "-",
      formLabel: row.formLabel || "-",
      amount: Number(row.amount || 0),
      paymentStatus: row.paymentStatus || "-",
      submittedAt: row.submittedAt || "-",
      reviewedAt: row.reviewedAt || "-",
      sourceReference: row.sourceReference || "-",
      transactionReference: row.transactionReference || "-",
    });
  });

  const totalsRow = worksheet.addRow({
    memberName: "Totals",
    groupName: includeGroupName ? "All groups" : undefined,
    formLabel: `${rows.length} records`,
    amount: Number(summary?.totalAmount || 0),
    paymentStatus: `${summary?.paidCount || 0} paid`,
    submittedAt: `${summary?.pendingCount || 0} pending`,
    reviewedAt: `${summary?.defaultedCount || 0} defaulted`,
  });
  totalsRow.font = { bold: true };
  totalsRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFE2E8F0" },
  };

  worksheet.autoFilter = {
    from: "A5",
    to: `${lastColumn}5`,
  };

  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber < 6) return;
    row.height = 28;
    row.eachCell({ includeEmpty: true }, (cell, cellNumber) => {
      cell.alignment = {
        vertical: "middle",
        horizontal: cellNumber === amountColumnIndex ? "right" : "left",
        wrapText: false,
      };
    });
  });

  worksheet.getColumn(amountColumnIndex).numFmt = `"NGN" #,##0`;
  worksheet.getColumn(amountColumnIndex).alignment = {
    horizontal: "right",
    vertical: "middle",
    wrapText: false,
  };

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}
