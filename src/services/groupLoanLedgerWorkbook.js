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

export async function generateGroupLoanLedgerWorkbookBuffer({
  groupName,
  loanTypeLabel,
  statusLabel,
  generatedAt,
  rows,
  summary,
}) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "CRC";
  workbook.lastModifiedBy = "CRC";
  workbook.created = new Date();
  workbook.modified = new Date();

  const worksheet = workbook.addWorksheet("Loan Ledger", {
    views: [{ state: "frozen", ySplit: 5 }],
  });

  worksheet.mergeCells("A1:Q1");
  worksheet.getCell("A1").value = "Loan Ledger";
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
  worksheet.getCell("A2").value = `${groupName} | ${loanTypeLabel} | ${statusLabel}`;
  worksheet.getCell("A2").font = { bold: true, size: 11, color: { argb: "FF111827" } };

  worksheet.mergeCells("A3:Q3");
  worksheet.getCell("A3").value = `Generated ${formatDate(generatedAt)} | ${rows.length} loan record(s)`;
  worksheet.getCell("A3").font = { size: 10, color: { argb: "FF6B7280" } };

  const summaryRow = worksheet.getRow(4);
  summaryRow.values = [
    "Loans",
    rows.length,
    "Principal Volume",
    Number(summary?.principal || 0),
    "Outstanding",
    Number(summary?.outstanding || 0),
    "Repaid So Far",
    Number(summary?.repaid || 0),
    "Patronage",
    Number(summary?.patronage || 0),
  ];
  summaryRow.font = { bold: true };
  [1, 3, 5, 7, 9].forEach((cellIndex) => {
    summaryRow.getCell(cellIndex).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFF8FAFC" },
    };
  });
  [4, 6, 8, 10].forEach((cellIndex) => {
    summaryRow.getCell(cellIndex).numFmt = `"NGN" #,##0.00`;
  });

  const columns = [
    { header: "Loan Code", key: "loanCode", width: 18 },
    { header: "Loan Type", key: "loanType", width: 18 },
    { header: "Borrower Name", key: "borrowerName", width: 24 },
    { header: "Membership Serial", key: "memberSerial", width: 18 },
    { header: "Borrower Email", key: "borrowerEmail", width: 28 },
    { header: "Borrower Phone", key: "borrowerPhone", width: 18 },
    { header: "Principal", key: "principal", width: 14 },
    { header: "Interest Rate", key: "interestRate", width: 14 },
    { header: "Remaining Principal Due", key: "remainingPrincipal", width: 18 },
    { header: "Remaining Interest Due", key: "remainingInterest", width: 18 },
    { header: "Principal Paid", key: "repaidPrincipal", width: 15 },
    { header: "Interest Paid", key: "repaidInterest", width: 15 },
    { header: "Interest Patronage", key: "patronage", width: 17 },
    { header: "Status", key: "status", width: 14 },
    { header: "Progress", key: "progress", width: 12 },
    { header: "Disbursed At", key: "disbursedAt", width: 16 },
    { header: "Updated At", key: "updatedAt", width: 16 },
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

  rows.forEach((row) => {
    worksheet.addRow({
      loanCode: row.loanCode,
      loanType: row.loanType,
      borrowerName: row.borrowerName,
      memberSerial: row.memberSerial,
      borrowerEmail: row.borrowerEmail,
      borrowerPhone: row.borrowerPhone,
      principal: Number(row.principal || 0),
      interestRate: row.interestRate,
      remainingPrincipal: Number(row.remainingPrincipal || 0),
      remainingInterest: Number(row.remainingInterest || 0),
      repaidPrincipal: Number(row.repaidPrincipal || 0),
      repaidInterest: Number(row.repaidInterest || 0),
      patronage: Number(row.patronage || 0),
      status: row.status,
      progress: row.progress,
      disbursedAt: row.disbursedAt,
      updatedAt: row.updatedAt,
    });
  });

  const totalsRow = worksheet.addRow({
    loanCode: "Totals",
    loanType: `${rows.length} loans`,
    principal: Number(summary?.principal || 0),
    remainingPrincipal: Number(summary?.remainingPrincipal || 0),
    remainingInterest: Number(summary?.remainingInterest || 0),
    repaidPrincipal: Number(summary?.repaidPrincipal || 0),
    repaidInterest: Number(summary?.repaidInterest || 0),
    patronage: Number(summary?.patronage || 0),
    status: summary?.statusLabel || "-",
  });
  totalsRow.font = { bold: true };
  totalsRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFE2E8F0" },
  };

  worksheet.autoFilter = {
    from: "A5",
    to: "Q5",
  };

  worksheet.eachRow((row, rowNumber) => {
    row.alignment = { vertical: "middle" };
    if (rowNumber >= 6) row.height = 20;
  });

  ["G", "I", "J", "K", "L", "M"].forEach((columnKey) => {
    worksheet.getColumn(columnKey).numFmt = `"NGN" #,##0.00`;
    worksheet.getColumn(columnKey).alignment = { horizontal: "right" };
  });

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}
