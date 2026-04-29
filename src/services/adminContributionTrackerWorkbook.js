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

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function autoSizeColumns(worksheet, minWidth = 12, maxWidth = 40) {
  worksheet.columns.forEach((column) => {
    let width = minWidth;

    column.eachCell({ includeEmpty: true }, (cell) => {
      const raw = cell.value;
      let text = "";

      if (raw === null || typeof raw === "undefined") {
        text = "";
      } else if (typeof raw === "object" && raw && "richText" in raw) {
        text = raw.richText.map((segment) => segment.text || "").join("");
      } else if (typeof raw === "object" && raw && "text" in raw) {
        text = String(raw.text || "");
      } else {
        text = String(raw);
      }

      const lineWidth = text
        .split(/\r?\n/)
        .reduce((max, line) => Math.max(max, line.length), 0);
      width = Math.max(width, lineWidth + 2);
    });

    column.width = Math.min(maxWidth, Math.max(minWidth, width));
  });
}

export async function generateAdminContributionTrackerWorkbookBuffer({
  contributionTypeLabel,
  periodLabel,
  generatedAt,
  groupLabel,
  statusLabel,
  searchLabel,
  sortLabel,
  records,
  summary,
}) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "CRC";
  workbook.lastModifiedBy = "CRC";
  workbook.created = new Date();
  workbook.modified = new Date();

  const worksheet = workbook.addWorksheet("Contribution Tracker", {
    views: [{ state: "frozen", ySplit: 8 }],
  });

  worksheet.mergeCells("A1:I1");
  worksheet.getCell("A1").value = "Contribution Tracker Export";
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

  worksheet.mergeCells("A2:I2");
  worksheet.getCell("A2").value = `${contributionTypeLabel} | ${periodLabel}`;
  worksheet.getCell("A2").font = { bold: true, size: 11, color: { argb: "FF111827" } };

  worksheet.mergeCells("A3:I3");
  worksheet.getCell("A3").value = `Generated ${formatDateTime(generatedAt)}`;
  worksheet.getCell("A3").font = { size: 10, color: { argb: "FF6B7280" } };

  const metaRows = [
    ["Group Filter", groupLabel, "Status Filter", statusLabel, "Sort", sortLabel],
    ["Search", searchLabel, "Records", records.length, "Collection Rate", `${Number(summary?.collectionRate || 0).toFixed(1)}%`],
    [
      "Total Expected",
      Number(summary?.totalExpected || 0),
      "Total Collected",
      Number(summary?.totalPaid || 0),
      "Defaulters",
      Number(summary?.defaulters || 0),
    ],
  ];

  metaRows.forEach((values, index) => {
    const rowNumber = 4 + index;
    const row = worksheet.getRow(rowNumber);
    row.values = values;
    row.height = 22;

    [1, 3, 5].forEach((cellIndex) => {
      const cell = row.getCell(cellIndex);
      cell.font = { bold: true, color: { argb: "FF0F172A" } };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFF1F5F9" },
      };
      cell.alignment = { vertical: "middle", horizontal: "left" };
    });
  });

  worksheet.getRow(6).getCell(2).numFmt = `"NGN" #,##0`;
  worksheet.getRow(6).getCell(4).numFmt = `"NGN" #,##0`;

  const columns = [
    { header: "S/N", key: "sn" },
    { header: "Member Serial", key: "memberSerial" },
    { header: "Member Name", key: "memberName" },
    { header: "Group", key: "groupName" },
    { header: "Expected", key: "expectedAmount" },
    { header: "Paid", key: "paidAmount" },
    { header: "Due Date", key: "dueDate" },
    { header: "Status", key: "status" },
    { header: "Months Defaulted", key: "monthsDefaulted" },
  ];

  worksheet.columns = columns.map((column) => ({ key: column.key, width: 12 }));

  const headerRow = worksheet.getRow(8);
  headerRow.values = columns.map((column) => column.header);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FF0F172A" },
  };
  headerRow.alignment = { vertical: "middle", horizontal: "center" };
  headerRow.height = 22;

  records.forEach((record, index) => {
    worksheet.addRow({
      sn: index + 1,
      memberSerial: record.memberSerial || "-",
      memberName: record.memberName || "Member",
      groupName: record.groupName || "Group",
      expectedAmount: Number(record.expectedAmount || 0),
      paidAmount: Number(record.paidAmount || 0),
      dueDate: record.dueDate ? formatDate(record.dueDate) : "Anytime",
      status: String(record.status || "-").toUpperCase(),
      monthsDefaulted: Number(record.monthsDefaulted || 0),
    });
  });

  const totalsRow = worksheet.addRow({
    sn: "Totals",
    memberSerial: "-",
    memberName: `${records.length} records`,
    groupName: groupLabel,
    expectedAmount: Number(summary?.totalExpected || 0),
    paidAmount: Number(summary?.totalPaid || 0),
    dueDate: "-",
    status: Number(summary?.defaulters || 0) > 0 ? "DEFAULTERS PRESENT" : "CLEAR",
    monthsDefaulted: Number(summary?.defaulters || 0),
  });
  totalsRow.font = { bold: true };
  totalsRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFE2E8F0" },
  };

  worksheet.autoFilter = {
    from: "A8",
    to: "I8",
  };

  worksheet.eachRow((row, rowNumber) => {
    row.alignment = { vertical: "middle" };
    if (rowNumber >= 9) {
      row.height = 20;
    }
  });

  ["E", "F"].forEach((columnKey) => {
    worksheet.getColumn(columnKey).numFmt = `"NGN" #,##0`;
    worksheet.getColumn(columnKey).alignment = { horizontal: "right" };
  });
  worksheet.getColumn("A").alignment = { horizontal: "right" };
  worksheet.getColumn("I").alignment = { horizontal: "right" };

  autoSizeColumns(worksheet);

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}
