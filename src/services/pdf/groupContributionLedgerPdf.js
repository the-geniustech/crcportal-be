import PDFDocument from "pdfkit";

const LAYOUT = {
  headerBarHeight: 38,
  headerGap: 1.0,
  headerMetaGap: 0.2,
  summaryCardHeight: 30,
  summaryCardGap: 6,
  summaryBottomGap: 6,
  tableHeaderHeight: 12,
  tableRowHeight: 11,
  tableRowPadding: 2.5,
  footerGap: 8,
};

function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
}

function formatTableCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value) || value <= 0) return "-";
  return value.toLocaleString("en-NG", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
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

function drawHeader(doc, { groupName, contributionTypeLabel, year, generatedAt }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const barHeight = LAYOUT.headerBarHeight;

  doc.save();
  doc.rect(left, top, width, barHeight).fill("#0F766E");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(16)
    .text("Contribution Ledger", left + 16, top + 14);
  doc
    .font("Helvetica")
    .fontSize(10)
    .text(groupName, left, top + 16, { width: width - 16, align: "right" });
  doc.restore();

  doc.moveDown(LAYOUT.headerGap);
  doc
    .font("Helvetica-Bold")
    .fontSize(10.5)
    .fillColor("#111827")
    .text(groupName);
  doc
    .font("Helvetica")
    .fontSize(7.5)
    .fillColor("#6B7280")
    .text(`${contributionTypeLabel} | ${year}`);
  doc
    .font("Helvetica")
    .fontSize(7.5)
    .fillColor("#9CA3AF")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(LAYOUT.headerMetaGap);
}

function drawSummary(doc, summary) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardGap = LAYOUT.summaryCardGap;
  const cardWidth = (width - cardGap * 3) / 4;
  const cardHeight = LAYOUT.summaryCardHeight;
  const startY = doc.y;

  const cards = [
    { label: "Members", value: summary.members },
    { label: "Expected YTD", value: formatCurrency(summary.expectedTotal) },
    { label: "Collected YTD", value: formatCurrency(summary.collectedTotal) },
    { label: "Collection Rate", value: `${summary.collectionRate}%` },
  ];

  cards.forEach((card, index) => {
    const x = left + index * (cardWidth + cardGap);
    const y = startY;

    doc.save();
    doc.roundedRect(x, y, cardWidth, cardHeight, 8).fill("#F8FAFC");
    doc
      .font("Helvetica")
      .fontSize(6.2)
      .fillColor("#6B7280")
      .text(card.label, x + 10, y + 5, { width: cardWidth - 20 });
    doc
      .font("Helvetica-Bold")
      .fontSize(8.6)
      .fillColor("#111827")
      .text(String(card.value), x + 10, y + 16, { width: cardWidth - 20 });
    doc.restore();
  });

  doc.y = startY + cardHeight + LAYOUT.summaryBottomGap;
}

function drawTableHeader(doc, columns) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const headerHeight = LAYOUT.tableHeaderHeight;
  const startY = doc.y;

  doc.save();
  doc.rect(left, startY, width, headerHeight).fill("#F1F5F9");

  let x = left;
  columns.forEach((col) => {
    const cellY = startY + 2.5;
    const prevX = doc.x;
    const prevY = doc.y;
    doc
      .font("Helvetica-Bold")
      .fontSize(6.2)
      .fillColor("#475569")
      .text(col.label, x + 4, cellY, {
        width: col.width - 8,
        align: col.align || "left",
        lineBreak: false,
        ellipsis: true,
        height: headerHeight - 4,
      });
    doc.x = prevX;
    doc.y = prevY;
    x += col.width;
  });
  doc.restore();
  doc.y = startY + headerHeight;
}

function drawTableRow(
  doc,
  columns,
  row,
  { isTotal = false, rowIndex = 0 } = {},
) {
  const rowHeight = LAYOUT.tableRowHeight;
  const left = doc.page.margins.left;
  const right = doc.page.width - doc.page.margins.right;
  const startY = doc.y;

  if (isTotal) {
    doc.save();
    doc.rect(left, startY, right - left, rowHeight).fill("#E2E8F0");
    doc.restore();
  } else if (rowIndex % 2 === 1) {
    doc.save();
    doc.rect(left, startY, right - left, rowHeight).fill("#F8FAFC");
    doc.restore();
  }

  let x = left;

  columns.forEach((col) => {
    const value = row[col.key] ?? "-";
    const prevX = doc.x;
    const prevY = doc.y;
    doc
      .font(isTotal ? "Helvetica-Bold" : "Helvetica")
      .fontSize(isTotal ? 7 : 6.5)
      .fillColor(isTotal ? "#111827" : "#1F2937")
      .text(String(value), x + 4, startY + LAYOUT.tableRowPadding, {
        width: col.width - 8,
        align: col.align || "left",
        lineBreak: false,
        ellipsis: true,
        height: rowHeight - LAYOUT.tableRowPadding,
      });
    doc.x = prevX;
    doc.y = prevY;
    x += col.width;
  });

  doc
    .strokeColor("#E2E8F0")
    .lineWidth(0.35)
    .moveTo(left, startY + rowHeight)
    .lineTo(right, startY + rowHeight)
    .stroke();

  doc.y = startY + rowHeight;
}

export async function generateGroupContributionLedgerPdfBuffer({
  groupName,
  contributionTypeLabel,
  year,
  generatedAt,
  expectedMonthly,
  expectedUnitAmount,
  monthsToDate,
  rows,
  totals,
  summary,
}) {
  const doc = new PDFDocument({
    size: "A3",
    layout: "landscape",
    margin: 24,
    info: { Title: "Contribution Ledger" },
  });
  doc.lineGap(0);
  const chunks = [];

  const months = [
    { key: "jan", label: "Jan" },
    { key: "feb", label: "Feb" },
    { key: "mar", label: "Mar" },
    { key: "apr", label: "Apr" },
    { key: "may", label: "May" },
    { key: "jun", label: "Jun" },
    { key: "jul", label: "Jul" },
    { key: "aug", label: "Aug" },
    { key: "sep", label: "Sep" },
    { key: "oct", label: "Oct" },
    { key: "nov", label: "Nov" },
    { key: "dec", label: "Dec" },
  ];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { groupName, contributionTypeLabel, year, generatedAt });
    drawSummary(doc, summary);

    doc
      .font("Helvetica")
      .fontSize(6.6)
      .fillColor("#6B7280")
      .text(
        `Avg Expected Monthly: ${formatCurrency(expectedMonthly)}${
          expectedUnitAmount
            ? ` | Unit: ${formatCurrency(expectedUnitAmount)}`
            : ""
        } | YTD: ${monthsToDate} months`,
      );
    doc.moveDown(0.2);

    const { left, right } = doc.page.margins;
    const width = doc.page.width - left - right;
    const snWidth = 26;
    const serialWidth = 80;
    const unitsWidth = 38;
    const trailingColumns = [
      { key: "ytdTotal", label: "YTD Total", width: 80, align: "right" },
      { key: "expectedYtd", label: "Expected YTD", width: 90, align: "right" },
      { key: "arrears", label: "Arrears", width: 80, align: "right" },
      { key: "status", label: "Status", width: 70, align: "left" },
    ];
    const trailingWidth = trailingColumns.reduce(
      (sum, col) => sum + col.width,
      0,
    );

    const fixedWidth = snWidth + serialWidth + unitsWidth + trailingWidth;
    let monthWidth = 32;
    let memberWidth = width - (fixedWidth + monthWidth * months.length);
    if (memberWidth < 160) {
      const availableForMonths = width - (fixedWidth + 160);
      monthWidth = Math.max(24, Math.floor(availableForMonths / months.length));
      memberWidth = width - (fixedWidth + monthWidth * months.length);
    } else if (memberWidth > 230) {
      const extra = memberWidth - 230;
      memberWidth = 230;
      monthWidth = Math.floor(
        (monthWidth * months.length + extra) / months.length,
      );
    }

    const baseColumns = [
      { key: "sn", label: "S/N", width: snWidth, align: "right" },
      { key: "serial", label: "Serial", width: serialWidth },
      { key: "member", label: "Member Name", width: memberWidth },
      { key: "units", label: "Units", width: unitsWidth, align: "right" },
    ];
    const monthColumns = months.map((month) => ({
      key: month.key,
      label: month.label,
      width: monthWidth,
      align: "right",
    }));

    const columns = [
      ...baseColumns,
      ...monthColumns,
      ...trailingColumns,
    ];

    drawTableHeader(doc, columns);

    const bottomY = doc.page.height - doc.page.margins.bottom - LAYOUT.footerGap;

    rows.forEach((row, index) => {
      if (doc.y + LAYOUT.tableRowHeight > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }

      const rowData = {
        sn: index + 1,
        serial: row.memberSerial ?? "-",
        member: row.memberName,
        units: row.units ?? "-",
        ...months.reduce((acc, month, idx) => {
          acc[month.key] = formatTableCurrency(row.months[idx]);
          return acc;
        }, {}),
        ytdTotal: formatCurrency(row.ytdTotal),
        expectedYtd: formatCurrency(row.expectedYtd),
        arrears: formatCurrency(row.arrears),
        status: row.status,
      };

      drawTableRow(doc, columns, rowData, { rowIndex: index });
    });

    if (doc.y + LAYOUT.tableRowHeight > bottomY) {
      doc.addPage();
      drawTableHeader(doc, columns);
    }

    const totalsRow = {
      sn: "Totals",
      serial: "-",
      member: `${summary.members} members`,
      units: totals.unitsTotal > 0 ? totals.unitsTotal : "-",
      ...months.reduce((acc, month, idx) => {
        acc[month.key] = formatTableCurrency(totals.monthTotals[idx]);
        return acc;
      }, {}),
      ytdTotal: formatCurrency(totals.ytdTotal),
      expectedYtd: formatCurrency(totals.expectedYtd),
      arrears: formatCurrency(totals.arrears),
      status: totals.status,
    };

    drawTableRow(doc, columns, totalsRow, { isTotal: true });

    doc
      .moveDown(0.6)
      .font("Helvetica")
      .fontSize(7)
      .fillColor("#94A3B8")
      .text(
        "CRC Cooperative Resource Center - Contribution Ledger",
        {
        align: "center",
      });

    doc.end();
  });
}

