import PDFDocument from "pdfkit";

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

function drawHeader(doc, { groupName, contributionTypeLabel, year, generatedAt }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const barHeight = 46;

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

  doc.moveDown(2.2);
  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor("#111827")
    .text(groupName);
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#6B7280")
    .text(`${contributionTypeLabel} | ${year}`);
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#9CA3AF")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(0.8);
}

function drawSummary(doc, summary) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardGap = 10;
  const cardWidth = (width - cardGap * 3) / 4;
  const cardHeight = 40;
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
      .fontSize(7)
      .fillColor("#6B7280")
      .text(card.label, x + 10, y + 8, { width: cardWidth - 20 });
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .fillColor("#111827")
      .text(String(card.value), x + 10, y + 22, { width: cardWidth - 20 });
    doc.restore();
  });

  doc.y = startY + cardHeight + 12;
}

function drawTableHeader(doc, columns) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const headerHeight = 18;

  doc.save();
  doc.rect(left, doc.y, width, headerHeight).fill("#F1F5F9");

  let x = left;
  columns.forEach((col) => {
    doc
      .font("Helvetica-Bold")
      .fontSize(7)
      .fillColor("#475569")
      .text(col.label, x + 4, doc.y + 5, {
        width: col.width - 8,
        align: col.align || "left",
      });
    x += col.width;
  });
  doc.restore();
  doc.y += headerHeight;
}

function drawTableRow(
  doc,
  columns,
  row,
  { isTotal = false, rowIndex = 0 } = {},
) {
  const rowHeight = 16;
  const left = doc.page.margins.left;
  const right = doc.page.width - doc.page.margins.right;

  if (isTotal) {
    doc.save();
    doc.rect(left, doc.y, right - left, rowHeight).fill("#E2E8F0");
    doc.restore();
  } else if (rowIndex % 2 === 1) {
    doc.save();
    doc.rect(left, doc.y, right - left, rowHeight).fill("#F8FAFC");
    doc.restore();
  }

  let x = left;

  columns.forEach((col) => {
    const value = row[col.key] ?? "-";
    doc
      .font(isTotal ? "Helvetica-Bold" : "Helvetica")
      .fontSize(isTotal ? 7.5 : 7)
      .fillColor(isTotal ? "#111827" : "#1F2937")
      .text(String(value), x + 4, doc.y + 4, {
        width: col.width - 8,
        align: col.align || "left",
        lineBreak: false,
        ellipsis: true,
      });
    x += col.width;
  });

  doc
    .strokeColor("#E2E8F0")
    .lineWidth(0.4)
    .moveTo(left, doc.y + rowHeight)
    .lineTo(right, doc.y + rowHeight)
    .stroke();

  doc.y += rowHeight;
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
    margin: 36,
    info: { Title: "Contribution Ledger" },
  });
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
      .fontSize(7.5)
      .fillColor("#6B7280")
      .text(
        `Expected Monthly: ${formatCurrency(expectedMonthly)}${
          expectedUnitAmount
            ? ` | Unit: ${formatCurrency(expectedUnitAmount)}`
            : ""
        } | YTD (${monthsToDate} months)`,
      );
    doc.moveDown(0.6);

    const { left, right } = doc.page.margins;
    const width = doc.page.width - left - right;
    const snWidth = 28;
    const unitsWidth = 40;
    const expectedWidth = 80;
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

    const fixedWidth = snWidth + unitsWidth + expectedWidth + trailingWidth;
    let monthWidth = 32;
    let memberWidth = width - (fixedWidth + monthWidth * months.length);
    if (memberWidth < 150) {
      const availableForMonths = width - (fixedWidth + 150);
      monthWidth = Math.max(24, Math.floor(availableForMonths / months.length));
      memberWidth = width - (fixedWidth + monthWidth * months.length);
    }

    const baseColumns = [
      { key: "sn", label: "S/N", width: snWidth, align: "right" },
      { key: "member", label: "Member Name", width: memberWidth },
      { key: "units", label: "Units", width: unitsWidth, align: "right" },
      {
        key: "expectedMonthly",
        label: "Expected Monthly",
        width: expectedWidth,
        align: "right",
      },
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

    const bottomY = doc.page.height - doc.page.margins.bottom - 16;

    rows.forEach((row, index) => {
      if (doc.y + 18 > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }

      const rowData = {
        sn: index + 1,
        member: row.memberName,
        units: row.units ?? "-",
        expectedMonthly: formatCurrency(expectedMonthly),
        ...months.reduce((acc, month, idx) => {
          acc[month.key] =
            row.months[idx] > 0 ? formatCurrency(row.months[idx]) : "-";
          return acc;
        }, {}),
        ytdTotal: formatCurrency(row.ytdTotal),
        expectedYtd: formatCurrency(row.expectedYtd),
        arrears: formatCurrency(row.arrears),
        status: row.status,
      };

      drawTableRow(doc, columns, rowData, { rowIndex: index });
    });

    if (doc.y + 18 > bottomY) {
      doc.addPage();
      drawTableHeader(doc, columns);
    }

    const totalsRow = {
      sn: "Totals",
      member: `${summary.members} members`,
      units:
        expectedUnitAmount && expectedMonthly > 0
          ? Math.max(1, Math.round(expectedMonthly / expectedUnitAmount))
          : "-",
      expectedMonthly: formatCurrency(expectedMonthly * summary.members),
      ...months.reduce((acc, month, idx) => {
        acc[month.key] =
          totals.monthTotals[idx] > 0
            ? formatCurrency(totals.monthTotals[idx])
            : "-";
        return acc;
      }, {}),
      ytdTotal: formatCurrency(totals.ytdTotal),
      expectedYtd: formatCurrency(totals.expectedYtd),
      arrears: formatCurrency(totals.arrears),
      status: totals.status,
    };

    drawTableRow(doc, columns, totalsRow, { isTotal: true });

    doc
      .moveDown(1)
      .font("Helvetica")
      .fontSize(8)
      .fillColor("#94A3B8")
      .text("CRC Cooperative Resource Center - Contribution Ledger", {
        align: "center",
      });

    doc.end();
  });
}
