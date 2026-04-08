import PDFDocument from "pdfkit";

function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0.00";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
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

function drawHeader(doc, { year, contributionTypeLabel, generatedAt }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const barHeight = 54;

  doc.save();
  doc.rect(left, top, width, barHeight).fill("#0F766E");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(18)
    .text("Summary Of Income", left + 16, top + 16);
  doc
    .font("Helvetica")
    .fontSize(10)
    .text(`Year ${year}`, left, top + 16, {
      width: width - 16,
      align: "right",
    });
  doc
    .font("Helvetica")
    .fontSize(9)
    .text(contributionTypeLabel, left, top + 32, {
      width: width - 16,
      align: "right",
    });
  doc.restore();

  doc.moveDown(2.4);
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#6B7280")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(0.8);
}

function drawSummaryCards(doc, totals) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardGap = 10;
  const cardWidth = (width - cardGap * 2) / 3;
  const cardHeight = 54;
  const startY = doc.y;

  const cards = [
    { label: "Total Contributions", value: formatCurrency(totals.contributions) },
    { label: "Total Interest", value: formatCurrency(totals.interest) },
    { label: "Cumulative Total", value: formatCurrency(totals.total) },
  ];

  cards.forEach((card, index) => {
    const x = left + index * (cardWidth + cardGap);
    const y = startY;

    doc.save();
    doc.roundedRect(x, y, cardWidth, cardHeight, 8).fill("#F9FAFB");
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#6B7280")
      .text(card.label, x + 12, y + 12, { width: cardWidth - 24 });
    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor("#111827")
      .text(card.value, x + 12, y + 28, { width: cardWidth - 24 });
    doc.restore();
  });

  doc.y = startY + cardHeight + 12;
}

function drawTableHeader(doc, columns) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const headerHeight = 24;
  const startY = doc.y;

  doc.save();
  doc.rect(left, startY, width, headerHeight).fill("#F3F4F6");

  let x = left;
  columns.forEach((col) => {
    const prevX = doc.x;
    const prevY = doc.y;
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .fillColor("#374151")
      .text(col.label, x + 8, startY + 7, {
        width: col.width - 12,
        lineBreak: false,
        ellipsis: true,
        height: headerHeight - 10,
      });
    doc.x = prevX;
    doc.y = prevY;
    x += col.width;
  });
  doc.restore();
  doc.y = startY + headerHeight;
}

function drawTableRow(doc, columns, row, { bold = false } = {}) {
  const rowHeight = 24;
  let x = doc.page.margins.left;
  const startY = doc.y;
  const font = bold ? "Helvetica-Bold" : "Helvetica";
  const color = bold ? "#111827" : "#374151";

  columns.forEach((col) => {
    const prevX = doc.x;
    const prevY = doc.y;
    doc
      .font(font)
      .fontSize(9)
      .fillColor(color)
      .text(String(row[col.key] ?? "-"), x + 8, startY + 7, {
        width: col.width - 12,
        lineBreak: false,
        ellipsis: true,
        height: rowHeight - 10,
      });
    doc.x = prevX;
    doc.y = prevY;
    x += col.width;
  });

  doc
    .strokeColor("#E5E7EB")
    .lineWidth(0.5)
    .moveTo(doc.page.margins.left, startY + rowHeight)
    .lineTo(doc.page.width - doc.page.margins.right, startY + rowHeight)
    .stroke();

  doc.y = startY + rowHeight;
}

export async function generateContributionIncomeSummaryPdfBuffer({
  year,
  contributionTypeLabel,
  generatedAt,
  totals,
  schedule,
  monthsComputed,
}) {
  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: "Summary Of Income" },
  });
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { year, contributionTypeLabel, generatedAt });
    drawSummaryCards(doc, totals);

    doc
      .font("Helvetica-Bold")
      .fontSize(11)
      .fillColor("#111827")
      .text("Monthly Breakdown");
    doc.moveDown(0.4);

    const { left, right } = doc.page.margins;
    const width = doc.page.width - left - right;
    const columns = [
      { key: "month", label: "Month", width: width * 0.22 },
      { key: "contributions", label: "Contributions", width: width * 0.2 },
      { key: "interest", label: "Interest", width: width * 0.18 },
      { key: "total", label: "Total", width: width * 0.2 },
      { key: "cumulative", label: "Cumulative", width: width * 0.2 },
    ];

    drawTableHeader(doc, columns);

    const bottomY = doc.page.height - doc.page.margins.bottom;

    schedule.forEach((row) => {
      if (doc.y + 28 > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }
      drawTableRow(doc, columns, {
        month: row.label,
        contributions: formatCurrency(row.contributions),
        interest: row.interest > 0 ? formatCurrency(row.interest) : "-",
        total: formatCurrency(row.total),
        cumulative: formatCurrency(row.cumulativeTotal),
      });
    });

    const lastCumulative =
      schedule[Math.max(0, (monthsComputed || 0) - 1)]?.cumulativeTotal ??
      schedule[schedule.length - 1]?.cumulativeTotal ??
      0;

    drawTableRow(
      doc,
      columns,
      {
        month: "Totals",
        contributions: formatCurrency(totals.contributions),
        interest: formatCurrency(totals.interest),
        total: formatCurrency(totals.total),
        cumulative: formatCurrency(lastCumulative),
      },
      { bold: true },
    );

    doc
      .moveDown(1.4)
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#9CA3AF")
      .text(
        "CRC Cooperative Resource Center - Summary Of Income",
        {
        align: "center",
      });

    doc.end();
  });
}

