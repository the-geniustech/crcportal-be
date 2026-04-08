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

function drawHeader(doc, { groupName, periodLabel, generatedAt }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const barHeight = 54;

  doc.save();
  doc.rect(left, top, width, barHeight).fill("#10B981");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(18)
    .text("Contribution Report", left + 16, top + 16);
  doc
    .font("Helvetica")
    .fontSize(10)
    .text(groupName, left, top + 20, { width: width - 16, align: "right" });
  doc.restore();

  doc.moveDown(2.4);
  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor("#111827")
    .text(groupName);
  doc
    .font("Helvetica")
    .fontSize(10)
    .fillColor("#6B7280")
    .text(`Period: ${periodLabel}`);
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#9CA3AF")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(1.2);
}

function drawSummaryCards(doc, summary) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardGap = 10;
  const cardWidth = (width - cardGap) / 2;
  const cardHeight = 54;
  const startY = doc.y;

  const cards = [
    { label: "Total Expected", value: formatCurrency(summary.totalExpected) },
    { label: "Total Collected", value: formatCurrency(summary.totalCollected) },
    { label: "Collection Rate", value: `${summary.collectionRate}%` },
    {
      label: "Paid / Pending / Overdue",
      value: `${summary.paidCount} / ${summary.pendingCount} / ${summary.overdueCount}`,
    },
  ];

  cards.forEach((card, index) => {
    const isRight = index % 2 === 1;
    const x = isRight ? left + cardWidth + cardGap : left;
    const y = startY + Math.floor(index / 2) * (cardHeight + cardGap);

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

  doc.y = startY + cardHeight * 2 + cardGap + 6;
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

function drawTableRow(doc, columns, row) {
  const rowHeight = 24;
  let x = doc.page.margins.left;
  const startY = doc.y;

  columns.forEach((col) => {
    const prevX = doc.x;
    const prevY = doc.y;
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#111827")
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

export async function generateGroupContributionReportPdfBuffer({
  groupName,
  periodLabel,
  generatedAt,
  summary,
  rows,
}) {
  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: "Contribution Report" },
  });
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { groupName, periodLabel, generatedAt });
    drawSummaryCards(doc, summary);

    doc.moveDown(0.6);
    doc.font("Helvetica-Bold").fontSize(11).fillColor("#111827").text("Member Details");
    doc.moveDown(0.4);

    const { left, right } = doc.page.margins;
    const width = doc.page.width - left - right;
    const columns = [
      { key: "memberSerial", label: "Serial", width: width * 0.18 },
      { key: "member", label: "Member", width: width * 0.32 },
      { key: "status", label: "Status", width: width * 0.16 },
      { key: "amount", label: "Amount", width: width * 0.17 },
      { key: "paidDate", label: "Paid Date", width: width * 0.17 },
    ];

    drawTableHeader(doc, columns);

    const bottomY = doc.page.height - doc.page.margins.bottom;

    rows.forEach((row) => {
      if (doc.y + 28 > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }
      drawTableRow(doc, columns, row);
    });

    doc
      .moveDown(1.4)
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#9CA3AF")
      .text(
        "CRC Cooperative Resource Center - Contribution Report",
        {
        align: "center",
      });

    doc.end();
  });
}

