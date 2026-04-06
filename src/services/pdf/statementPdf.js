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

function drawHeader(doc, { title, memberName, memberEmail, periodLabel }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const barHeight = 56;

  doc.save();
  doc.rect(left, top, width, barHeight).fill("#10B981");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(18)
    .text("CRC", left + 16, top + 18);
  doc
    .font("Helvetica")
    .fontSize(11)
    .text(title, left, top + 20, { width: width - 16, align: "right" });
  doc.restore();

  doc.moveDown(2.2);
  doc
    .font("Helvetica-Bold")
    .fontSize(14)
    .fillColor("#111827")
    .text(memberName || "Member");
  if (memberEmail) {
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(memberEmail);
  }
  doc
    .font("Helvetica")
    .fontSize(9.5)
    .fillColor("#6B7280")
    .text(`Period: ${periodLabel}`);
  doc.moveDown(0.6);
}

function drawSummaryCards(doc, summary) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardGap = 10;
  const cardWidth = (width - cardGap) / 2;
  const cardHeight = 52;
  const startY = doc.y;

  const cards = [
    { label: "Total Credits", value: formatCurrency(summary.totalCredits) },
    { label: "Total Debits", value: formatCurrency(summary.totalDebits) },
    { label: "Net Position", value: formatCurrency(summary.netPosition) },
    { label: "Transactions", value: `${summary.transactionCount}` },
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
      .text(card.label, x + 12, y + 10, { width: cardWidth - 24 });
    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor("#111827")
      .text(card.value, x + 12, y + 26, { width: cardWidth - 24 });
    doc.restore();
  });

  doc.y = startY + cardHeight * 2 + cardGap + 4;
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
        align: col.align || "left",
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
        align: col.align || "left",
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

export async function generateStatementPdfBuffer({
  memberName,
  memberEmail,
  periodLabel,
  generatedAt,
  summary,
  rows,
}) {
  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: "Account Statement" },
  });
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, {
      title: "Account Statement",
      memberName,
      memberEmail,
      periodLabel,
    });

    drawSummaryCards(doc, summary);

    doc.moveDown(0.4);
    doc
      .font("Helvetica-Bold")
      .fontSize(11)
      .fillColor("#111827")
      .text("Transaction Details");
    doc.moveDown(0.4);

    const { left, right } = doc.page.margins;
    const width = doc.page.width - left - right;
    const columns = [
      { key: "date", label: "Date", width: width * 0.14 },
      { key: "type", label: "Type", width: width * 0.18 },
      { key: "description", label: "Description", width: width * 0.26 },
      { key: "reference", label: "Reference", width: width * 0.2 },
      { key: "status", label: "Status", width: width * 0.1 },
      { key: "amount", label: "Amount", width: width * 0.12, align: "right" },
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
      .moveDown(1.2)
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#9CA3AF")
      .text(`Generated: ${formatDate(generatedAt)}`, { align: "center" });

    doc.end();
  });
}

export { formatCurrency, formatDate };
