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
  doc.rect(left, top, width, barHeight).fill("#B45309");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(18)
    .text("Sharing Formula Of Interest", left + 16, top + 16);
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

function drawTotalCard(doc, totalInterest) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardHeight = 54;
  const startY = doc.y;

  doc.save();
  doc.roundedRect(left, startY, width, cardHeight, 8).fill("#FFFBEB");
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#92400E")
    .text("Total Interest", left + 12, startY + 12, { width: width - 24 });
  doc
    .font("Helvetica-Bold")
    .fontSize(13)
    .fillColor("#111827")
    .text(formatCurrency(totalInterest), left + 12, startY + 28, {
      width: width - 24,
    });
  doc.restore();

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

export async function generateContributionInterestSharingPdfBuffer({
  year,
  contributionTypeLabel,
  generatedAt,
  totalInterest,
  categories,
}) {
  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: "Sharing Formula Of Interest" },
  });
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { year, contributionTypeLabel, generatedAt });
    drawTotalCard(doc, totalInterest);

    doc
      .font("Helvetica-Bold")
      .fontSize(11)
      .fillColor("#111827")
      .text("Category Allocation");
    doc.moveDown(0.4);

    const { left, right } = doc.page.margins;
    const width = doc.page.width - left - right;
    const columns = [
      { key: "category", label: "Category", width: width * 0.4 },
      { key: "percentage", label: "Percentage", width: width * 0.18 },
      { key: "amount", label: "Amount", width: width * 0.21 },
      { key: "shared", label: "Amount Shared", width: width * 0.21 },
    ];

    drawTableHeader(doc, columns);

    const bottomY = doc.page.height - doc.page.margins.bottom;

    categories.forEach((category) => {
      if (doc.y + 28 > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }
      drawTableRow(doc, columns, {
        category: category.label,
        percentage: `${category.percentage}%`,
        amount: formatCurrency(category.amount),
        shared: formatCurrency(category.amountShared),
      });
    });

    drawTableRow(
      doc,
      columns,
      {
        category: "Totals",
        percentage: "100%",
        amount: formatCurrency(totalInterest),
        shared: formatCurrency(totalInterest),
      },
      { bold: true },
    );

    doc
      .moveDown(1.4)
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#9CA3AF")
      .text("CRC Champions Revolving Contributions - Interest Sharing", {
        align: "center",
      });

    doc.end();
  });
}
