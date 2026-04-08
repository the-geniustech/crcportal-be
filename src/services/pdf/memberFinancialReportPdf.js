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

function drawHeader(doc, { title, memberName, periodLabel, generatedAt }) {
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
  doc
    .font("Helvetica")
    .fontSize(9.5)
    .fillColor("#6B7280")
    .text(`Period: ${periodLabel}`);
  doc
    .font("Helvetica")
    .fontSize(8.5)
    .fillColor("#9CA3AF")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(0.8);
}

function drawSummaryCards(doc, summaryItems) {
  if (!summaryItems || summaryItems.length === 0) return;
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardGap = 10;
  const cardWidth = (width - cardGap) / 2;
  const cardHeight = 52;
  const startY = doc.y;

  summaryItems.forEach((card, index) => {
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

  const rows = Math.ceil(summaryItems.length / 2);
  doc.y = startY + rows * cardHeight + (rows - 1) * cardGap + 6;
}

function drawSectionTitle(doc, label) {
  doc
    .font("Helvetica-Bold")
    .fontSize(11)
    .fillColor("#111827")
    .text(label);
  doc.moveDown(0.4);
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

export async function generateMemberFinancialReportPdfBuffer({
  title,
  memberName,
  periodLabel,
  generatedAt,
  summaryItems,
  sections,
  footerNote,
}) {
  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: title || "Financial Report" },
  });
  const contentWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, {
      title: title || "Financial Report",
      memberName,
      periodLabel,
      generatedAt,
    });

    drawSummaryCards(doc, summaryItems);

    const normalizeColumns = (cols) =>
      cols.map((col) => {
        if (typeof col.width === "number" && col.width > 0 && col.width <= 1) {
          return { ...col, width: contentWidth * col.width };
        }
        if (!col.width) {
          return { ...col, width: contentWidth / cols.length };
        }
        return col;
      });

    const safeSections = Array.isArray(sections) ? sections : [];
    safeSections.forEach((section) => {
      if (!section) return;
      drawSectionTitle(doc, section.title);

      if (section.type === "table") {
        const columns = normalizeColumns(section.columns);
        drawTableHeader(doc, columns);
        const bottomY = doc.page.height - doc.page.margins.bottom;

        section.rows.forEach((row) => {
          if (doc.y + 28 > bottomY) {
            doc.addPage();
            drawTableHeader(doc, columns);
          }
          drawTableRow(doc, columns, row);
        });
      }

      if (section.type === "list") {
        section.items.forEach((item) => {
          doc
            .font("Helvetica")
            .fontSize(9.5)
            .fillColor("#111827")
            .text(`- ${item}`);
        });
      }

      doc.moveDown(0.8);
    });

    if (footerNote) {
      doc
        .font("Helvetica")
        .fontSize(9)
        .fillColor("#6B7280")
        .text(footerNote, { align: "center" });
    }

    doc.end();
  });
}

export { formatCurrency, formatDate };

