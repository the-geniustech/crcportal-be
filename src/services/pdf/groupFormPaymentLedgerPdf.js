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

const LAYOUT = {
  headerHeight: 38,
  summaryHeight: 32,
  summaryGap: 8,
  tableHeaderHeight: 16,
  tableRowHeight: 18,
  rowPadding: 4,
};

function drawHeader(doc, { groupName, scopeLabel, generatedAt }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;

  doc.save();
  doc.rect(left, top, width, LAYOUT.headerHeight).fill("#0F766E");
  doc
    .fillColor("#FFFFFF")
    .font("Helvetica-Bold")
    .fontSize(16)
    .text("Form Payment Ledger", left + 16, top + 12);
  doc
    .font("Helvetica")
    .fontSize(9)
    .text(groupName, left, top + 15, { width: width - 16, align: "right" });
  doc.restore();

  doc.moveDown(1.25);
  doc.font("Helvetica-Bold").fontSize(10.5).fillColor("#111827").text(groupName);
  doc.font("Helvetica").fontSize(8).fillColor("#6B7280").text(scopeLabel);
  doc
    .font("Helvetica")
    .fontSize(7.5)
    .fillColor("#9CA3AF")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(0.35);
}

function drawSummary(doc, summary) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardWidth = (width - LAYOUT.summaryGap * 3) / 4;
  const cardY = doc.y;
  const cards = [
    { label: "Records", value: summary.totalRecords || 0 },
    { label: "Total Expected", value: formatCurrency(summary.totalAmount) },
    { label: "Paid", value: formatCurrency(summary.paidAmount) },
    { label: "Pending / Defaulted", value: `${formatCurrency(summary.pendingAmount)} / ${formatCurrency(summary.defaultedAmount)}` },
  ];

  cards.forEach((card, index) => {
    const x = left + index * (cardWidth + LAYOUT.summaryGap);
    doc.save();
    doc.roundedRect(x, cardY, cardWidth, LAYOUT.summaryHeight, 8).fill("#F8FAFC");
    doc
      .font("Helvetica")
      .fontSize(6.8)
      .fillColor("#6B7280")
      .text(card.label, x + 10, cardY + 6, { width: cardWidth - 20 });
    doc
      .font("Helvetica-Bold")
      .fontSize(8.4)
      .fillColor("#111827")
      .text(String(card.value), x + 10, cardY + 18, {
        width: cardWidth - 20,
        lineBreak: false,
        ellipsis: true,
      });
    doc.restore();
  });

  doc.y = cardY + LAYOUT.summaryHeight + 10;
}

function drawTableHeader(doc, columns) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const startY = doc.y;

  doc.save();
  doc.rect(left, startY, width, LAYOUT.tableHeaderHeight).fill("#F1F5F9");
  let x = left;
  columns.forEach((column) => {
    doc
      .font("Helvetica-Bold")
      .fontSize(7)
      .fillColor("#475569")
      .text(column.label, x + 4, startY + 4, {
        width: column.width - 8,
        align: column.align || "left",
        lineBreak: false,
        ellipsis: true,
      });
    x += column.width;
  });
  doc.restore();
  doc.y = startY + LAYOUT.tableHeaderHeight;
}

function drawTableRow(doc, columns, row, { isTotal = false, rowIndex = 0 } = {}) {
  const left = doc.page.margins.left;
  const right = doc.page.width - doc.page.margins.right;
  const startY = doc.y;

  if (isTotal) {
    doc.save();
    doc.rect(left, startY, right - left, LAYOUT.tableRowHeight).fill("#E2E8F0");
    doc.restore();
  } else if (rowIndex % 2 === 1) {
    doc.save();
    doc.rect(left, startY, right - left, LAYOUT.tableRowHeight).fill("#F8FAFC");
    doc.restore();
  }

  let x = left;
  columns.forEach((column) => {
    doc
      .font(isTotal ? "Helvetica-Bold" : "Helvetica")
      .fontSize(isTotal ? 7 : 6.6)
      .fillColor(isTotal ? "#111827" : "#1F2937")
      .text(String(row[column.key] ?? "-"), x + 4, startY + LAYOUT.rowPadding, {
        width: column.width - 8,
        align: column.align || "left",
        lineBreak: false,
        ellipsis: true,
      });
    x += column.width;
  });

  doc
    .strokeColor("#E2E8F0")
    .lineWidth(0.35)
    .moveTo(left, startY + LAYOUT.tableRowHeight)
    .lineTo(right, startY + LAYOUT.tableRowHeight)
    .stroke();

  doc.y = startY + LAYOUT.tableRowHeight;
}

export async function generateGroupFormPaymentLedgerPdfBuffer({
  groupName,
  scopeLabel,
  generatedAt,
  rows,
  summary,
  includeGroupName = false,
}) {
  const doc = new PDFDocument({
    size: "A3",
    layout: "landscape",
    margin: 26,
    info: { Title: "Form Payment Ledger" },
  });
  const chunks = [];

  const columns = includeGroupName
    ? [
        { key: "memberName", label: "Member", width: 110 },
        { key: "groupName", label: "Group", width: 110 },
        { key: "memberPhone", label: "Phone", width: 72 },
        { key: "formLabel", label: "Form Type", width: 136 },
        { key: "amount", label: "Amount", width: 70, align: "right" },
        { key: "paymentStatus", label: "Status", width: 62 },
        { key: "submittedAt", label: "Submitted", width: 70 },
        { key: "reviewedAt", label: "Reviewed", width: 70 },
        { key: "sourceReference", label: "Source Ref", width: 112 },
        { key: "transactionReference", label: "Transaction Ref", width: 210 },
      ]
    : [
        { key: "memberName", label: "Member", width: 118 },
        { key: "memberPhone", label: "Phone", width: 76 },
        { key: "formLabel", label: "Form Type", width: 132 },
        { key: "amount", label: "Amount", width: 72, align: "right" },
        { key: "paymentStatus", label: "Status", width: 66 },
        { key: "submittedAt", label: "Submitted", width: 72 },
        { key: "reviewedAt", label: "Reviewed", width: 72 },
        { key: "sourceReference", label: "Source Ref", width: 110 },
        { key: "transactionReference", label: "Transaction Ref", width: 194 },
      ];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { groupName, scopeLabel, generatedAt });
    drawSummary(doc, summary);
    drawTableHeader(doc, columns);

    const bottomY = doc.page.height - doc.page.margins.bottom - 10;

    rows.forEach((row, index) => {
      if (doc.y + LAYOUT.tableRowHeight > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }

      drawTableRow(doc, columns, row, { rowIndex: index });
    });

    if (doc.y + LAYOUT.tableRowHeight > bottomY) {
      doc.addPage();
      drawTableHeader(doc, columns);
    }

    drawTableRow(
      doc,
      columns,
      {
        memberName: "Totals",
        groupName: includeGroupName ? "All groups" : undefined,
        memberPhone: `${rows.length} records`,
        formLabel: "-",
        amount: formatCurrency(summary.totalAmount),
        paymentStatus: `${summary.paidCount || 0} paid`,
        submittedAt: `${summary.pendingCount || 0} pending`,
        reviewedAt: `${summary.defaultedCount || 0} defaulted`,
        sourceReference: "-",
        transactionReference: "-",
      },
      { isTotal: true },
    );

    doc.end();
  });
}
