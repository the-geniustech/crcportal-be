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
  summaryHeight: 30,
  summaryGap: 6,
  tableHeaderHeight: 12,
  tableRowHeight: 11,
  rowPadding: 2.5,
};

function drawHeader(doc, { groupName, loanTypeLabel, statusLabel, generatedAt }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;

  doc.save();
  doc.rect(left, top, width, LAYOUT.headerHeight).fill("#0F766E");
  doc
    .fillColor("#FFFFFF")
    .font("Helvetica-Bold")
    .fontSize(16)
    .text("Loan Ledger", left + 16, top + 13);
  doc
    .font("Helvetica")
    .fontSize(10)
    .text(groupName, left, top + 15, { width: width - 16, align: "right" });
  doc.restore();

  doc.moveDown(1.2);
  doc.font("Helvetica-Bold").fontSize(10.5).fillColor("#111827").text(groupName);
  doc
    .font("Helvetica")
    .fontSize(7.5)
    .fillColor("#6B7280")
    .text(`${loanTypeLabel} | ${statusLabel}`);
  doc
    .font("Helvetica")
    .fontSize(7.5)
    .fillColor("#9CA3AF")
    .text(`Generated: ${formatDate(generatedAt)}`);
  doc.moveDown(0.2);
}

function drawSummary(doc, summary) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const cardWidth = (width - LAYOUT.summaryGap * 3) / 4;
  const cardY = doc.y;
  const cards = [
    { label: "Loans", value: summary.total },
    { label: "Principal Volume", value: formatCurrency(summary.principal) },
    { label: "Outstanding", value: formatCurrency(summary.outstanding) },
    { label: "Repaid So Far", value: formatCurrency(summary.repaid) },
  ];

  cards.forEach((card, index) => {
    const x = left + index * (cardWidth + LAYOUT.summaryGap);
    doc.save();
    doc.roundedRect(x, cardY, cardWidth, LAYOUT.summaryHeight, 8).fill("#F8FAFC");
    doc
      .font("Helvetica")
      .fontSize(6.2)
      .fillColor("#6B7280")
      .text(card.label, x + 10, cardY + 5, { width: cardWidth - 20 });
    doc
      .font("Helvetica-Bold")
      .fontSize(8.6)
      .fillColor("#111827")
      .text(String(card.value), x + 10, cardY + 16, { width: cardWidth - 20 });
    doc.restore();
  });

  doc.y = cardY + LAYOUT.summaryHeight + 8;
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
      .fontSize(6.2)
      .fillColor("#475569")
      .text(column.label, x + 3, startY + 2.5, {
        width: column.width - 6,
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
      .fontSize(isTotal ? 6.8 : 6.4)
      .fillColor(isTotal ? "#111827" : "#1F2937")
      .text(String(row[column.key] ?? "-"), x + 3, startY + LAYOUT.rowPadding, {
        width: column.width - 6,
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

export async function generateGroupLoanLedgerPdfBuffer({
  groupName,
  loanTypeLabel,
  statusLabel,
  generatedAt,
  rows,
  summary,
}) {
  const doc = new PDFDocument({
    size: "A3",
    layout: "landscape",
    margin: 24,
    info: { Title: "Loan Ledger" },
  });
  const chunks = [];

  const columns = [
    { key: "loanCode", label: "Loan", width: 70 },
    { key: "loanType", label: "Type", width: 72 },
    { key: "borrowerName", label: "Borrower", width: 96 },
    { key: "memberSerial", label: "Serial", width: 58 },
    { key: "principal", label: "Principal", width: 62, align: "right" },
    { key: "interestRate", label: "Rate", width: 58 },
    { key: "remainingPrincipal", label: "Principal Due", width: 68, align: "right" },
    { key: "remainingInterest", label: "Interest Due", width: 66, align: "right" },
    { key: "repaidPrincipal", label: "Principal Paid", width: 68, align: "right" },
    { key: "repaidInterest", label: "Interest Paid", width: 66, align: "right" },
    { key: "patronage", label: "Patronage", width: 62, align: "right" },
    { key: "status", label: "Status", width: 56 },
    { key: "progress", label: "Progress", width: 46, align: "right" },
    { key: "updatedAt", label: "Updated", width: 56 },
  ];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { groupName, loanTypeLabel, statusLabel, generatedAt });
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

    const totalsRow = {
      loanCode: "Totals",
      loanType: `${summary.total} loans`,
      borrowerName: "-",
      memberSerial: "-",
      principal: formatCurrency(summary.principal),
      interestRate: "-",
      remainingPrincipal: formatCurrency(summary.remainingPrincipal),
      remainingInterest: formatCurrency(summary.remainingInterest),
      repaidPrincipal: formatCurrency(summary.repaidPrincipal),
      repaidInterest: formatCurrency(summary.repaidInterest),
      patronage: formatCurrency(summary.patronage),
      status: summary.statusLabel || "-",
      progress: "-",
      updatedAt: "-",
    };

    if (doc.y + LAYOUT.tableRowHeight > bottomY) {
      doc.addPage();
      drawTableHeader(doc, columns);
    }
    drawTableRow(doc, columns, totalsRow, { isTotal: true });

    doc
      .moveDown(0.8)
      .font("Helvetica")
      .fontSize(7)
      .fillColor("#6B7280")
      .text(
        `Outstanding includes principal and accrued interest. Interest patronage reflects 3% of interest repaid so far.`,
      );

    doc.end();
  });
}
