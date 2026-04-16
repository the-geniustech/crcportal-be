import PDFDocument from "pdfkit";

function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0.00";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function ensureSpace(doc, neededHeight = 80) {
  const bottomY = doc.page.height - doc.page.margins.bottom;
  if (doc.y + neededHeight <= bottomY) return;
  doc.addPage();
}

function drawHeader(doc, title, subtitle) {
  const contentWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const top = doc.page.margins.top;
  doc.save();
  doc.rect(doc.page.margins.left, top, contentWidth, 58).fill("#0F172A");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(19)
    .text(title, doc.page.margins.left + 16, top + 14);
  doc
    .font("Helvetica")
    .fontSize(10)
    .fillColor("#CBD5E1")
    .text(subtitle, doc.page.margins.left + 16, top + 36);
  doc.restore();
  doc.y = top + 76;
}

function drawStatGrid(doc, stats) {
  const left = doc.page.margins.left;
  const contentWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const cardGap = 10;
  const cardWidth = (contentWidth - cardGap) / 2;
  const cardHeight = 58;
  const startY = doc.y;

  stats.forEach((stat, index) => {
    const col = index % 2;
    const row = Math.floor(index / 2);
    const x = left + col * (cardWidth + cardGap);
    const y = startY + row * (cardHeight + cardGap);
    doc.save();
    doc.roundedRect(x, y, cardWidth, cardHeight, 10).fill(stat.bg);
    doc
      .fillColor(stat.labelColor)
      .font("Helvetica")
      .fontSize(9)
      .text(stat.label, x + 12, y + 10, { width: cardWidth - 24 });
    doc
      .fillColor(stat.valueColor)
      .font("Helvetica-Bold")
      .fontSize(14)
      .text(stat.value, x + 12, y + 26, { width: cardWidth - 24 });
    doc.restore();
  });

  doc.y = startY + Math.ceil(stats.length / 2) * (cardHeight + cardGap);
}

function drawSectionTitle(doc, label) {
  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor("#111827")
    .text(label);
  doc.moveDown(0.4);
}

function drawLabelValueTable(doc, rows) {
  const contentWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const left = doc.page.margins.left;
  const labelWidth = contentWidth * 0.34;
  const rowHeight = 22;

  rows.forEach((row, index) => {
    ensureSpace(doc, rowHeight + 12);
    const y = doc.y;
    doc
      .rect(left, y, contentWidth, rowHeight)
      .strokeColor("#E5E7EB")
      .lineWidth(0.5)
      .stroke();
    if (index % 2 === 0) {
      doc.save();
      doc.rect(left, y, contentWidth, rowHeight).fillOpacity(0.05).fill("#111827");
      doc.restore();
    }
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .fillColor("#6B7280")
      .text(row.label, left + 10, y + 6, { width: labelWidth - 14 });
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#111827")
      .text(row.value, left + labelWidth, y + 6, {
        width: contentWidth - labelWidth - 10,
      });
    doc.y = y + rowHeight;
  });
  doc.moveDown(0.8);
}

function drawRepaymentRow(doc, repayment) {
  ensureSpace(doc, 120);
  const left = doc.page.margins.left;
  const contentWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const boxTop = doc.y;

  doc.save();
  doc.roundedRect(left, boxTop, contentWidth, 96, 10).fill("#F8FAFC");
  doc.restore();

  doc
    .fillColor("#111827")
    .font("Helvetica-Bold")
    .fontSize(12)
    .text(formatCurrency(repayment.amount), left + 14, boxTop + 12);
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#475569")
    .text(`Reference: ${repayment.reference || "-"}`, left + 14, boxTop + 30)
    .text(
      `Received: ${formatDateTime(repayment.receivedAt || repayment.recordedAt)}`,
      left + 14,
      boxTop + 44,
    )
    .text(
      `Method: ${repayment.paymentMethod || repayment.gateway || "-"}`,
      left + 14,
      boxTop + 58,
    );

  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor("#334155")
    .text(
      `Posted by: ${repayment.recordedBy?.name || "System"}`,
      left + contentWidth * 0.48,
      boxTop + 12,
      { width: contentWidth * 0.44 },
    )
    .text(
      `Balance after: ${formatCurrency(repayment.remainingBalanceAfterPayment)}`,
      left + contentWidth * 0.48,
      boxTop + 30,
      { width: contentWidth * 0.44 },
    )
    .text(
      `Settled installments: ${Number(repayment.settledInstallmentCount || 0)}`,
      left + contentWidth * 0.48,
      boxTop + 44,
      { width: contentWidth * 0.44 },
    );

  const allocationText = Array.isArray(repayment.allocations)
    ? repayment.allocations
        .map(
          (item) =>
            `#${item.installmentNumber}: ${formatCurrency(item.appliedAmount)}`,
        )
        .join(" | ")
    : "";

  if (allocationText) {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor("#64748B")
      .text(`Allocations: ${allocationText}`, left + 14, boxTop + 76, {
        width: contentWidth - 28,
      });
  }

  doc.y = boxTop + 108;
}

export async function generateLoanRepaymentHistoryPdfBuffer({
  organization,
  loan,
  summary,
  repayments,
} = {}) {
  const doc = new PDFDocument({
    size: "A4",
    margin: 40,
    info: { Title: "Loan Repayment History" },
  });
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(
      doc,
      `${organization?.name || "CRC"} Loan Repayment History`,
      organization?.subtitle || "Loan operations summary",
    );

    drawSectionTitle(doc, "Loan Overview");
    drawLabelValueTable(doc, [
      { label: "Borrower", value: loan?.borrowerName || "Member" },
      { label: "Borrower Email", value: loan?.borrowerEmail || "-" },
      { label: "Borrower Phone", value: loan?.borrowerPhone || "-" },
      { label: "Loan Code", value: loan?.loanCode || "-" },
      { label: "Loan Type", value: loan?.loanType || "-" },
      { label: "Group", value: loan?.groupName || "-" },
      { label: "Status", value: String(loan?.loanStatus || "-").toUpperCase() },
      { label: "Disbursed At", value: formatDateTime(loan?.disbursedAt) },
    ]);

    drawStatGrid(doc, [
      {
        label: "Total Collected",
        value: formatCurrency(summary?.totalCollected),
        bg: "#ECFDF3",
        labelColor: "#047857",
        valueColor: "#064E3B",
      },
      {
        label: "Remaining Balance",
        value: formatCurrency(summary?.remainingBalance),
        bg: "#FEF3C7",
        labelColor: "#B45309",
        valueColor: "#78350F",
      },
      {
        label: "Next Due",
        value: formatCurrency(summary?.nextPaymentAmount),
        bg: "#DBEAFE",
        labelColor: "#1D4ED8",
        valueColor: "#1E3A8A",
      },
      {
        label: "Installments Settled",
        value: `${summary?.settledInstallments || 0}/${summary?.totalInstallments || 0}`,
        bg: "#F1F5F9",
        labelColor: "#475569",
        valueColor: "#0F172A",
      },
    ]);

    drawSectionTitle(doc, "Repayment Ledger");
    if (!Array.isArray(repayments) || repayments.length === 0) {
      doc
        .font("Helvetica")
        .fontSize(10)
        .fillColor("#6B7280")
        .text("No repayments recorded for this loan.");
    } else {
      repayments.forEach((repayment) => drawRepaymentRow(doc, repayment));
    }

    doc.end();
  });
}
