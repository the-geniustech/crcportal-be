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

function formatRate(rate, rateType) {
  if (!Number.isFinite(Number(rate))) return "-";
  const typeLabel = rateType ? String(rateType).toUpperCase() : "";
  return `${Number(rate).toFixed(2)}% ${typeLabel}`.trim();
}

function formatFileSize(value) {
  const size = Number(value || 0);
  if (!Number.isFinite(size) || size <= 0) return "-";
  if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  if (size >= 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${size} B`;
}

function ensureSpace(doc, height = 80) {
  const bottom = doc.page.height - doc.page.margins.bottom;
  if (doc.y + height > bottom) {
    doc.addPage();
  }
}

function drawHeader(doc, { title, status, reference }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const barHeight = 54;

  doc.save();
  doc.rect(left, top, width, barHeight).fill("#0F766E");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(18)
    .text("CRC", left + 16, top + 16);
  doc
    .font("Helvetica")
    .fontSize(11)
    .text(title, left, top + 20, { width: width - 16, align: "right" });
  doc.restore();

  doc.moveDown(2.4);

  doc
    .font("Helvetica-Bold")
    .fontSize(14)
    .fillColor("#111827")
    .text(reference || "Loan Application");
  doc
    .font("Helvetica")
    .fontSize(10)
    .fillColor("#6B7280")
    .text(
      `Status: ${String(status || "pending")
        .replace(/_/g, " ")
        .toUpperCase()}`,
    );
  doc.moveDown(0.8);
}

function drawSectionTitle(doc, label) {
  ensureSpace(doc, 60);
  doc.font("Helvetica-Bold").fontSize(11.5).fillColor("#111827").text(label);
  doc.moveDown(0.4);
}

function drawLabelValueTable(doc, rows, contentWidth) {
  const tableTop = doc.y + 4;
  const labelWidth = contentWidth * 0.35;
  const valueWidth = contentWidth - labelWidth;
  const rowHeight = 24;

  rows.forEach((row, index) => {
    const y = tableTop + index * rowHeight;
    doc
      .rect(doc.page.margins.left, y, contentWidth, rowHeight)
      .strokeColor("#E5E7EB")
      .lineWidth(0.5)
      .stroke();
    if (index % 2 === 0) {
      doc
        .rect(doc.page.margins.left, y, contentWidth, rowHeight)
        .fillOpacity(0.05)
        .fill("#111827")
        .fillOpacity(1);
    }
    doc
      .font("Helvetica-Bold")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(row.label, doc.page.margins.left + 10, y + 7, {
        width: labelWidth - 16,
        align: "left",
      });
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#111827")
      .text(row.value, doc.page.margins.left + labelWidth, y + 7, {
        width: valueWidth - 12,
        align: "left",
      });
  });

  doc.y = tableTop + rows.length * rowHeight + 10;
}

export async function generateLoanApplicationPdfBuffer(payload) {
  const loan = payload.loan || {};
  const applicant = payload.applicant || {};
  const organization = payload.organization || {};

  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: "Loan Application Summary" },
  });
  const rawWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const contentWidth = Number.isFinite(rawWidth) ? rawWidth : 500;
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    const reference = loan.loanCode || loan.loanNumber || loan._id || "Loan";

    drawHeader(doc, {
      title: "Loan Application Summary",
      status: loan.status,
      reference: `Loan ${reference}`,
    });

    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor("#111827")
      .text(organization.name || "Champions Revolving Contributions");
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(organization.subtitle || "Loan Processing Desk");

    doc.moveDown(1);

    drawSectionTitle(doc, "Applicant & Group");
    drawLabelValueTable(
      doc,
      [
        {
          label: "Applicant",
          value: applicant.fullName || applicant.name || "-",
        },
        { label: "Email", value: applicant.email || "-" },
        { label: "Phone", value: applicant.phone || "-" },
        { label: "Group", value: loan.groupName || "-" },
        { label: "Loan Type", value: loan.loanType || "-" },
      ],
      contentWidth,
    );

    drawSectionTitle(doc, "Loan Details");
    drawLabelValueTable(
      doc,
      [
        {
          label: "Loan Code",
          value:
            loan.loanCode || (loan.loanNumber ? `LN-${loan.loanNumber}` : "-"),
        },
        { label: "Loan Purpose", value: loan.loanPurpose || "-" },
        { label: "Requested Amount", value: formatCurrency(loan.loanAmount) },
        {
          label: "Approved Amount",
          value:
            loan.approvedAmount != null
              ? formatCurrency(loan.approvedAmount)
              : "-",
        },
        {
          label: "Interest Rate",
          value: formatRate(
            loan.approvedInterestRate ?? loan.interestRate,
            loan.interestRateType,
          ),
        },
        {
          label: "Repayment Period",
          value: loan.repaymentPeriod ? `${loan.repaymentPeriod} months` : "-",
        },
        {
          label: "Monthly Payment",
          value:
            loan.monthlyPayment != null
              ? formatCurrency(loan.monthlyPayment)
              : "-",
        },
        {
          label: "Total Repayable",
          value:
            loan.totalRepayable != null
              ? formatCurrency(loan.totalRepayable)
              : "-",
        },
        {
          label: "Remaining Balance",
          value:
            loan.remainingBalance != null
              ? formatCurrency(loan.remainingBalance)
              : "-",
        },
      ],
      contentWidth,
    );

    drawSectionTitle(doc, "Purpose Description");
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(loan.purposeDescription || "No additional description provided.");

    doc.moveDown(0.8);

    drawSectionTitle(doc, "Financial Profile");
    const monthlyIncome = Number(loan.monthlyIncome || 0);
    const monthlyPayment = Number(loan.monthlyPayment || 0);
    const dti =
      monthlyIncome > 0 && monthlyPayment > 0
        ? `${((monthlyPayment / monthlyIncome) * 100).toFixed(1)}%`
        : "-";
    drawLabelValueTable(
      doc,
      [
        {
          label: "Monthly Income",
          value: monthlyIncome ? formatCurrency(monthlyIncome) : "-",
        },
        { label: "Debt-to-Income", value: dti },
      ],
      contentWidth,
    );

    drawSectionTitle(doc, "Timeline");
    drawLabelValueTable(
      doc,
      [
        { label: "Submitted", value: formatDate(loan.createdAt) },
        { label: "Approved", value: formatDate(loan.approvedAt) },
        { label: "Disbursed", value: formatDate(loan.disbursedAt) },
        {
          label: "Repayment Start",
          value: formatDate(loan.repaymentStartDate),
        },
      ],
      contentWidth,
    );

    drawSectionTitle(doc, "Review Notes");
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(loan.reviewNotes || "No review notes recorded.");

    doc.moveDown(0.8);

    drawSectionTitle(doc, "Guarantors");
    const guarantors = Array.isArray(loan.guarantors) ? loan.guarantors : [];
    if (guarantors.length === 0) {
      doc
        .font("Helvetica")
        .fontSize(9.5)
        .fillColor("#6B7280")
        .text("No guarantors submitted.");
      doc.moveDown(0.8);
    } else {
      guarantors.forEach((g) => {
        ensureSpace(doc, 40);
        doc
          .font("Helvetica-Bold")
          .fontSize(10)
          .fillColor("#111827")
          .text(g.name || "Guarantor");
        doc
          .font("Helvetica")
          .fontSize(9)
          .fillColor("#6B7280")
          .text(`Relationship: ${g.relationship || g.type || "-"}`);
        doc
          .font("Helvetica")
          .fontSize(9)
          .fillColor("#6B7280")
          .text(`Contact: ${g.phone || g.email || "-"}`);
        doc
          .font("Helvetica")
          .fontSize(9)
          .fillColor("#6B7280")
          .text(
            `Liability: ${g.liabilityPercentage != null ? `${g.liabilityPercentage}%` : "-"}`,
          );
        doc.moveDown(0.6);
      });
    }

    drawSectionTitle(doc, "Documents");
    const documents = Array.isArray(loan.documents) ? loan.documents : [];
    if (documents.length === 0) {
      doc
        .font("Helvetica")
        .fontSize(9.5)
        .fillColor("#6B7280")
        .text("No documents uploaded.");
      doc.moveDown(0.8);
    } else {
      documents.forEach((docItem) => {
        ensureSpace(doc, 36);
        doc
          .font("Helvetica-Bold")
          .fontSize(9.5)
          .fillColor("#111827")
          .text(docItem.name || "Document");
        doc
          .font("Helvetica")
          .fontSize(9)
          .fillColor("#6B7280")
          .text(
            `Type: ${docItem.type || "-"} | Size: ${formatFileSize(docItem.size)} | Status: ${docItem.status || "-"}`,
          );
        if (docItem.url) {
          doc
            .font("Helvetica")
            .fontSize(8.5)
            .fillColor("#9CA3AF")
            .text(`URL: ${docItem.url}`);
        }
        doc.moveDown(0.5);
      });
    }

    doc
      .moveDown(1)
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor("#9CA3AF")
      .text(`Generated ${formatDate(new Date())}`, { align: "right" });

    doc.end();
  });
}
