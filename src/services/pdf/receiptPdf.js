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
  if (!value) return "";
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

function drawHeader(doc, { title }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const height = 56;
  doc.save();
  doc.rect(left, top, width, height).fill("#10B981");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(20)
    .text("CRC", left + 16, top + 16);
  doc
    .font("Helvetica")
    .fontSize(12)
    .text(title, left, top + 20, { width: width - 16, align: "right" });
  doc.restore();
  doc.moveDown(2.2);
}

function drawPill(doc, text, color, contentWidth) {
  const { left } = doc.page.margins;
  const pillWidth = contentWidth * 0.35;
  const pillHeight = 20;
  const y = doc.y;
  doc.save();
  doc.roundedRect(left, y, pillWidth, pillHeight, 9).fill(color);
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(9)
    .text(text, left, y + 5, { width: pillWidth, align: "center" });
  doc.restore();
  doc.moveDown(1.4);
}

function drawSectionTitle(doc, label) {
  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor("#111827")
    .text(label);
}

function drawLabelValueTable(doc, rows, contentWidth) {
  const tableTop = doc.y + 8;
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
    if (index === 0) {
      doc
        .rect(doc.page.margins.left, y, contentWidth, rowHeight)
        .fillOpacity(0.07)
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

export async function generateReceiptPdfBuffer(payload) {
  const receipt = payload.receipt || {};
  const organization = payload.organization || {};
  const member = payload.member || {};

  const doc = new PDFDocument({
    size: "A4",
    margin: 42,
    info: { Title: "CRC Receipt" },
  });
  const rawWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const contentWidth = Number.isFinite(rawWidth) ? rawWidth : 500;
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, { title: "Payment Receipt" });

    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor("#111827")
      .text(organization.name || "Cooperative Resource Center");
    doc
      .font("Helvetica")
      .fontSize(10)
      .fillColor("#6B7280")
      .text(organization.subtitle || "Ogun Baptist Conference Secretariat");

    drawPill(
      doc,
      `${String(receipt.status || "pending").toUpperCase()} PAYMENT`,
      receipt.status === "success"
        ? "#10B981"
        : receipt.status === "failed"
        ? "#EF4444"
        : "#F59E0B",
      contentWidth,
    );

    doc
      .font("Helvetica-Bold")
      .fontSize(26)
      .fillColor("#111827")
      .text(formatCurrency(receipt.amount), { align: "center" });
    doc
      .font("Helvetica")
      .fontSize(10)
      .fillColor("#6B7280")
      .text(receipt.typeLabel || "Payment", { align: "center" });

    doc.moveDown(1.2);

    const detailRows = [
      { label: "Reference", value: receipt.reference || "-" },
      { label: "Date & Time", value: receipt.dateLabel || formatDate(receipt.date) || "-" },
      { label: "Payment Type", value: receipt.typeLabel || "-" },
      { label: "Status", value: String(receipt.status || "-").toUpperCase() },
    ];

    if (receipt.channel) {
      detailRows.push({
        label: "Channel",
        value: String(receipt.channel).replace("_", " ").toUpperCase(),
      });
    }
    if (receipt.groupName) {
      detailRows.push({ label: "Group", value: receipt.groupName });
    }
    if (receipt.loanName) {
      detailRows.push({ label: "Loan", value: receipt.loanName });
    }
    if (receipt.gateway) {
      detailRows.push({ label: "Gateway", value: receipt.gateway });
    }

    drawLabelValueTable(doc, detailRows, contentWidth);

    drawSectionTitle(doc, "Description");
    doc
      .font("Helvetica")
      .fontSize(10.5)
      .fillColor("#111827")
      .text(receipt.description || "Payment received.");

    doc.moveDown(1.0);
    drawSectionTitle(doc, "Member Details");

    drawLabelValueTable(doc, [
      { label: "Full Name", value: member.name || "Member" },
      { label: "Email", value: member.email || "-" },
      { label: "Phone", value: member.phone || "-" },
    ], contentWidth);

    doc
      .font("Helvetica-Bold")
      .fontSize(11)
      .fillColor("#111827")
      .text("Thank you for your payment!", { align: "center" });
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(organization.addressLine1 || "Olabisi Onabanjo Way, Idi Aba", {
        align: "center",
      });
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(organization.addressLine2 || "Abeokuta, Ogun State", {
        align: "center",
      });
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#6B7280")
      .text(`${organization.phone || "Phone: 08060707575"} | ${organization.email || "Email: olayoyinoyeniyi@gmail.com"}`, {
        align: "center",
      });

    if (receipt.issuedAt || receipt.issuedAtLabel) {
      doc
        .moveDown(0.6)
        .font("Helvetica-Oblique")
        .fontSize(8.5)
        .fillColor("#9CA3AF")
        .text(`Issued: ${receipt.issuedAtLabel || formatDate(receipt.issuedAt)}`, {
          align: "center",
        });
    }

    doc.end();
  });
}

