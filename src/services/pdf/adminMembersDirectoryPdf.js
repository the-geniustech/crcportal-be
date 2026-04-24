import PDFDocument from "pdfkit";

function formatCount(value) {
  const safe = Number(value || 0);
  return Number.isFinite(safe) ? safe.toLocaleString("en-NG") : "0";
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

function drawHeader(doc, { generatedAt, totalRecords }) {
  const { left, right, top } = doc.page.margins;
  const width = doc.page.width - left - right;
  const bannerHeight = 56;

  doc.save();
  doc.roundedRect(left, top, width, bannerHeight, 10).fill("#0F766E");
  doc
    .fillColor("#ffffff")
    .font("Helvetica-Bold")
    .fontSize(18)
    .text("Members Management Report", left + 18, top + 16);
  doc
    .font("Helvetica")
    .fontSize(10)
    .text(`Generated ${formatDate(generatedAt)}`, left, top + 18, {
      width: width - 18,
      align: "right",
    });
  doc
    .font("Helvetica")
    .fontSize(9)
    .text(`${formatCount(totalRecords)} member records in scope`, left, top + 34, {
      width: width - 18,
      align: "right",
    });
  doc.restore();

  doc.moveDown(2.4);
}

function drawSummaryCards(doc, summary) {
  const cards = [
    { label: "Member Records", value: formatCount(summary.totalRecords) },
    { label: "Unique Members", value: formatCount(summary.uniqueMembers) },
    { label: "Groups Covered", value: formatCount(summary.groupsCovered) },
    { label: "Active Members", value: formatCount(summary.activeMembers) },
    {
      label: "Needs Attention",
      value: formatCount(
        Number(summary.pendingMembers || 0) + Number(summary.suspendedMembers || 0),
      ),
    },
    { label: "Added This Month", value: formatCount(summary.newThisMonth) },
  ];

  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const gap = 10;
  const columns = 3;
  const cardWidth = (width - gap * (columns - 1)) / columns;
  const cardHeight = 52;
  const startY = doc.y;

  cards.forEach((card, index) => {
    const column = index % columns;
    const row = Math.floor(index / columns);
    const x = left + column * (cardWidth + gap);
    const y = startY + row * (cardHeight + gap);

    doc.save();
    doc.roundedRect(x, y, cardWidth, cardHeight, 8).fill("#F8FAFC");
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#64748B")
      .text(card.label, x + 12, y + 11, { width: cardWidth - 24 });
    doc
      .font("Helvetica-Bold")
      .fontSize(13)
      .fillColor("#0F172A")
      .text(card.value, x + 12, y + 28, { width: cardWidth - 24 });
    doc.restore();
  });

  doc.y = startY + Math.ceil(cards.length / columns) * (cardHeight + gap);
}

function drawScope(doc, scopeLines) {
  if (!Array.isArray(scopeLines) || scopeLines.length === 0) return;

  doc
    .font("Helvetica-Bold")
    .fontSize(10.5)
    .fillColor("#111827")
    .text("Current Scope");
  doc.moveDown(0.25);

  scopeLines.forEach((line) => {
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#4B5563")
      .text(line);
  });

  doc.moveDown(0.8);
}

function drawTableHeader(doc, columns) {
  const { left, right } = doc.page.margins;
  const width = doc.page.width - left - right;
  const headerHeight = 24;
  const startY = doc.y;

  doc.save();
  doc.rect(left, startY, width, headerHeight).fill("#E2E8F0");

  let x = left;
  columns.forEach((column) => {
    const previousX = doc.x;
    const previousY = doc.y;
    doc
      .font("Helvetica-Bold")
      .fontSize(8.5)
      .fillColor("#334155")
      .text(column.label, x + 8, startY + 7, {
        width: column.width - 12,
        height: headerHeight - 10,
        lineBreak: false,
        ellipsis: true,
      });
    doc.x = previousX;
    doc.y = previousY;
    x += column.width;
  });

  doc.restore();
  doc.y = startY + headerHeight;
}

function drawTableRow(doc, columns, row) {
  const rowHeight = 24;
  const startY = doc.y;
  let x = doc.page.margins.left;

  columns.forEach((column) => {
    const previousX = doc.x;
    const previousY = doc.y;
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor("#0F172A")
      .text(String(row[column.key] ?? "-"), x + 8, startY + 7, {
        width: column.width - 12,
        height: rowHeight - 10,
        lineBreak: false,
        ellipsis: true,
      });
    doc.x = previousX;
    doc.y = previousY;
    x += column.width;
  });

  doc
    .strokeColor("#E2E8F0")
    .lineWidth(0.5)
    .moveTo(doc.page.margins.left, startY + rowHeight)
    .lineTo(doc.page.width - doc.page.margins.right, startY + rowHeight)
    .stroke();

  doc.y = startY + rowHeight;
}

export async function generateAdminMembersDirectoryPdfBuffer({
  summary,
  rows,
  generatedAt,
  scopeLines,
}) {
  const doc = new PDFDocument({
    size: "A4",
    layout: "landscape",
    margin: 36,
    info: { Title: "Members Management Report" },
  });
  const chunks = [];

  return new Promise((resolve, reject) => {
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    drawHeader(doc, {
      generatedAt,
      totalRecords: summary.totalRecords,
    });
    drawSummaryCards(doc, summary);
    drawScope(doc, scopeLines);

    doc
      .font("Helvetica-Bold")
      .fontSize(10.5)
      .fillColor("#111827")
      .text("Members Table");
    doc.moveDown(0.4);

    const contentWidth =
      doc.page.width - doc.page.margins.left - doc.page.margins.right;
    const columns = [
      { key: "memberSerial", label: "Serial", width: contentWidth * 0.13 },
      { key: "fullName", label: "Member", width: contentWidth * 0.17 },
      { key: "contact", label: "Contact", width: contentWidth * 0.18 },
      { key: "groupName", label: "Group", width: contentWidth * 0.16 },
      { key: "role", label: "Role", width: contentWidth * 0.1 },
      { key: "memberStatus", label: "Member Status", width: contentWidth * 0.1 },
      { key: "profileStatus", label: "Profile Status", width: contentWidth * 0.08 },
      { key: "joinedAt", label: "Joined", width: contentWidth * 0.08 },
    ];

    drawTableHeader(doc, columns);

    const bottomY = doc.page.height - doc.page.margins.bottom;
    rows.forEach((row) => {
      if (doc.y + 28 > bottomY) {
        doc.addPage();
        drawTableHeader(doc, columns);
      }

      const contact =
        row.email && row.email !== "-" && row.phone && row.phone !== "-"
          ? `${row.email} / ${row.phone}`
          : row.email && row.email !== "-"
            ? row.email
            : row.phone || "-";

      drawTableRow(doc, columns, {
        memberSerial: row.memberSerial,
        fullName: row.fullName,
        contact,
        groupName: row.groupName,
        role: row.role,
        memberStatus: row.memberStatus,
        profileStatus: row.profileStatus,
        joinedAt: row.joinedAt,
      });
    });

    doc
      .moveDown(1.2)
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor("#94A3B8")
      .text("CRC Champions Revolving Contributions - Admin Members Export", {
        align: "center",
      });

    doc.end();
  });
}
