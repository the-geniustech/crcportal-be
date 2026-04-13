import json
import sys
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def parse_iso(value):
    if not value:
        return ""
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def format_datetime(value):
    dt = parse_iso(value)
    if not dt:
        return str(value) if value else ""
    return dt.strftime("%d %b %Y, %I:%M %p")


def format_amount(amount, currency="NGN"):
    try:
        number = float(amount)
    except Exception:
        number = 0.0
    return f"{currency} {number:,.2f}"


def status_color(status):
    status = str(status or "").lower()
    if status == "success":
        return colors.HexColor("#10B981")
    if status == "pending":
        return colors.HexColor("#F59E0B")
    if status == "failed":
        return colors.HexColor("#EF4444")
    return colors.HexColor("#6B7280")


def build_receipt(payload, output_path):
    org = payload.get("organization") or {}
    receipt = payload.get("receipt") or {}
    member = payload.get("member") or {}

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
        title="CRC Payment Receipt",
    )

    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="HeaderLeft",
            parent=styles["Heading1"],
            fontSize=20,
            textColor=colors.white,
            leading=22,
        )
    )
    styles.add(
        ParagraphStyle(
            name="HeaderRight",
            parent=styles["Heading2"],
            fontSize=14,
            textColor=colors.white,
            alignment=2,
        )
    )
    styles.add(
        ParagraphStyle(
            name="SectionTitle",
            parent=styles["Heading3"],
            fontSize=12,
            textColor=colors.HexColor("#111827"),
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Muted",
            parent=styles["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#6B7280"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="Amount",
            parent=styles["Heading1"],
            fontSize=26,
            leading=30,
            alignment=1,
            textColor=colors.HexColor("#111827"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="Badge",
            parent=styles["Normal"],
            fontSize=9,
            alignment=1,
            textColor=colors.white,
        )
    )

    story = []

    header = Table(
        [
            [
                Paragraph("CRC", styles["HeaderLeft"]),
                Paragraph("Payment Receipt", styles["HeaderRight"]),
            ]
        ],
        colWidths=[doc.width * 0.35, doc.width * 0.65],
    )
    header.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#10B981")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 14),
                ("RIGHTPADDING", (0, 0), (-1, -1), 14),
                ("TOPPADDING", (0, 0), (-1, -1), 12),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ]
        )
    )
    story.append(header)
    story.append(Spacer(1, 8))

    org_name = org.get("name") or "Champions Revolving Contributions"
    org_subtitle = org.get("subtitle") or "Ogun Baptist Conference Secretariat"
    story.append(Paragraph(org_name, styles["SectionTitle"]))
    story.append(Paragraph(org_subtitle, styles["Muted"]))
    story.append(Spacer(1, 10))

    badge_color = status_color(receipt.get("status"))
    badge_label = f"{str(receipt.get('status') or 'pending').title()} Payment"
    badge = Table([[Paragraph(badge_label, styles["Badge"])]], colWidths=[doc.width * 0.35])
    badge.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), badge_color),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(badge)
    story.append(Spacer(1, 12))

    amount_text = format_amount(receipt.get("amount"), receipt.get("currency", "NGN"))
    story.append(Paragraph(amount_text, styles["Amount"]))
    story.append(Paragraph("Amount Paid", styles["Muted"]))
    story.append(Spacer(1, 14))

    details = [
        ["Reference", receipt.get("reference") or "-"],
        ["Date & Time", format_datetime(receipt.get("date"))],
        ["Payment Type", receipt.get("typeLabel") or "-"],
        ["Status", str(receipt.get("status") or "-").title()],
    ]
    if receipt.get("channel"):
        details.append(["Channel", str(receipt.get("channel")).replace("_", " ").title()])
    if receipt.get("groupName"):
        details.append(["Group", receipt.get("groupName")])
    if receipt.get("loanName"):
        details.append(["Loan", receipt.get("loanName")])
    if receipt.get("gateway"):
        details.append(["Gateway", receipt.get("gateway")])

    detail_table = Table(details, colWidths=[doc.width * 0.35, doc.width * 0.65])
    detail_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
            ]
        )
    )
    story.append(detail_table)
    story.append(Spacer(1, 12))

    story.append(Paragraph("Description", styles["SectionTitle"]))
    description = receipt.get("description") or "Payment received."
    story.append(Paragraph(description, styles["Normal"]))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Member Details", styles["SectionTitle"]))
    member_rows = [
        ["Full Name", member.get("name") or "Member"],
        ["Email", member.get("email") or "-"],
        ["Phone", member.get("phone") or "-"],
    ]
    member_table = Table(member_rows, colWidths=[doc.width * 0.35, doc.width * 0.65])
    member_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F9FAFB")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9.5),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(member_table)
    story.append(Spacer(1, 16))

    footer_lines = [
        org.get("addressLine1") or "Olabisi Onabanjo Way, Idi Aba",
        org.get("addressLine2") or "Abeokuta, Ogun State",
        org.get("phone") or "Phone: 08060707575",
        org.get("email") or "Email: olayoyinoyeniyi@gmail.com",
    ]
    story.append(Paragraph("Thank you for your payment!", styles["SectionTitle"]))
    for line in footer_lines:
        story.append(Paragraph(line, styles["Muted"]))

    issued_at = receipt.get("issuedAt")
    if issued_at:
        story.append(Spacer(1, 8))
        story.append(Paragraph(f"Issued: {format_datetime(issued_at)}", styles["Muted"]))

    doc.build(story)


def main():
    if len(sys.argv) < 3:
        raise SystemExit("Usage: generateReceiptPdf.py <input_json> <output_pdf>")

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    with open(input_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    build_receipt(payload, output_path)


if __name__ == "__main__":
    main()
