function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0.00";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatCurrencyHtml(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "&#8358;0.00";
  return `&#8358;${value.toLocaleString("en-NG", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatDateLabel(value) {
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

export const receiptTypeLabels = {
  deposit: "Savings Deposit",
  loan_disbursement: "Loan Disbursement",
  loan_repayment: "Loan Repayment",
  group_contribution: "Group Contribution",
  withdrawal: "Withdrawal",
  interest: "Interest",
};

export const receiptOrganizationInfo = {
  name: "Champions Revolving Contributions",
  subtitle: "Ogun Baptist Conference Secretariat",
  addressLine1: "Olabisi Onabanjo Way, Idi Aba",
  addressLine2: "Abeokuta, Ogun State",
  phone: "Phone: 08060707575",
  email: "Email: olayoyinoyeniyi@gmail.com",
};

export function buildReceiptPayload({ tx, profile }) {
  const dateValue = tx?.date?.toISOString?.() || tx?.date;
  return {
    organization: receiptOrganizationInfo,
    receipt: {
      reference: tx?.reference,
      amount: tx?.amount,
      currency: "NGN",
      status: tx?.status,
      typeLabel: receiptTypeLabels[tx?.type] || tx?.type,
      description: tx?.description,
      date: dateValue,
      dateLabel: formatDateLabel(dateValue),
      channel: tx?.channel,
      groupName: tx?.groupName,
      loanName: tx?.loanName,
      gateway: tx?.gateway,
      issuedAt: new Date().toISOString(),
      issuedAtLabel: formatDateLabel(new Date().toISOString()),
    },
    member: {
      name: profile?.fullName || "Member",
      email: profile?.email || null,
      phone: profile?.phone || null,
    },
  };
}

export function buildReceiptEmailText(payload) {
  const receipt = payload?.receipt || {};
  const member = payload?.member || {};
  return [
    "CRC Payment Receipt",
    `Reference: ${receipt.reference || "-"}`,
    `Amount: ${formatCurrency(receipt.amount)}`,
    `Type: ${receipt.typeLabel || "-"}`,
    `Status: ${String(receipt.status || "-").toUpperCase()}`,
    `Date: ${receipt.dateLabel || receipt.date || "-"}`,
    `Description: ${receipt.description || "-"}`,
    receipt.groupName ? `Group: ${receipt.groupName}` : null,
    receipt.loanName ? `Loan: ${receipt.loanName}` : null,
    receipt.channel ? `Channel: ${receipt.channel}` : null,
    "",
    `Member: ${member.name || "Member"}`,
    member.email ? `Email: ${member.email}` : null,
    member.phone ? `Phone: ${member.phone}` : null,
    "",
    "Thank you for your payment!",
    receiptOrganizationInfo.subtitle,
    receiptOrganizationInfo.addressLine1,
    receiptOrganizationInfo.addressLine2,
    receiptOrganizationInfo.phone,
    receiptOrganizationInfo.email,
  ]
    .filter(Boolean)
    .join("\n");
}

export function buildReceiptEmailHtml(payload) {
  const receipt = payload?.receipt || {};
  const member = payload?.member || {};
  return `
    <div style="font-family: Arial, sans-serif; background: #f9fafb; padding: 24px;">
      <div style="max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden; border: 1px solid #e5e7eb;">
        <div style="background: #10b981; color: #ffffff; padding: 18px 24px; display: flex; justify-content: space-between; align-items: center;">
          <div style="font-size: 20px; font-weight: 700;">CRC</div>
          <div style="font-size: 14px;">Payment Receipt</div>
        </div>
        <div style="padding: 24px;">
          <div style="color: #6b7280; font-size: 12px;">${receiptOrganizationInfo.name}</div>
          <div style="color: #111827; font-size: 14px; font-weight: 600;">${receiptOrganizationInfo.subtitle}</div>
          <div style="margin-top: 12px; display: inline-block; background: #ecfdf3; color: #065f46; padding: 6px 12px; border-radius: 999px; font-size: 12px;">
            ${String(receipt.status || "pending").toUpperCase()} PAYMENT
          </div>
          <div style="text-align: center; margin: 20px 0;">
            <div style="font-size: 32px; font-weight: 700; color: #111827;">${formatCurrencyHtml(receipt.amount)}</div>
            <div style="color: #6b7280; font-size: 13px;">${receipt.typeLabel || "-"}</div>
          </div>
          <div style="background: #f9fafb; border-radius: 10px; padding: 16px;">
            <table style="width: 100%; font-size: 13px; color: #111827;">
              <tr><td style="padding: 6px 0; color: #6b7280;">Reference</td><td style="padding: 6px 0; font-weight: 600;">${receipt.reference || "-"}</td></tr>
              <tr><td style="padding: 6px 0; color: #6b7280;">Date & Time</td><td style="padding: 6px 0; font-weight: 600;">${receipt.dateLabel || receipt.date || "-"}</td></tr>
              <tr><td style="padding: 6px 0; color: #6b7280;">Status</td><td style="padding: 6px 0; font-weight: 600;">${String(receipt.status || "-").toUpperCase()}</td></tr>
              ${receipt.channel ? `<tr><td style="padding: 6px 0; color: #6b7280;">Channel</td><td style="padding: 6px 0; font-weight: 600;">${String(receipt.channel).replace("_", " ").toUpperCase()}</td></tr>` : ""}
              ${receipt.groupName ? `<tr><td style="padding: 6px 0; color: #6b7280;">Group</td><td style="padding: 6px 0; font-weight: 600;">${receipt.groupName}</td></tr>` : ""}
              ${receipt.loanName ? `<tr><td style="padding: 6px 0; color: #6b7280;">Loan</td><td style="padding: 6px 0; font-weight: 600;">${receipt.loanName}</td></tr>` : ""}
            </table>
          </div>
          <div style="margin-top: 16px;">
            <div style="font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.08em;">Description</div>
            <div style="font-size: 14px; color: #111827; margin-top: 4px;">${receipt.description || "-"}</div>
          </div>
          <div style="margin-top: 16px;">
            <div style="font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.08em;">Member</div>
            <div style="font-size: 14px; color: #111827; margin-top: 4px;">${member.name || "Member"}</div>
            ${member.email ? `<div style="font-size: 12px; color: #6b7280;">${member.email}</div>` : ""}
          </div>
        </div>
        <div style="padding: 16px 24px; background: #f9fafb; font-size: 11px; color: #6b7280; text-align: center;">
          <div>Thank you for your payment!</div>
          <div>${receiptOrganizationInfo.addressLine1}</div>
          <div>${receiptOrganizationInfo.addressLine2}</div>
          <div>${receiptOrganizationInfo.phone} | ${receiptOrganizationInfo.email}</div>
        </div>
      </div>
    </div>
  `;
}
