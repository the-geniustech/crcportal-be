import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  LoanApplicationModel,
  LoanApplicationStatuses,
} from "../models/LoanApplication.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { ProfileModel } from "../models/Profile.js";
import { UserModel } from "../models/User.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ContributionModel } from "../models/Contribution.js";
import { TransactionModel } from "../models/Transaction.js";
import { createNotification } from "../services/notificationService.js";
import {
  applyLoanRepayment,
  buildLoanNextPaymentMap,
} from "../services/loanRepaymentService.js";
import { sendEmailOtp } from "../services/mail/sendEmailOtp.js";
import { sendPhoneOtp } from "../services/sms/sendPhoneOtp.js";
import { randomId, sha256 } from "../utils/crypto.js";
import {
  LoanFacilityTypes,
  getLoanFacility,
  getLoanInterestConfig,
  getLoanRepaymentDeadline,
  isInterestRateAllowed,
  isLoanFacilityAvailable,
  resolveInterestRate,
} from "../utils/loanPolicy.js";
import {
  createTransferRecipient,
  finalizeTransfer,
  initiateTransfer,
  listBanks as listPaystackBanks,
  resendTransferOtp,
  verifyTransfer,
} from "../services/paystack.js";
import { hasUserRole } from "../utils/roles.js";
import { normalizeNigerianPhone } from "../utils/phone.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) out[key] = obj[key];
  }
  return out;
}

function formatLoanCode(n) {
  const num = Number(n) || 0;
  return `L${String(num).padStart(3, "0")}`;
}

async function getNextLoanNumber() {
  const last = await LoanApplicationModel.findOne({
    loanNumber: { $ne: null },
  })
    .sort({ loanNumber: -1 })
    .select("loanNumber");
  return (last?.loanNumber ?? 0) + 1;
}

function parseDateOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  // eslint-disable-next-line no-restricted-globals
  return isNaN(d.getTime()) ? null : d;
}

function addMonths(date, months) {
  const d = new Date(date);
  const day = d.getDate();
  d.setMonth(d.getMonth() + months);
  if (d.getDate() < day) d.setDate(0);
  return d;
}

function withRepaymentToDate(loan) {
  const plain =
    loan && typeof loan.toObject === "function" ? loan.toObject() : loan;
  if (!plain) return plain;
  const totalRepayable = Number(plain.totalRepayable ?? 0);
  const remainingBalance = Number(plain.remainingBalance ?? 0);
  const repaymentToDate =
    Number.isFinite(totalRepayable) &&
    totalRepayable > 0 &&
    Number.isFinite(remainingBalance)
      ? Math.max(0, totalRepayable - remainingBalance)
      : null;
  return { ...plain, repaymentToDate };
}

function normalizeLoanType(raw) {
  if (!raw) return null;
  const loanTypeRaw = String(raw).trim().toLowerCase();
  if (!loanTypeRaw) return null;
  if (!LoanFacilityTypes.includes(loanTypeRaw)) {
    throw new AppError("Invalid loanType", 400);
  }
  return loanTypeRaw;
}

const LOAN_OTP_RESEND_COOLDOWN_MS = (() => {
  const secondsRaw = Number(
    process.env.LOAN_OTP_RESEND_COOLDOWN_SECONDS ||
      process.env.WITHDRAWAL_OTP_RESEND_COOLDOWN_SECONDS,
  );
  if (Number.isFinite(secondsRaw) && secondsRaw > 0) {
    return Math.round(secondsRaw * 1000);
  }
  const msRaw = Number(
    process.env.LOAN_OTP_RESEND_COOLDOWN_MS ||
      process.env.WITHDRAWAL_OTP_RESEND_COOLDOWN_MS,
  );
  if (Number.isFinite(msRaw) && msRaw > 0) return Math.round(msRaw);
  return 60_000;
})();

const MANUAL_LOAN_DISBURSEMENT_OTP_TTL_MINUTES = (() => {
  const raw = Number(process.env.LOAN_MANUAL_DISBURSEMENT_OTP_TTL_MINUTES);
  if (Number.isFinite(raw) && raw > 0) return Math.round(raw);
  return 10;
})();

const ManualLoanDisbursementMethods = [
  "cash",
  "bank_transfer",
  "bank_settlement",
  "cheque",
  "pos",
  "other",
];

function generateOtp() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

function maskPhoneNumber(phone) {
  const raw = String(phone || "").trim();
  if (raw.length <= 4) return raw;
  return `${raw.slice(0, 4)}${"*".repeat(Math.max(0, raw.length - 6))}${raw.slice(-2)}`;
}

function maskEmailAddress(email) {
  const raw = String(email || "").trim().toLowerCase();
  const atIndex = raw.indexOf("@");
  if (atIndex <= 1) return raw;
  const local = raw.slice(0, atIndex);
  const domain = raw.slice(atIndex);
  return `${local.slice(0, 1)}${"*".repeat(Math.max(1, local.length - 2))}${local.slice(-1)}${domain}`;
}

function buildManualDisbursementReference(loan) {
  const base = String(loan?.loanCode || loan?._id || "loan")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  return `manual_${base}_${Date.now().toString(36)}_${randomId(3)}`.slice(
    0,
    50,
  );
}

function normalizeManualDisbursementMethod(value) {
  const method = String(value || "")
    .trim()
    .toLowerCase();
  if (!ManualLoanDisbursementMethods.includes(method)) {
    throw new AppError("Invalid manual disbursement method", 400);
  }
  return method;
}

function clearManualDisbursementAuthorization(loan) {
  if (!loan) return;
  loan.manualDisbursementOtpHash = null;
  loan.manualDisbursementOtpExpiresAt = null;
}

function clearPendingPaystackDisbursementState(loan) {
  if (!loan) return;
  loan.payoutReference = null;
  loan.payoutGateway = null;
  loan.payoutTransferCode = null;
  loan.payoutStatus = null;
  loan.payoutOtpResentAt = null;
}

function clearPendingManualDisbursementState(loan) {
  if (!loan) return;
  clearPendingPaystackDisbursementState(loan);
  loan.manualDisbursement = null;
  clearManualDisbursementAuthorization(loan);
}

function isMissingOrStalePaystackTransferError(error) {
  const message = String(error?.message || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

  return (
    message.includes("transfer not found") ||
    message.includes("could not find transfer") ||
    message.includes("invalid paystack transfer response") ||
    message.includes("invalid transfer response") ||
    message.includes("transfer reference does not exist") ||
    message.includes("reference not found")
  );
}

function buildSupersededPaystackAttemptNote({
  reference = null,
  transferCode = null,
  status = null,
} = {}) {
  const fragments = ["Superseded Paystack disbursement attempt"];
  if (reference) fragments.push(`ref: ${reference}`);
  if (transferCode) fragments.push(`code: ${transferCode}`);
  if (status) fragments.push(`status: ${status}`);
  return fragments.join(" | ");
}

function appendOperationalNote(existing, extra) {
  const base = String(existing || "").trim();
  const addition = String(extra || "").trim();

  if (!addition) return base || null;
  if (!base) return addition;
  if (base.includes(addition)) return base;
  return `${base}\n${addition}`;
}

async function sendManualDisbursementOtp({ user, otp }) {
  if (user?.phone) {
    await sendPhoneOtp({
      toPhone: user.phone,
      otp,
      ttlMinutes: MANUAL_LOAN_DISBURSEMENT_OTP_TTL_MINUTES,
    });
    return {
      channel: "phone",
      recipient: user.phone,
      maskedRecipient: maskPhoneNumber(user.phone),
    };
  }

  if (user?.email) {
    await sendEmailOtp({
      toEmail: user.email,
      otp,
      ttlMinutes: MANUAL_LOAN_DISBURSEMENT_OTP_TTL_MINUTES,
      purpose: "manual loan disbursement confirmation",
    });
    return {
      channel: "email",
      recipient: user.email,
      maskedRecipient: maskEmailAddress(user.email),
    };
  }

  throw new AppError(
    "Authorized user must have a phone number or email to receive OTP.",
    400,
  );
}

function getAuthenticatedUserId(req) {
  return String(req?.user?._id || req?.user?.id || "");
}

function ensureManualDisbursementInitiator(loan, req) {
  const actorUserId = getAuthenticatedUserId(req);
  const initiatedByUserId = String(
    loan?.manualDisbursement?.initiatedByUserId || "",
  );

  if (!initiatedByUserId || !actorUserId || initiatedByUserId !== actorUserId) {
    throw new AppError(
      "Only the admin who requested this manual disbursement OTP can finalize or resend it.",
      403,
    );
  }
}

async function loadLoanWithManualOtpState(loanId) {
  return LoanApplicationModel.findById(loanId).select(
    "+manualDisbursementOtpHash +manualDisbursementOtpExpiresAt",
  );
}

function normalizeBankName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function buildTransferReference() {
  const stamp = Date.now().toString(36);
  const rand = randomId(4);
  return `loan_${stamp}_${rand}`.slice(0, 50);
}

function sanitizeTransferReference(input) {
  const raw = String(input || "").toLowerCase().replace(/[^a-z0-9_-]/g, "");
  if (raw.length >= 16 && raw.length <= 50) return raw;
  return buildTransferReference();
}

async function resolveBankCode({ bankCode, bankName } = {}) {
  if (bankCode) return String(bankCode).trim();
  if (!bankName) return null;
  const paystackRes = await listPaystackBanks({ country: "nigeria" });
  const banks = Array.isArray(paystackRes?.data) ? paystackRes.data : [];
  if (banks.length === 0) return null;
  const target = normalizeBankName(bankName);
  const match = banks.find(
    (bank) =>
      normalizeBankName(bank?.name) === target ||
      normalizeBankName(bank?.slug) === target,
  );
  return match?.code ? String(match.code) : null;
}

function normalizeBankAccountId(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function applyDisbursementSnapshot(payload, account) {
  if (!payload || !account) return;
  payload.disbursementBankAccountId = account._id;
  payload.disbursementBankName = account.bankName;
  payload.disbursementBankCode = account.bankCode || null;
  payload.disbursementAccountNumber = account.accountNumber;
  payload.disbursementAccountName = account.accountName;
}

function clearDisbursementSnapshot(payload) {
  if (!payload) return;
  payload.disbursementBankAccountId = null;
  payload.disbursementBankName = null;
  payload.disbursementBankCode = null;
  payload.disbursementAccountNumber = null;
  payload.disbursementAccountName = null;
}

async function resolveBorrowerBankAccount(profileId, bankAccountId) {
  const normalizedId = normalizeBankAccountId(bankAccountId);
  if (!normalizedId) return null;
  const account = await BankAccountModel.findOne({
    _id: normalizedId,
    userId: profileId,
  });
  if (!account) {
    throw new AppError("Bank account not found", 404);
  }
  return account;
}

async function selectDisbursementAccount(loan, bankAccountId) {
  if (!loan?.userId) return null;
  if (bankAccountId) {
    return BankAccountModel.findOne({
      _id: bankAccountId,
      userId: loan.userId,
    });
  }
  return BankAccountModel.findOne({ userId: loan.userId }).sort({
    isPrimary: -1,
    createdAt: -1,
  });
}

function buildManualDisbursementPayload({ loan, body, repaymentStartDate }) {
  const amount = Number(loan?.approvedAmount ?? loan?.loanAmount ?? 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new AppError("Approved loan amount is required for disbursement", 400);
  }

  const method = normalizeManualDisbursementMethod(body?.method);
  const externalReferenceRaw =
    body?.externalReference || body?.reference || body?.settlementReference;
  const externalReference = externalReferenceRaw
    ? String(externalReferenceRaw).trim()
    : null;
  const notesRaw = body?.notes ? String(body.notes).trim() : "";
  const notes = notesRaw || null;
  const occurredAt =
    parseDateOrNull(body?.occurredAt || body?.disbursedAt || body?.paidAt) ||
    new Date();

  return {
    method,
    amount,
    externalReference,
    notes,
    occurredAt,
    repaymentStartDate,
  };
}

async function buildDisbursementContext(loan) {
  if (!loan) throw new AppError("Missing loan context", 500);

  if (loan.status !== "approved") {
    throw new AppError(
      "Loan application must be approved before disbursement",
      400,
    );
  }

  const loanType = loan.loanType || "revolving";
  if (!isLoanFacilityAvailable(loanType, new Date())) {
    const facility = getLoanFacility(loanType);
    const label = facility?.label || "This loan type";
    throw new AppError(`${label} is not available at this time`, 400);
  }

  const principal = Number(loan.approvedAmount ?? loan.loanAmount);
  const rate = Number(loan.approvedInterestRate ?? loan.interestRate ?? 0);
  const termMonths = Number(loan.repaymentPeriod);
  const interestCfg = getLoanInterestConfig(loanType);

  if (
    interestCfg.termMonths &&
    Number(termMonths) !== Number(interestCfg.termMonths)
  ) {
    throw new AppError(
      `repaymentPeriod must be ${interestCfg.termMonths} months for this loan type`,
      400,
    );
  }

  if (!isInterestRateAllowed(loanType, rate)) {
    throw new AppError("interestRate is not allowed for this loan type", 400);
  }

  const guarantors = await LoanGuarantorModel.find({
    loanApplicationId: loan._id,
  });
  const appGuarantors = Array.isArray(loan.guarantors)
    ? loan.guarantors
    : [];
  const externalGuarantors = appGuarantors.filter(
    (g) => g && g.type === "external",
  );

  if (guarantors.length === 0 && externalGuarantors.length === 0) {
    throw new AppError(
      "At least one guarantor is required to disburse this loan",
      400,
    );
  }

  const liabilitySource =
    appGuarantors.length > 0 ? appGuarantors : guarantors;
  const liabilityTotal = liabilitySource.reduce(
    (sum, g) => sum + Number(g.liabilityPercentage || 0),
    0,
  );
  const allAccepted =
    guarantors.length === 0
      ? true
      : guarantors.every((g) => g.status === "accepted");
  const externalSigned =
    externalGuarantors.length === 0
      ? true
      : externalGuarantors.every((g) => isValidSignature(g.signature));

  if (liabilityTotal !== 100) {
    throw new AppError(
      "Guarantor liabilityPercentage must total 100 to disburse this loan",
      400,
    );
  }
  if (!allAccepted) {
    throw new AppError("All guarantors must accept before disbursement", 400);
  }
  if (!externalSigned) {
    throw new AppError(
      "External guarantors must provide a valid signature before disbursement",
      400,
    );
  }

  return { loanType, principal, rate, termMonths, interestCfg };
}

function buildDisbursementSchedule({
  principal,
  rate,
  termMonths,
  rateType,
  repaymentStartDate,
  loanType,
}) {
  const { items, totalRepayable, monthlyPayment } = buildRepaymentSchedule({
    principal,
    ratePct: rate,
    rateType,
    months: termMonths,
    startDate: repaymentStartDate,
  });

  const deadline = getLoanRepaymentDeadline(loanType, repaymentStartDate);
  const lastDueDate = items.length ? items[items.length - 1].dueDate : null;
  if (
    deadline &&
    lastDueDate &&
    new Date(lastDueDate).getTime() > deadline.getTime()
  ) {
    throw new AppError(
      loanType === "bridging"
        ? "Bridging loans must be fully repaid by January"
        : "Loans must be fully repaid by October",
      400,
    );
  }

  return { items, totalRepayable, monthlyPayment };
}

function resolveLoanDisbursementRepaymentStartDate(loan, rawDate) {
  return (
    parseDateOrNull(rawDate) || loan?.repaymentStartDate || addMonths(new Date(), 1)
  );
}

function syncLoanPayoutFromTransfer(
  loan,
  transfer,
  { fallbackReference = null, fallbackTransferCode = null } = {},
) {
  if (!loan) throw new AppError("Missing loan context", 500);
  if (!transfer) throw new AppError("Invalid Paystack transfer response", 502);

  const status = String(transfer?.status || "").toLowerCase();
  const reference = transfer?.reference
    ? String(transfer.reference)
    : fallbackReference || loan.payoutReference || null;
  const transferCode =
    transfer?.transfer_code || fallbackTransferCode || loan.payoutTransferCode || null;

  loan.payoutReference = reference || null;
  loan.payoutGateway = "paystack";
  loan.payoutTransferCode = transferCode;
  loan.payoutStatus = status || null;

  return { status, reference, transferCode };
}

async function persistLoanDisbursementOutcome({
  loan,
  transferStatus,
  repaymentStartDate,
  disbursedBy,
}) {
  if (transferStatus === "success") {
    const { loanType, principal, rate, termMonths, interestCfg } =
      await buildDisbursementContext(loan);

    const rateType =
      loan.interestRateType || interestCfg.rateType || "annual";

    const { items, totalRepayable, monthlyPayment } = buildDisbursementSchedule({
      principal,
      rate,
      termMonths,
      rateType,
      repaymentStartDate,
      loanType,
    });

    await applyLoanDisbursement({
      loan,
      items,
      repaymentStartDate,
      rateType,
      monthlyPayment,
      totalRepayable,
      principal,
      disbursedBy,
    });
    return;
  }

  await loan.save();
}

function isRecoverableFinalizeOtpStateError(error) {
  const message = String(error?.message || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

  return (
    message.includes("not currently awaiting otp") ||
    message.includes("not currentiy awaiting otp") ||
    message.includes("not awaiting otp") ||
    message.includes("awaiting_otp") ||
    message.includes("already finalized") ||
    message.includes("already fulfilled") ||
    message.includes("transfer has been sent")
  );
}

async function verifyAndSyncLoanDisbursementTransfer({
  loan,
  repaymentStartDate,
  disbursedBy,
}) {
  const referenceRaw = loan?.payoutReference;
  if (!referenceRaw) {
    throw new AppError("payoutReference is required to verify transfer", 400);
  }

  const verifyRes = await verifyTransfer(referenceRaw);
  const transfer = verifyRes?.data;
  if (!transfer) {
    throw new AppError("Invalid Paystack transfer response", 502);
  }

  const { status } = syncLoanPayoutFromTransfer(loan, transfer, {
    fallbackReference: referenceRaw,
  });

  await persistLoanDisbursementOutcome({
    loan,
    transferStatus: status,
    repaymentStartDate,
    disbursedBy,
  });

  return transfer;
}

async function preparePaystackDisbursementSwitch({
  loan,
  repaymentStartDate,
  disbursedBy,
}) {
  const payoutGateway = String(loan?.payoutGateway || "").toLowerCase();
  const payoutStatus = String(loan?.payoutStatus || "").toLowerCase();
  const payoutReference = loan?.payoutReference
    ? String(loan.payoutReference)
    : null;
  const payoutTransferCode = loan?.payoutTransferCode
    ? String(loan.payoutTransferCode)
    : null;

  if (payoutGateway !== "paystack" || !payoutReference) {
    return { switched: false, note: null };
  }

  if (loan?.status === "disbursed" || payoutStatus === "success") {
    throw new AppError(
      "This loan has already been disbursed through Paystack and cannot be switched to manual.",
      400,
    );
  }

  try {
    await verifyAndSyncLoanDisbursementTransfer({
      loan,
      repaymentStartDate,
      disbursedBy,
    });
  } catch (error) {
    const verificationState = isMissingOrStalePaystackTransferError(error)
      ? payoutStatus || "unverified"
      : "verification_unavailable";
    clearPendingPaystackDisbursementState(loan);
    return {
      switched: true,
      note: appendOperationalNote(
        buildSupersededPaystackAttemptNote({
          reference: payoutReference,
          transferCode: payoutTransferCode,
          status: verificationState,
        }),
        "Switching to manual disbursement after Paystack verification could not be completed.",
      ),
    };
  }

  const refreshedStatus = String(loan?.payoutStatus || "").toLowerCase();
  if (loan?.status === "disbursed" || refreshedStatus === "success") {
    throw new AppError(
      "The previous Paystack disbursement has already completed and this loan is now marked as disbursed.",
      400,
    );
  }

  const note = buildSupersededPaystackAttemptNote({
    reference: loan?.payoutReference || payoutReference,
    transferCode: loan?.payoutTransferCode || payoutTransferCode,
    status: refreshedStatus || payoutStatus || "pending",
  });

  clearPendingPaystackDisbursementState(loan);
  return { switched: true, note };
}

async function upsertLoanDisbursementTransaction({
  loan,
  principal,
  disbursedAt,
}) {
  const reference =
    loan?.payoutReference || `loan_disbursement_${String(loan?._id || randomId(6))}`;
  const channel =
    loan?.payoutGateway === "manual"
      ? loan?.manualDisbursement?.method || "manual"
      : "bank_transfer";
  const description =
    loan?.payoutGateway === "manual"
      ? `Manual loan disbursement for ${loan?.loanCode || "loan"}`
      : `Loan disbursement for ${loan?.loanCode || "loan"}`;
  const metadata = {
    loanApplicationId: loan?._id ?? null,
    loanCode: loan?.loanCode || null,
    approvedAmount: Number(principal || 0),
    payoutGateway: loan?.payoutGateway || null,
    payoutStatus: loan?.payoutStatus || null,
    disbursedAt: disbursedAt?.toISOString?.() || disbursedAt || null,
    paystackTransfer:
      loan?.payoutGateway === "paystack"
        ? {
            reference: loan?.payoutReference || reference,
            transferCode: loan?.payoutTransferCode || null,
            status: loan?.payoutStatus || null,
          }
        : null,
    manualDisbursement:
      loan?.payoutGateway === "manual" && loan?.manualDisbursement
        ? {
            method: loan.manualDisbursement.method || null,
            externalReference:
              loan.manualDisbursement.externalReference || null,
            notes: loan.manualDisbursement.notes || null,
            occurredAt:
              loan.manualDisbursement.occurredAt?.toISOString?.() ||
              loan.manualDisbursement.occurredAt ||
              null,
          }
        : null,
  };

  const basePayload = {
    userId: loan.userId,
    reference,
    amount: Number(principal || 0),
    type: "loan_disbursement",
    status: "success",
    description,
    channel,
    groupId: loan.groupId ?? null,
    groupName: loan.groupName ?? null,
    loanId: loan._id ?? null,
    loanName: loan.loanCode || null,
    gateway: loan.payoutGateway || "manual",
    metadata,
  };

  const existing = await TransactionModel.findOne({ reference });
  if (existing) {
    existing.userId = basePayload.userId;
    existing.amount = basePayload.amount;
    existing.type = basePayload.type;
    existing.status = basePayload.status;
    existing.description = basePayload.description;
    existing.channel = basePayload.channel;
    existing.groupId = basePayload.groupId;
    existing.groupName = basePayload.groupName;
    existing.loanId = basePayload.loanId;
    existing.loanName = basePayload.loanName;
    existing.gateway = basePayload.gateway;
    existing.metadata = metadata;
    await existing.save();
    return existing;
  }

  return TransactionModel.create(basePayload);
}

async function applyLoanDisbursement({
  loan,
  items,
  repaymentStartDate,
  rateType,
  monthlyPayment,
  totalRepayable,
  principal,
  disbursedBy,
}) {
  await LoanRepaymentScheduleItemModel.deleteMany({
    loanApplicationId: loan._id,
  });
  await LoanRepaymentScheduleItemModel.insertMany(
    items.map((it) => ({
      loanApplicationId: loan._id,
      ...it,
    })),
    { ordered: false },
  );

  const effectiveDisbursedAt =
    loan?.payoutGateway === "manual"
      ? parseDateOrNull(loan?.manualDisbursement?.occurredAt) || new Date()
      : new Date();

  loan.status = "disbursed";
  loan.disbursedAt = effectiveDisbursedAt;
  loan.disbursedBy = disbursedBy;
  loan.repaymentStartDate = repaymentStartDate;
  loan.interestRateType = rateType;
  loan.monthlyPayment = monthlyPayment;
  loan.totalRepayable = totalRepayable;
  loan.remainingBalance = totalRepayable;

  await loan.save();
  await upsertLoanDisbursementTransaction({
    loan,
    principal,
    disbursedAt: loan.disbursedAt,
  });

  createNotification({
    userId: loan.userId,
    title: "Loan disbursed",
    message: `Your loan ${loan.loanCode} has been disbursed.`,
    type: "loan_disbursed",
    metadata: {
      loanId: loan._id,
      loanCode: loan.loanCode,
      amount: principal,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan disbursement notification", err);
  });

  return loan;
}

function sanitizeDraftPayload(body = {}) {
  const allowed = [
    "groupId",
    "groupName",
    "loanType",
    "loanAmount",
    "loanPurpose",
    "purposeDescription",
    "repaymentPeriod",
    "interestRate",
    "interestRateType",
    "monthlyIncome",
    "documents",
    "guarantors",
    "bankAccountId",
    "draftStep",
  ];
  const payload = pick(body || {}, allowed);

  if (payload.loanType) {
    payload.loanType = normalizeLoanType(payload.loanType);
  }

  if (
    typeof payload.interestRate !== "undefined" &&
    payload.interestRate !== null &&
    payload.loanType &&
    !isInterestRateAllowed(payload.loanType, payload.interestRate)
  ) {
    throw new AppError("interestRate is not allowed for this loan type", 400);
  }

  payload.documents = Array.isArray(payload.documents) ? payload.documents : [];
  payload.guarantors = normalizeGuarantorList(payload.guarantors);

  if (payload.draftStep !== undefined && payload.draftStep !== null) {
    payload.draftStep = Math.max(0, Number(payload.draftStep) || 0);
  }

  return payload;
}

const EDIT_REQUEST_FIELDS = [
  "loanAmount",
  "loanPurpose",
  "purposeDescription",
  "repaymentPeriod",
  "documents",
  "guarantors",
  "bankAccountId",
];

function sanitizeSignatureInput(signature) {
  if (!signature || typeof signature !== "object") return null;
  const method = String(signature.method || "").trim().toLowerCase();
  const allowedMethods = new Set(["text", "draw", "upload"]);
  return {
    method: allowedMethods.has(method) ? method : null,
    text: signature.text ? String(signature.text) : "",
    font: signature.font ? String(signature.font) : "",
    imageUrl: signature.imageUrl ? String(signature.imageUrl) : null,
    imagePublicId: signature.imagePublicId
      ? String(signature.imagePublicId)
      : null,
    signedAt: parseDateOrNull(signature.signedAt),
  };
}

function sanitizeGuarantorInput(raw) {
  if (!raw || typeof raw !== "object") return null;
  const typeRaw = String(raw.type || "").trim().toLowerCase();
  const type = typeRaw === "member" || typeRaw === "external" ? typeRaw : null;
  if (!type) return null;

  const rawPhone = raw.phone ? String(raw.phone).trim() : "";
  const normalizedPhone = rawPhone ? normalizeNigerianPhone(rawPhone) : "";
  if (rawPhone && !normalizedPhone) {
    throw new AppError("Provide a valid phone number", 400);
  }

  return {
    type,
    profileId: raw.profileId ? String(raw.profileId) : null,
    name: String(raw.name || "").trim(),
    email: raw.email ? String(raw.email).trim().toLowerCase() : "",
    phone: normalizedPhone || "",
    relationship: raw.relationship ? String(raw.relationship).trim() : "",
    occupation: raw.occupation ? String(raw.occupation).trim() : "",
    address: raw.address ? String(raw.address).trim() : "",
    memberSince: raw.memberSince ? String(raw.memberSince).trim() : "",
    savingsBalance: Number.isFinite(Number(raw.savingsBalance))
      ? Number(raw.savingsBalance)
      : null,
    liabilityPercentage: Number.isFinite(Number(raw.liabilityPercentage))
      ? Number(raw.liabilityPercentage)
      : null,
    requestMessage: raw.requestMessage ? String(raw.requestMessage) : null,
    signature: sanitizeSignatureInput(raw.signature),
  };
}

function normalizeGuarantorPayload(raw) {
  if (!raw || typeof raw !== "object") return raw;
  const rawPhone = raw.phone ? String(raw.phone).trim() : "";
  if (!rawPhone) return { ...raw, phone: "" };
  const normalized = normalizeNigerianPhone(rawPhone);
  if (!normalized) {
    throw new AppError("Provide a valid phone number", 400);
  }
  return { ...raw, phone: normalized };
}

function normalizeGuarantorList(list) {
  return Array.isArray(list) ? list.map((g) => normalizeGuarantorPayload(g)) : [];
}

function sanitizeEditPayload(body = {}) {
  const payload = pick(body || {}, EDIT_REQUEST_FIELDS);
  const requestedBankAccountId =
    body?.bankAccountId ||
    body?.bank_account_id ||
    body?.disbursementBankAccountId ||
    null;
  if (requestedBankAccountId !== null) {
    payload.bankAccountId = requestedBankAccountId;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "loanAmount")) {
    payload.loanAmount = Number(payload.loanAmount);
  }
  if (Object.prototype.hasOwnProperty.call(payload, "repaymentPeriod")) {
    payload.repaymentPeriod = Number(payload.repaymentPeriod);
  }

  if (Object.prototype.hasOwnProperty.call(payload, "documents")) {
    const docs = Array.isArray(payload.documents) ? payload.documents : [];
    payload.documents = docs
      .filter((doc) => doc && typeof doc === "object")
      .map((doc) => ({
        name: String(doc.name || "document"),
        type: String(doc.type || "application/octet-stream"),
        size: Number(doc.size || 0),
        status: String(doc.status || "uploaded"),
        url: doc.url ? String(doc.url) : null,
      }));
  }

  if (Object.prototype.hasOwnProperty.call(payload, "guarantors")) {
    const guarantors = Array.isArray(payload.guarantors)
      ? payload.guarantors
      : [];
    payload.guarantors = guarantors
      .map((g) => sanitizeGuarantorInput(g))
      .filter(Boolean);
  }

  if (Object.prototype.hasOwnProperty.call(payload, "bankAccountId")) {
    payload.bankAccountId = normalizeBankAccountId(payload.bankAccountId);
  }

  return payload;
}

function summarizeDocuments(docs = []) {
  if (!Array.isArray(docs) || docs.length === 0) return "No documents";
  const names = docs.map((d) => d?.name).filter(Boolean);
  if (names.length === 0) return `${docs.length} document(s)`;
  return names.slice(0, 3).join(", ") + (names.length > 3 ? "…" : "");
}

function summarizeGuarantors(list = []) {
  if (!Array.isArray(list) || list.length === 0) return "No guarantors";
  const names = list.map((g) => g?.name).filter(Boolean);
  if (names.length === 0) return `${list.length} guarantor(s)`;
  if (names.length <= 2) return names.join(", ");
  return `${names.slice(0, 2).join(", ")} +${names.length - 2} more`;
}

function summarizeBankSnapshot({ bankName, accountNumber, accountName } = {}) {
  const name = bankName ? String(bankName) : "Bank account";
  const lastFour = accountNumber ? String(accountNumber).slice(-4) : "";
  const holder = accountName ? ` (${accountName})` : "";
  if (!lastFour) return `${name}${holder}`;
  return `${name} **** ${lastFour}${holder}`;
}

function normalizeGuarantorForCompare(guarantor = {}) {
  const normalizedPhone = normalizeNigerianPhone(guarantor?.phone || "") || "";
  const signature =
    guarantor?.signature && typeof guarantor.signature === "object"
      ? {
          method: guarantor.signature.method ?? null,
          text: guarantor.signature.text ?? null,
          font: guarantor.signature.font ?? null,
          imageUrl: guarantor.signature.imageUrl ?? null,
          imagePublicId: guarantor.signature.imagePublicId ?? null,
          signedAt: guarantor.signature.signedAt
            ? String(guarantor.signature.signedAt)
            : null,
        }
      : null;

  return {
    type: guarantor?.type || null,
    profileId: guarantor?.profileId ? String(guarantor.profileId) : null,
    name: String(guarantor?.name || "").trim(),
    email: String(guarantor?.email || "")
      .trim()
      .toLowerCase(),
    phone: normalizedPhone,
    relationship: String(guarantor?.relationship || "").trim(),
    occupation: String(guarantor?.occupation || "").trim(),
    address: String(guarantor?.address || "").trim(),
    signature,
  };
}

function buildEditChanges(current, payload) {
  const changes = [];

  if (
    Object.prototype.hasOwnProperty.call(payload, "loanAmount") &&
    Number(payload.loanAmount) !== Number(current.loanAmount)
  ) {
    changes.push({
      field: "loanAmount",
      label: "Loan Amount",
      from: Number(current.loanAmount || 0),
      to: Number(payload.loanAmount || 0),
    });
  }

  if (
    Object.prototype.hasOwnProperty.call(payload, "loanPurpose") &&
    String(payload.loanPurpose || "").trim() !==
      String(current.loanPurpose || "").trim()
  ) {
    changes.push({
      field: "loanPurpose",
      label: "Loan Purpose",
      from: String(current.loanPurpose || ""),
      to: String(payload.loanPurpose || ""),
    });
  }

  if (
    Object.prototype.hasOwnProperty.call(payload, "purposeDescription") &&
    String(payload.purposeDescription || "").trim() !==
      String(current.purposeDescription || "").trim()
  ) {
    changes.push({
      field: "purposeDescription",
      label: "Purpose Description",
      from: String(current.purposeDescription || ""),
      to: String(payload.purposeDescription || ""),
    });
  }

  if (
    Object.prototype.hasOwnProperty.call(payload, "repaymentPeriod") &&
    Number(payload.repaymentPeriod) !== Number(current.repaymentPeriod)
  ) {
    changes.push({
      field: "repaymentPeriod",
      label: "Repayment Period",
      from: Number(current.repaymentPeriod || 0),
      to: Number(payload.repaymentPeriod || 0),
    });
  }

  if (Object.prototype.hasOwnProperty.call(payload, "documents")) {
    const currentDocs = Array.isArray(current.documents) ? current.documents : [];
    const nextDocs = Array.isArray(payload.documents) ? payload.documents : [];
    const currentKey = JSON.stringify(
      currentDocs.map((doc) => ({
        name: doc.name,
        type: doc.type,
        size: doc.size,
        url: doc.url || null,
      })),
    );
    const nextKey = JSON.stringify(
      nextDocs.map((doc) => ({
        name: doc.name,
        type: doc.type,
        size: doc.size,
        url: doc.url || null,
      })),
    );

    if (currentKey !== nextKey) {
      changes.push({
        field: "documents",
        label: "Documents",
        from: summarizeDocuments(currentDocs),
        to: summarizeDocuments(nextDocs),
      });
    }
  }

  if (Object.prototype.hasOwnProperty.call(payload, "guarantors")) {
    const currentGuarantors = Array.isArray(current.guarantors)
      ? current.guarantors
      : [];
    const nextGuarantors = Array.isArray(payload.guarantors)
      ? payload.guarantors
      : [];
    const currentKey = JSON.stringify(
      currentGuarantors.map((g) => normalizeGuarantorForCompare(g)),
    );
    const nextKey = JSON.stringify(
      nextGuarantors.map((g) => normalizeGuarantorForCompare(g)),
    );
    if (currentKey !== nextKey) {
      changes.push({
        field: "guarantors",
        label: "Guarantors",
        from: summarizeGuarantors(currentGuarantors),
        to: summarizeGuarantors(nextGuarantors),
      });
    }
  }

  if (Object.prototype.hasOwnProperty.call(payload, "bankAccountId")) {
    const nextId = normalizeBankAccountId(payload.bankAccountId);
    const currentId = normalizeBankAccountId(current.disbursementBankAccountId);
    if (nextId && nextId !== currentId) {
      changes.push({
        field: "bankAccountId",
        label: "Disbursement Account",
        from: summarizeBankSnapshot({
          bankName: current.disbursementBankName,
          accountNumber: current.disbursementAccountNumber,
          accountName: current.disbursementAccountName,
        }),
        to: "Updated bank account",
      });
    }
  }

  return changes;
}

function isValidSignature(signature) {
  if (!signature || typeof signature !== "object") return false;
  const method = String(signature.method || "").toLowerCase();
  const text = String(signature.text || "").trim();
  const imageUrl = String(signature.imageUrl || "").trim();

  if (method === "text") {
    return Boolean(text);
  }
  if (method === "draw" || method === "upload") {
    return Boolean(imageUrl);
  }

  // Backwards/lenient support: accept if either form is present.
  return Boolean(text || imageUrl);
}

async function validateEditGuarantors({
  guarantors,
  borrowerProfileId,
  borrowerProfile,
  groupId,
}) {
  const list = Array.isArray(guarantors) ? guarantors : [];
  const memberGuarantors = list.filter((g) => g && g.type === "member");
  const externalGuarantors = list.filter((g) => g && g.type === "external");

  const normalizeEmail = (value) =>
    String(value || "")
      .trim()
      .toLowerCase();
  const normalizePhone = (value) => normalizeNigerianPhone(value) || "";

  const borrowerEmail = normalizeEmail(borrowerProfile?.email);
  const borrowerPhone = normalizePhone(borrowerProfile?.phone);

  let liabilitySum = 0;
  const seenProfiles = new Set();
  const seenExternal = new Set();

  for (const g of memberGuarantors) {
    if (!g.profileId) {
      throw new AppError("Member guarantors must include profileId", 400);
    }
    const profileId = String(g.profileId);
    if (profileId === borrowerProfileId) {
      throw new AppError("Borrower cannot be a guarantor", 400);
    }
    if (seenProfiles.has(profileId)) {
      throw new AppError("Duplicate guarantor profileId", 400);
    }
    seenProfiles.add(profileId);

    const pct = Number(g.liabilityPercentage);
    if (!pct || pct < 1 || pct > 100) {
      throw new AppError("Invalid guarantor liabilityPercentage", 400);
    }
    liabilitySum += pct;
  }

  for (const g of externalGuarantors) {
    const emailKey = normalizeEmail(g.email);
    const phoneKey = normalizePhone(g.phone);
    if (borrowerEmail && emailKey && emailKey === borrowerEmail) {
      throw new AppError("Borrower cannot be a guarantor", 400);
    }
    if (borrowerPhone && phoneKey && phoneKey === borrowerPhone) {
      throw new AppError("Borrower cannot be a guarantor", 400);
    }
    if (!isValidSignature(g.signature)) {
      throw new AppError(
        "External guarantors must provide a valid signature",
        400,
      );
    }
    const key = emailKey || phoneKey ? `${emailKey}::${phoneKey}` : null;
    if (key && seenExternal.has(key)) {
      throw new AppError("Duplicate external guarantor", 400);
    }
    if (key) seenExternal.add(key);
  }

  if (liabilitySum > 100) {
    throw new AppError("Total liabilityPercentage cannot exceed 100", 400);
  }

  if (groupId) {
    for (const g of memberGuarantors) {
      const membership = await GroupMembershipModel.findOne({
        groupId,
        userId: g.profileId,
        status: "active",
      });
      if (!membership) {
        throw new AppError(
          "All member guarantors must be active group members",
          400,
        );
      }
    }
  }
}

function buildRepaymentSchedule({
  principal,
  ratePct,
  rateType,
  months,
  startDate,
}) {
  const P = Number(principal);
  const n = Math.max(1, Number(months) | 0);
  const rate = Math.max(0, Number(ratePct) || 0);
  const normalizedType = rateType || "annual";

  const items = [];

  if (normalizedType === "total") {
    const totalInterest = Math.round(P * (rate / 100));
    const basePayment = (P + totalInterest) / n;
    let remainingPrincipal = P;
    let remainingInterest = totalInterest;

    for (let i = 1; i <= n; i += 1) {
      const interest =
        i === n ? remainingInterest : Math.round(totalInterest / n);
      const principalPaid =
        i === n ? remainingPrincipal : Math.round(basePayment - interest);
      const total = principalPaid + interest;

      remainingPrincipal = Math.max(0, remainingPrincipal - principalPaid);
      remainingInterest = Math.max(0, remainingInterest - interest);

      const dueDate = addMonths(startDate, i - 1);

      items.push({
        installmentNumber: i,
        dueDate,
        principalAmount: Math.round(principalPaid),
        interestAmount: Math.round(interest),
        totalAmount: Math.round(total),
        status: i === 1 ? "pending" : "upcoming",
      });
    }

    const totalRepayable = items.reduce((sum, it) => sum + it.totalAmount, 0);
    const monthlyPayment = items[0]?.totalAmount ?? Math.round(basePayment);

    return { items, totalRepayable, monthlyPayment };
  }

  const monthlyRate =
    normalizedType === "monthly" ? rate / 100 : rate / 100 / 12;

  const payment =
    monthlyRate === 0
      ? P / n
      : (P * monthlyRate * Math.pow(1 + monthlyRate, n)) /
        (Math.pow(1 + monthlyRate, n) - 1);

  let balance = P;

  for (let i = 1; i <= n; i += 1) {
    const interest = monthlyRate === 0 ? 0 : balance * monthlyRate;
    let principalPaid = payment - interest;

    if (i === n) {
      principalPaid = balance;
    }

    const total = principalPaid + interest;
    balance = Math.max(0, balance - principalPaid);

    const dueDate = addMonths(startDate, i - 1);

    items.push({
      installmentNumber: i,
      dueDate,
      principalAmount: Math.round(principalPaid),
      interestAmount: Math.round(interest),
      totalAmount: Math.round(total),
      status: i === 1 ? "pending" : "upcoming",
    });
  }

  const totalRepayable = items.reduce((sum, it) => sum + it.totalAmount, 0);
  const monthlyPayment = items[0]?.totalAmount ?? Math.round(payment);

  return { items, totalRepayable, monthlyPayment };
}

async function ensureActiveMember(profileId) {
  const profile = await ProfileModel.findById(profileId).select(
    "membershipStatus email phone",
  );
  if (!profile) throw new AppError("User profile not found", 400);
  if (profile.membershipStatus !== "active") {
    throw new AppError("Membership is not active", 403);
  }
  return profile;
}

export const getLoanEligibility = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const profile = await ProfileModel.findById(req.user.profileId).select(
    "createdAt membershipStatus",
  );
  const membership = await GroupMembershipModel.findOne({
    userId: req.user.profileId,
    status: "active",
  });

  if (!profile) return next(new AppError("User profile not found", 400));
  if (!membership) {
    return next(
      new AppError("You must be an active member to apply for a loan", 403),
    );
  }

  const now = new Date();
  // const createdAt = profile.createdAt ? new Date(profile.createdAt) : now;
  const joinedAt = membership.joinedAt ? new Date(membership.joinedAt) : now;
  const membershipDuration = Math.max(
    0,
    (now.getFullYear() - joinedAt.getFullYear()) * 12 +
      (now.getMonth() - joinedAt.getMonth()),
  );
  console.log("Membership Duration: ", membershipDuration);

  const [
    groupsJoined,
    contributionAgg,
    previousLoans,
    defaultedLoans,
    overdueContributions,
    activeLoans,
  ] = await Promise.all([
    GroupMembershipModel.countDocuments({
      userId: req.user.profileId,
      status: "active",
    }),
    ContributionModel.aggregate([
      {
        $match: {
          userId: profile._id,
          status: { $in: ["completed", "verified"] },
        },
      },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
    LoanApplicationModel.countDocuments({
      userId: req.user.profileId,
      status: { $in: ["disbursed", "completed", "defaulted"] },
    }),
    LoanApplicationModel.countDocuments({
      userId: req.user.profileId,
      status: "defaulted",
    }),
    ContributionModel.countDocuments({
      userId: req.user.profileId,
      status: "overdue",
    }),
    LoanApplicationModel.find(
      {
        userId: req.user.profileId,
        status: { $in: ["disbursed", "defaulted"] },
      },
      { _id: 1 },
    ).lean(),
  ]);

  const totalContributions = Number(contributionAgg?.[0]?.sum ?? 0);
  const activeLoanIds = activeLoans.map((l) => l._id);
  const overdueRepayments =
    activeLoanIds.length === 0
      ? 0
      : await LoanRepaymentScheduleItemModel.countDocuments({
          loanApplicationId: { $in: activeLoanIds },
          $or: [
            { status: "overdue" },
            {
              status: { $in: ["pending", "upcoming"] },
              dueDate: { $lt: now },
            },
          ],
        });

  const eligibility = {
    savingsBalance: totalContributions,
    totalContributions,
    membershipDuration,
    groupsJoined,
    attendanceRate: 92,
    contributionStreak: 12,
    previousLoans,
    defaultedLoans,
    overdueContributions,
    overdueRepayments,
    creditScore: 850,
    contributionWindow: {
      startDay: 1,
      endDay: 31,
      isOpen: true,
    },
  };

  return sendSuccess(res, { statusCode: 200, data: { eligibility } });
});

export const createLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const draftId = req.body?.draftId || req.body?.applicationId || null;
  let draftApplication = null;
  if (draftId) {
    draftApplication = await LoanApplicationModel.findById(draftId);
    if (!draftApplication) {
      return next(new AppError("Draft loan application not found", 404));
    }
    if (!req.user.profileId) {
      return next(new AppError("User profile not found", 400));
    }
    if (String(draftApplication.userId) !== String(req.user.profileId)) {
      return next(new AppError("You do not have access to this draft", 403));
    }
    if (draftApplication.status !== "draft") {
      return next(
        new AppError("Only draft applications can be submitted", 400),
      );
    }
  }

  let borrowerProfile = null;
  if (!hasUserRole(req.user, "admin")) {
    borrowerProfile = await ensureActiveMember(req.user.profileId);
  } else if (req.user.profileId) {
    borrowerProfile = await ProfileModel.findById(req.user.profileId).select(
      "email phone",
    );
  }

  const allowed = [
    "groupId",
    "groupName",
    "loanType",
    "loanAmount",
    "loanPurpose",
    "purposeDescription",
    "repaymentPeriod",
    "interestRate",
    "monthlyIncome",
    "documents",
    "guarantors",
  ];

  const payload = pick(req.body || {}, allowed);
  const hasDocuments = Object.prototype.hasOwnProperty.call(
    req.body || {},
    "documents",
  );
  const hasGuarantors = Object.prototype.hasOwnProperty.call(
    req.body || {},
    "guarantors",
  );

  if (draftApplication) {
    for (const key of allowed) {
      if (
        payload[key] === undefined ||
        payload[key] === null ||
        payload[key] === ""
      ) {
        if (
          draftApplication[key] !== undefined &&
          draftApplication[key] !== null
        ) {
          payload[key] = draftApplication[key];
        }
      }
    }
    if (!hasDocuments && Array.isArray(draftApplication.documents)) {
      payload.documents = draftApplication.documents;
    }
    if (!hasGuarantors && Array.isArray(draftApplication.guarantors)) {
      payload.guarantors = draftApplication.guarantors;
    }
  }

  if (Object.prototype.hasOwnProperty.call(payload, "guarantors")) {
    payload.guarantors = normalizeGuarantorList(payload.guarantors);
  }

  const loanTypeRaw = String(payload.loanType || "")
    .trim()
    .toLowerCase();
  if (!loanTypeRaw) {
    return next(new AppError("loanType is required", 400));
  }
  if (!LoanFacilityTypes.includes(loanTypeRaw)) {
    return next(new AppError("Invalid loanType", 400));
  }
  if (!isLoanFacilityAvailable(loanTypeRaw, new Date())) {
    const facility = getLoanFacility(loanTypeRaw);
    const label = facility?.label || "This loan type";
    return next(new AppError(`${label} is not available at this time`, 400));
  }
  payload.loanType = loanTypeRaw;

  if (!hasUserRole(req.user, "admin") && !payload.groupId) {
    return next(
      new AppError("groupId is required for member loan applications", 400),
    );
  }

  if (!payload.loanAmount || Number(payload.loanAmount) <= 0) {
    return next(new AppError("loanAmount is required", 400));
  }
  if (!payload.loanPurpose)
    return next(new AppError("loanPurpose is required", 400));
  if (!payload.repaymentPeriod || Number(payload.repaymentPeriod) <= 0) {
    return next(new AppError("repaymentPeriod is required", 400));
  }

  let group = null;
  if (payload.groupId) {
    group = await GroupModel.findById(payload.groupId);
    if (!group) return next(new AppError("Group not found", 404));

    if (!hasUserRole(req.user, "admin")) {
      const membership = await GroupMembershipModel.findOne({
        groupId: group._id,
        userId: req.user.profileId,
        status: "active",
      });
      if (!membership) {
        return next(
          new AppError("You must be an active member of this group", 403),
        );
      }
    }

    payload.groupName = payload.groupName || group.groupName;
  }

  const interestConfig = getLoanInterestConfig(payload.loanType);
  if (
    interestConfig.termMonths &&
    Number(payload.repaymentPeriod) !== Number(interestConfig.termMonths)
  ) {
    return next(
      new AppError(
        `repaymentPeriod must be ${interestConfig.termMonths} months for this loan type`,
        400,
      ),
    );
  }

  if (
    typeof payload.interestRate !== "undefined" &&
    payload.interestRate !== null &&
    !isInterestRateAllowed(payload.loanType, payload.interestRate)
  ) {
    return next(
      new AppError("interestRate is not allowed for this loan type", 400),
    );
  }

  const resolvedInterest = resolveInterestRate(
    payload.loanType,
    payload.interestRate,
  );
  payload.interestRate = resolvedInterest.rate;
  payload.interestRateType = resolvedInterest.rateType;

  const requestedBankAccountId =
    req.body?.bankAccountId ||
    req.body?.bank_account_id ||
    req.body?.disbursementBankAccountId ||
    draftApplication?.disbursementBankAccountId ||
    null;

  if (requestedBankAccountId) {
    const disbursementAccount = await resolveBorrowerBankAccount(
      req.user.profileId,
      requestedBankAccountId,
    );
    applyDisbursementSnapshot(payload, disbursementAccount);
  } else if (!hasUserRole(req.user, "admin")) {
    return next(
      new AppError(
        "bankAccountId is required to submit a loan application",
        400,
      ),
    );
  }

  const now = new Date();
  const [contributionAgg, overdueContribs, defaultedLoans, activeLoans] =
    await Promise.all([
      ContributionModel.aggregate([
        {
          $match: {
            userId: req.user.profileId,
            status: { $in: ["completed", "verified"] },
          },
        },
        { $group: { _id: null, sum: { $sum: "$amount" } } },
      ]),
      ContributionModel.countDocuments({
        userId: req.user.profileId,
        status: "overdue",
      }),
      LoanApplicationModel.countDocuments({
        userId: req.user.profileId,
        status: "defaulted",
      }),
      LoanApplicationModel.find(
        {
          userId: req.user.profileId,
          status: { $in: ["disbursed", "defaulted"] },
        },
        { _id: 1 },
      ).lean(),
    ]);

  const totalContributions = Number(contributionAgg?.[0]?.sum ?? 0);

  if (
    payload.loanType === "revolving" &&
    Number(payload.loanAmount) > totalContributions
  ) {
    return next(
      new AppError(
        "Revolving loans cannot exceed your total contributions",
        400,
      ),
    );
  }

  if (overdueContribs > 0) {
    return next(
      new AppError(
        "Outstanding contributions detected. Please settle them before applying.",
        400,
      ),
    );
  }

  if (defaultedLoans > 0) {
    return next(new AppError("Defaulted loans must be resolved first", 400));
  }

  const activeLoanIds = activeLoans.map((l) => l._id);
  if (activeLoanIds.length > 0) {
    const overdueScheduleCount =
      await LoanRepaymentScheduleItemModel.countDocuments({
        loanApplicationId: { $in: activeLoanIds },
        $or: [
          { status: "overdue" },
          {
            status: { $in: ["pending", "upcoming"] },
            dueDate: { $lt: now },
          },
        ],
      });

    if (overdueScheduleCount > 0) {
      return next(
        new AppError(
          "Loan repayments are overdue. Please clear overdue installments before applying.",
          400,
        ),
      );
    }
  }

  const guarantors = Array.isArray(payload.guarantors)
    ? payload.guarantors
    : [];
  const memberGuarantors = guarantors.filter((g) => g && g.type === "member");
  const externalGuarantors = guarantors.filter(
    (g) => g && g.type === "external",
  );
  const borrowerProfileId = String(req.user.profileId);

  const normalizeEmail = (value) =>
    String(value || "")
      .trim()
      .toLowerCase();
  const normalizePhone = (value) => normalizeNigerianPhone(value) || "";
  const borrowerEmail = normalizeEmail(borrowerProfile?.email);
  const borrowerPhone = normalizePhone(borrowerProfile?.phone);

  let liabilitySum = 0;
  const seenProfiles = new Set();
  const seenExternal = new Set();

  for (const g of memberGuarantors) {
    if (!g.profileId) {
      return next(
        new AppError("Member guarantors must include profileId", 400),
      );
    }
    const profileId = String(g.profileId);
    if (profileId === borrowerProfileId) {
      return next(new AppError("Borrower cannot be a guarantor", 400));
    }
    if (seenProfiles.has(profileId)) {
      return next(new AppError("Duplicate guarantor profileId", 400));
    }
    seenProfiles.add(profileId);

    const pct = Number(g.liabilityPercentage);
    if (!pct || pct < 1 || pct > 100) {
      return next(new AppError("Invalid guarantor liabilityPercentage", 400));
    }
    liabilitySum += pct;
  }

  for (const g of externalGuarantors) {
    const emailKey = normalizeEmail(g.email);
    const phoneKey = normalizePhone(g.phone);
    if (borrowerEmail && emailKey && emailKey === borrowerEmail) {
      console.log("Borrower email match the guarantor's own", {
        borrowerEmail,
        guarantorEmail: emailKey,
      });
      return next(new AppError("Borrower cannot be a guarantor", 400));
    }
    if (borrowerPhone && phoneKey && phoneKey === borrowerPhone) {
      console.log("Borrower phone match the guarantor's own", {
        borrowerPhone,
        guarantorPhone: phoneKey,
      });
      return next(new AppError("Borrower cannot be a guarantor", 400));
    }
    if (!isValidSignature(g.signature)) {
      return next(
        new AppError("External guarantors must provide a valid signature", 400),
      );
    }
    const key = emailKey || phoneKey ? `${emailKey}::${phoneKey}` : null;
    if (key && seenExternal.has(key)) {
      return next(new AppError("Duplicate external guarantor", 400));
    }
    if (key) seenExternal.add(key);
  }

  if (liabilitySum > 100) {
    return next(
      new AppError("Total liabilityPercentage cannot exceed 100", 400),
    );
  }

  if (group) {
    for (const g of memberGuarantors) {
      const membership = await GroupMembershipModel.findOne({
        groupId: group._id,
        userId: g.profileId,
        status: "active",
      });
      if (!membership) {
        return next(
          new AppError(
            "All member guarantors must be active group members",
            400,
          ),
        );
      }
    }
  }

  const loanNumber = await getNextLoanNumber();
  const loanCode = formatLoanCode(loanNumber);

  let application;
  if (draftApplication) {
    draftApplication.set({
      ...payload,
      userId: req.user.profileId,
      groupId: payload.groupId || null,
      groupName: payload.groupName || null,
      loanNumber,
      loanCode,
      status: "pending",
      remainingBalance: 0,
      draftStep: 0,
      draftLastSavedAt: null,
    });
    application = await draftApplication.save();
  } else {
    application = await LoanApplicationModel.create({
      ...payload,
      userId: req.user.profileId,
      groupId: payload.groupId || null,
      groupName: payload.groupName || null,
      loanNumber,
      loanCode,
      status: "pending",
      remainingBalance: 0,
    });
  }

  const guarantorOps = [];
  for (const g of memberGuarantors) {
    guarantorOps.push({
      loanApplicationId: application._id,
      guarantorUserId: g.profileId,
      guarantorName: g.name,
      guarantorEmail: g.email || null,
      guarantorPhone: g.phone || null,
      liabilityPercentage: Number(g.liabilityPercentage),
      requestMessage: g.requestMessage || null,
      status: "pending",
    });
  }

  const guarantorRecords = guarantorOps.length
    ? await LoanGuarantorModel.insertMany(guarantorOps, { ordered: false })
    : [];

  if (guarantorRecords.length) {
    const notifications = guarantorRecords.map((gr) => ({
      guarantorId: gr._id,
      notificationType: "new_request",
      message: `You have a new guarantor request for loan ${application.loanCode}.`,
      sentVia: [],
      readAt: null,
    }));
    await GuarantorNotificationModel.insertMany(notifications, {
      ordered: false,
    });
  }

  createNotification({
    userId: req.user.profileId,
    title: "Loan application received",
    message: `Your loan application ${application.loanCode} has been received and is pending review.`,
    type: "loan_application",
    metadata: {
      loanId: application._id,
      loanCode: application.loanCode,
      status: application.status,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan application notification", err);
  });

  return sendSuccess(res, {
    statusCode: 201,
    data: {
      application,
      guarantorRequests: guarantorRecords,
    },
  });
});

export const createLoanDraft = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  let payload;
  try {
    payload = sanitizeDraftPayload(req.body || {});
  } catch (err) {
    return next(err);
  }

  let group = null;
  if (payload.groupId) {
    group = await GroupModel.findById(payload.groupId);
    if (!group) return next(new AppError("Group not found", 404));
    payload.groupName = payload.groupName || group.groupName;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "bankAccountId")) {
    const bankAccountId = normalizeBankAccountId(payload.bankAccountId);
    if (bankAccountId) {
      const account = await resolveBorrowerBankAccount(
        req.user.profileId,
        bankAccountId,
      );
      applyDisbursementSnapshot(payload, account);
    } else {
      clearDisbursementSnapshot(payload);
    }
    delete payload.bankAccountId;
  }

  const application = await LoanApplicationModel.create({
    ...payload,
    userId: req.user.profileId,
    groupId: payload.groupId || null,
    groupName: payload.groupName || null,
    status: "draft",
    draftStep: payload.draftStep ?? 0,
    draftLastSavedAt: new Date(),
  });

  return sendSuccess(res, {
    statusCode: 201,
    message: "Draft saved",
    data: { application },
  });
});

export const updateLoanDraft = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (req.loanApplication.status !== "draft") {
    return next(new AppError("Only draft applications can be updated", 400));
  }

  let payload;
  try {
    payload = sanitizeDraftPayload(req.body || {});
  } catch (err) {
    return next(err);
  }

  if (payload.groupId) {
    const group = await GroupModel.findById(payload.groupId);
    if (!group) return next(new AppError("Group not found", 404));
    payload.groupName = payload.groupName || group.groupName;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "bankAccountId")) {
    const bankAccountId = normalizeBankAccountId(payload.bankAccountId);
    if (bankAccountId) {
      const account = await resolveBorrowerBankAccount(
        req.user.profileId,
        bankAccountId,
      );
      applyDisbursementSnapshot(payload, account);
    } else {
      clearDisbursementSnapshot(payload);
    }
    delete payload.bankAccountId;
  }

  req.loanApplication.set({
    ...payload,
    groupId: payload.groupId || req.loanApplication.groupId || null,
    groupName: payload.groupName || req.loanApplication.groupName || null,
    draftStep:
      typeof payload.draftStep === "number"
        ? payload.draftStep
        : req.loanApplication.draftStep || 0,
    draftLastSavedAt: new Date(),
  });

  const updated = await req.loanApplication.save();

  return sendSuccess(res, {
    statusCode: 200,
    message: "Draft updated",
    data: { application: updated },
  });
});

export const deleteLoanDraft = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (req.loanApplication.status !== "draft") {
    return next(new AppError("Only draft applications can be deleted", 400));
  }

  await LoanApplicationModel.deleteOne({ _id: req.loanApplication._id });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Draft deleted",
  });
});

export const createLoanEditRequest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const application = req.loanApplication;
  if (String(application.userId) !== String(req.user.profileId)) {
    return next(new AppError("You do not have access to this loan", 403));
  }

  const disallowedStatuses = new Set([
    "draft",
    "disbursed",
    "completed",
    "defaulted",
    "cancelled",
  ]);
  if (disallowedStatuses.has(String(application.status))) {
    return next(
      new AppError(
        "Edits are only allowed before a loan is finalized or disbursed",
        400,
      ),
    );
  }

  const existingPending = await LoanApplicationEditRequestModel.findOne({
    loanApplicationId: application._id,
    status: "pending",
  }).lean();
  if (existingPending) {
    return next(
      new AppError(
        "You already have a pending edit request for this loan",
        400,
      ),
    );
  }

  const payload = sanitizeEditPayload(req.body || {});
  let requestedBankAccount = null;

  if (Object.prototype.hasOwnProperty.call(payload, "guarantors")) {
    const borrowerProfile = await ProfileModel.findById(
      req.user.profileId,
    ).select("email phone");
    await validateEditGuarantors({
      guarantors: payload.guarantors,
      borrowerProfileId: String(req.user.profileId),
      borrowerProfile,
      groupId: application.groupId,
    });
  }

  if (payload.bankAccountId) {
    requestedBankAccount = await resolveBorrowerBankAccount(
      req.user.profileId,
      payload.bankAccountId,
    );
  }

  const changes = buildEditChanges(application, payload);
  if (requestedBankAccount) {
    const bankChange = changes.find(
      (change) => change.field === "bankAccountId",
    );
    if (bankChange) {
      bankChange.from = summarizeBankSnapshot({
        bankName: application.disbursementBankName,
        accountNumber: application.disbursementAccountNumber,
        accountName: application.disbursementAccountName,
      });
      bankChange.to = summarizeBankSnapshot(requestedBankAccount);
    }
  }
  if (changes.length === 0) {
    return next(new AppError("No changes provided", 400));
  }

  const request = await LoanApplicationEditRequestModel.create({
    loanApplicationId: application._id,
    userId: req.user.profileId,
    status: "pending",
    requestedAt: new Date(),
    changes,
    payload,
  });

  createNotification({
    userId: req.user.profileId,
    title: "Loan edit request submitted",
    message:
      "Your loan edit request has been submitted for review by the admin.",
    type: "loan_edit_request",
    metadata: {
      loanId: application._id,
      requestId: request._id,
      status: request.status,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan edit request notification", err);
  });

  return sendSuccess(res, {
    statusCode: 201,
    message: "Edit request submitted",
    data: { editRequest: request },
  });
});

export const listMyLoanApplications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const apps = await LoanApplicationModel.find({
    userId: req.user.profileId,
  })
    .sort({ createdAt: -1 })
    .lean();
  const nextPaymentMap = await buildLoanNextPaymentMap(apps);
  const editRequests = await LoanApplicationEditRequestModel.find({
    loanApplicationId: { $in: apps.map((app) => app._id) },
  })
    .sort({ requestedAt: -1 })
    .lean();
  const latestEditMap = new Map();
  for (const request of editRequests) {
    const key = String(request.loanApplicationId);
    if (!latestEditMap.has(key)) {
      latestEditMap.set(key, request);
    }
  }

  const enriched = apps.map((app) => {
    const base = withRepaymentToDate(app);
    const next = nextPaymentMap.get(String(app._id));
    const editRequest = latestEditMap.get(String(app._id));
    return {
      ...base,
      nextPaymentDueDate: next?.dueDate ?? null,
      nextPaymentAmount: next?.amountDue ?? null,
      nextPaymentStatus: next?.status ?? null,
      latestEditRequest: editRequest
        ? {
            id: editRequest._id,
            status: editRequest.status,
            requestedAt: editRequest.requestedAt,
            reviewedAt: editRequest.reviewedAt,
            reviewNotes: editRequest.reviewNotes ?? null,
            changes: editRequest.changes ?? [],
            documents: Array.isArray(editRequest.payload?.documents)
              ? editRequest.payload.documents.map((doc) => ({
                  name: doc.name,
                  type: doc.type,
                  size: doc.size,
                  status: doc.status ?? "uploaded",
                  url: doc.url ?? null,
                }))
              : [],
          }
        : null,
    };
  });
  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    data: { applications: enriched },
  });
});

export const listLoanApplications = catchAsync(async (req, res) => {
  const filter = {};

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = req.query.status.trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }
  if (!filter.status) {
    filter.status = { $ne: "draft" };
  }

  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    filter.$or = [
      { loanCode: { $regex: search, $options: "i" } },
      { groupName: { $regex: search, $options: "i" } },
      { loanPurpose: { $regex: search, $options: "i" } },
    ];
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
  const skip = (page - 1) * limit;

  const [applications, total] = await Promise.all([
    LoanApplicationModel.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .lean(),
    LoanApplicationModel.countDocuments(filter),
  ]);
  const nextPaymentMap = await buildLoanNextPaymentMap(applications);
  const enriched = applications.map((app) => {
    const base = withRepaymentToDate(app);
    const next = nextPaymentMap.get(String(app._id));
    return {
      ...base,
      nextPaymentDueDate: next?.dueDate ?? null,
      nextPaymentAmount: next?.amountDue ?? null,
      nextPaymentStatus: next?.status ?? null,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    total,
    page,
    limit,
    data: { applications: enriched },
  });
});

export const getLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const [guarantors, schedule, editRequests] = await Promise.all([
    LoanGuarantorModel.find({
      loanApplicationId: req.loanApplication._id,
    }).sort({ createdAt: -1 }),
    LoanRepaymentScheduleItemModel.find({
      loanApplicationId: req.loanApplication._id,
    }).sort({
      installmentNumber: 1,
    }),
    LoanApplicationEditRequestModel.find({
      loanApplicationId: req.loanApplication._id,
    })
      .sort({ requestedAt: -1 })
      .lean(),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      application: withRepaymentToDate(req.loanApplication),
      guarantors,
      schedule,
      editRequests: editRequests.map((reqItem) => ({
        id: reqItem._id,
        status: reqItem.status,
        requestedAt: reqItem.requestedAt,
        reviewedAt: reqItem.reviewedAt,
        reviewNotes: reqItem.reviewNotes ?? null,
        changes: reqItem.changes ?? [],
        documents: Array.isArray(reqItem.payload?.documents)
          ? reqItem.payload.documents.map((doc) => ({
              name: doc.name,
              type: doc.type,
              size: doc.size,
              status: doc.status ?? "uploaded",
              url: doc.url ?? null,
            }))
          : [],
      })),
    },
  });
});

export const reviewLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const { status, reviewNotes, approvedAmount, approvedInterestRate } =
    req.body || {};

  const allowedStatuses = new Set(["under_review", "approved", "rejected"]);
  if (!status || !allowedStatuses.has(String(status))) {
    return next(new AppError("Invalid review status", 400));
  }

  req.loanApplication.status = status;
  req.loanApplication.reviewNotes =
    reviewNotes ?? req.loanApplication.reviewNotes;
  req.loanApplication.reviewedBy = req.user.profileId;
  req.loanApplication.reviewedAt = new Date();

  if (status === "approved") {
    if (typeof approvedAmount !== "undefined" && approvedAmount !== null) {
      req.loanApplication.approvedAmount = Number(approvedAmount);
    }
    if (
      typeof approvedInterestRate !== "undefined" &&
      approvedInterestRate !== null
    ) {
      if (
        !isInterestRateAllowed(
          req.loanApplication.loanType || "revolving",
          approvedInterestRate,
        )
      ) {
        return next(
          new AppError(
            "approvedInterestRate is not allowed for this loan type",
            400,
          ),
        );
      }
      req.loanApplication.approvedInterestRate = Number(approvedInterestRate);
    }
    if (!req.loanApplication.interestRateType) {
      const cfg = getLoanInterestConfig(
        req.loanApplication.loanType || "revolving",
      );
      req.loanApplication.interestRateType = cfg.rateType;
    }
    req.loanApplication.approvedAt = new Date();
  }

  await req.loanApplication.save();

  const statusLabel =
    status === "approved"
      ? "approved"
      : status === "rejected"
        ? "rejected"
        : "under review";

  createNotification({
    userId: req.loanApplication.userId,
    title: "Loan application update",
    message: `Your loan application ${req.loanApplication.loanCode} is ${statusLabel}.`,
    type: "loan_status",
    metadata: {
      loanId: req.loanApplication._id,
      loanCode: req.loanApplication.loanCode,
      status: req.loanApplication.status,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan status notification", err);
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const finalizeLoanDisbursementOtp = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (req.loanApplication.status !== "approved") {
    return next(
      new AppError(
        "Loan application must be approved before disbursement",
        400,
      ),
    );
  }
  if (
    req.loanApplication.payoutGateway &&
    String(req.loanApplication.payoutGateway).toLowerCase() !== "paystack"
  ) {
    return next(
      new AppError(
        "This payout is awaiting manual OTP authorization, not Paystack finalization.",
        400,
      ),
    );
  }

  const transferCodeRaw =
    req.body?.transferCode ||
    req.body?.transfer_code ||
    req.loanApplication.payoutTransferCode ||
    null;
  const transferCode = transferCodeRaw ? String(transferCodeRaw).trim() : "";
  if (!transferCode)
    return next(new AppError("transfer_code is required", 400));

  const otpRaw = req.body?.otp ? String(req.body.otp).trim() : "";
  if (!otpRaw) return next(new AppError("otp is required", 400));

  const repaymentStartDate = resolveLoanDisbursementRepaymentStartDate(
    req.loanApplication,
    req.body?.repaymentStartDate,
  );

  let transfer = null;
  try {
    const finalizeRes = await finalizeTransfer({
      transfer_code: transferCode,
      otp: otpRaw,
    });
    transfer = finalizeRes?.data;
    if (!transfer) {
      return next(new AppError("Invalid Paystack transfer response", 502));
    }
  } catch (error) {
    if (!isRecoverableFinalizeOtpStateError(error)) {
      throw error;
    }

    await verifyAndSyncLoanDisbursementTransfer({
      loan: req.loanApplication,
      repaymentStartDate,
      disbursedBy: req.user.profileId,
    });

    return sendSuccess(res, {
      statusCode: 200,
      data: { application: req.loanApplication },
      message:
        req.loanApplication.payoutStatus === "success"
          ? "Transfer was already completed and has now been synchronized."
          : "Transfer state refreshed. Review the latest payout status before retrying.",
    });
  }

  const { status } = syncLoanPayoutFromTransfer(req.loanApplication, transfer, {
    fallbackTransferCode: transferCode,
  });

  await persistLoanDisbursementOutcome({
    loan: req.loanApplication,
    transferStatus: status,
    repaymentStartDate,
    disbursedBy: req.user.profileId,
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const initiateManualLoanDisbursement = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));
    if (!req.loanApplication)
      return next(new AppError("Missing loan context", 500));

    if (req.loanApplication.status !== "approved") {
      return next(
        new AppError(
          "Loan application must be approved before disbursement",
          400,
        ),
      );
    }

    const repaymentStartDate = resolveLoanDisbursementRepaymentStartDate(
      req.loanApplication,
      req.body?.repaymentStartDate,
    );
    const paystackSwitch = await preparePaystackDisbursementSwitch({
      loan: req.loanApplication,
      repaymentStartDate,
      disbursedBy: req.user.profileId,
    });
    const { interestCfg } = await buildDisbursementContext(req.loanApplication);
    const rateType =
      req.loanApplication.interestRateType || interestCfg.rateType || "annual";

    const bankAccountId =
      req.body?.bankAccountId ||
      req.body?.bank_account_id ||
      req.loanApplication.disbursementBankAccountId ||
      null;
    if (bankAccountId) {
      const account = await selectDisbursementAccount(
        req.loanApplication,
        bankAccountId,
      );
      if (!account) {
        return next(new AppError("Bank account not found", 404));
      }
      applyDisbursementSnapshot(req.loanApplication, account);
    }

    const manualPayload = buildManualDisbursementPayload({
      loan: req.loanApplication,
      body: req.body || {},
      repaymentStartDate,
    });
    manualPayload.notes = appendOperationalNote(
      manualPayload.notes,
      paystackSwitch.note,
    );
    const actingUser = await UserModel.findById(getAuthenticatedUserId(req))
      .select("email phone")
      .lean();
    if (!actingUser) {
      return next(new AppError("Authorized user not found", 404));
    }

    const otp = generateOtp();
    const delivery = await sendManualDisbursementOtp({
      user: actingUser,
      otp,
    });

    req.loanApplication.repaymentStartDate = repaymentStartDate;
    req.loanApplication.interestRateType = rateType;
    req.loanApplication.payoutGateway = "manual";
    req.loanApplication.payoutStatus = "otp";
    req.loanApplication.payoutTransferCode = null;
    req.loanApplication.payoutReference =
      req.loanApplication.payoutReference &&
      String(req.loanApplication.payoutGateway || "").toLowerCase() ===
        "manual" &&
      String(req.loanApplication.payoutStatus || "").toLowerCase() === "otp"
        ? req.loanApplication.payoutReference
        : buildManualDisbursementReference(req.loanApplication);
    req.loanApplication.payoutOtpResentAt = new Date();
    req.loanApplication.manualDisbursement = {
      status: "pending_otp",
      method: manualPayload.method,
      amount: manualPayload.amount,
      externalReference: manualPayload.externalReference,
      occurredAt: manualPayload.occurredAt,
      repaymentStartDate,
      notes: manualPayload.notes,
      initiatedByUserId: getAuthenticatedUserId(req),
      initiatedBy: req.user.profileId,
      authorizedBy: null,
      initiatedAt: new Date(),
      completedAt: null,
      otpChannel: delivery.channel,
      otpRecipient: delivery.maskedRecipient,
      otpSentAt: new Date(),
    };
    req.loanApplication.manualDisbursementOtpHash = sha256(otp);
    req.loanApplication.manualDisbursementOtpExpiresAt = new Date(
      Date.now() + MANUAL_LOAN_DISBURSEMENT_OTP_TTL_MINUTES * 60 * 1000,
    );

    await req.loanApplication.save();

    return sendSuccess(res, {
      statusCode: 200,
      message: `Manual disbursement OTP sent to ${delivery.maskedRecipient}.`,
      data: { application: req.loanApplication },
    });
  },
);

export const resendLoanDisbursementOtp = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (req.loanApplication.status !== "approved") {
    return next(
      new AppError(
        "Loan application must be approved before disbursement",
        400,
      ),
    );
  }
  if (
    req.loanApplication.payoutGateway &&
    String(req.loanApplication.payoutGateway).toLowerCase() !== "paystack"
  ) {
    return next(
      new AppError(
        "This payout uses manual OTP authorization. Use the manual resend endpoint instead.",
        400,
      ),
    );
  }

  if (LOAN_OTP_RESEND_COOLDOWN_MS > 0 && req.loanApplication.payoutOtpResentAt) {
    const lastResentAt = new Date(
      req.loanApplication.payoutOtpResentAt,
    ).getTime();
    if (Number.isFinite(lastResentAt)) {
      const elapsedMs = Date.now() - lastResentAt;
      if (elapsedMs >= 0 && elapsedMs < LOAN_OTP_RESEND_COOLDOWN_MS) {
        const retryAfterSeconds = Math.ceil(
          (LOAN_OTP_RESEND_COOLDOWN_MS - elapsedMs) / 1000,
        );
        res.set("Retry-After", String(retryAfterSeconds));
        return next(
          new AppError(
            `Please wait ${retryAfterSeconds}s before resending OTP.`,
            429,
          ),
        );
      }
    }
  }

  const transferCodeRaw =
    req.body?.transferCode ||
    req.body?.transfer_code ||
    req.loanApplication.payoutTransferCode ||
    null;
  const transferCode = transferCodeRaw ? String(transferCodeRaw).trim() : "";
  if (!transferCode)
    return next(new AppError("transfer_code is required", 400));

  const reasonRaw = req.body?.reason ? String(req.body.reason).trim() : "";
  const normalizedReason = reasonRaw.toLowerCase();
  let reason = "transfer";
  if (normalizedReason === "disable_otp") {
    reason = "disable_otp";
  } else if (normalizedReason === "transfer") {
    reason = "transfer";
  } else if (normalizedReason === "resend_otp") {
    reason = "transfer";
  }

  await resendTransferOtp({
    transfer_code: transferCode,
    reason,
  });

  req.loanApplication.payoutTransferCode = transferCode;
  req.loanApplication.payoutStatus = "otp";
  req.loanApplication.payoutGateway = "paystack";
  req.loanApplication.payoutOtpResentAt = new Date();

  await req.loanApplication.save();

  if (LOAN_OTP_RESEND_COOLDOWN_MS > 0) {
    res.set(
      "Retry-After",
      String(Math.ceil(LOAN_OTP_RESEND_COOLDOWN_MS / 1000)),
    );
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const resendManualLoanDisbursementOtp = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));
    if (!req.loanApplication)
      return next(new AppError("Missing loan context", 500));

    if (req.loanApplication.status !== "approved") {
      return next(
        new AppError(
          "Loan application must be approved before disbursement",
          400,
        ),
      );
    }
    if (String(req.loanApplication.payoutGateway || "").toLowerCase() !== "manual") {
      return next(new AppError("No pending manual disbursement found", 400));
    }
    if (String(req.loanApplication.payoutStatus || "").toLowerCase() !== "otp") {
      return next(
        new AppError("Manual disbursement is not currently awaiting OTP", 400),
      );
    }

    const otpLoan = await loadLoanWithManualOtpState(req.loanApplication._id);
    if (!otpLoan) {
      return next(new AppError("Loan application not found", 404));
    }
    ensureManualDisbursementInitiator(otpLoan, req);

    if (LOAN_OTP_RESEND_COOLDOWN_MS > 0 && otpLoan.payoutOtpResentAt) {
      const lastResentAt = new Date(otpLoan.payoutOtpResentAt).getTime();
      if (Number.isFinite(lastResentAt)) {
        const elapsedMs = Date.now() - lastResentAt;
        if (elapsedMs >= 0 && elapsedMs < LOAN_OTP_RESEND_COOLDOWN_MS) {
          const retryAfterSeconds = Math.ceil(
            (LOAN_OTP_RESEND_COOLDOWN_MS - elapsedMs) / 1000,
          );
          res.set("Retry-After", String(retryAfterSeconds));
          return next(
            new AppError(
              `Please wait ${retryAfterSeconds}s before resending OTP.`,
              429,
            ),
          );
        }
      }
    }

    const actingUser = await UserModel.findById(getAuthenticatedUserId(req))
      .select("email phone")
      .lean();
    if (!actingUser) {
      return next(new AppError("Authorized user not found", 404));
    }

    const otp = generateOtp();
    const delivery = await sendManualDisbursementOtp({
      user: actingUser,
      otp,
    });

    otpLoan.manualDisbursementOtpHash = sha256(otp);
    otpLoan.manualDisbursementOtpExpiresAt = new Date(
      Date.now() + MANUAL_LOAN_DISBURSEMENT_OTP_TTL_MINUTES * 60 * 1000,
    );
    otpLoan.payoutOtpResentAt = new Date();
    otpLoan.manualDisbursement = {
      ...(otpLoan.manualDisbursement || {}),
      status: "pending_otp",
      otpChannel: delivery.channel,
      otpRecipient: delivery.maskedRecipient,
      otpSentAt: new Date(),
    };

    await otpLoan.save();

    if (LOAN_OTP_RESEND_COOLDOWN_MS > 0) {
      res.set(
        "Retry-After",
        String(Math.ceil(LOAN_OTP_RESEND_COOLDOWN_MS / 1000)),
      );
    }

    return sendSuccess(res, {
      statusCode: 200,
      message: `Manual disbursement OTP sent to ${delivery.maskedRecipient}.`,
      data: { application: otpLoan },
    });
  },
);

export const cancelManualLoanDisbursement = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));
    if (!req.loanApplication)
      return next(new AppError("Missing loan context", 500));

    if (req.loanApplication.status !== "approved") {
      return next(
        new AppError(
          "Loan application must be approved before disbursement",
          400,
        ),
      );
    }
    if (String(req.loanApplication.payoutGateway || "").toLowerCase() !== "manual") {
      return next(new AppError("No pending manual disbursement found", 400));
    }
    if (String(req.loanApplication.payoutStatus || "").toLowerCase() !== "otp") {
      return next(
        new AppError("Manual disbursement is not currently awaiting OTP", 400),
      );
    }

    clearPendingManualDisbursementState(req.loanApplication);
    await req.loanApplication.save();

    return sendSuccess(res, {
      statusCode: 200,
      message:
        "Pending manual disbursement authorization cancelled. You can switch back to Paystack now.",
      data: { application: req.loanApplication },
    });
  },
);

export const listLoanBorrowerBankAccounts = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const borrowerId = req.loanApplication.userId;
  if (!borrowerId) {
    return next(new AppError("Borrower profile not found", 400));
  }

  const accounts = await BankAccountModel.find({ userId: borrowerId }).sort({
    isPrimary: -1,
    createdAt: -1,
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: accounts.length,
    data: { accounts },
  });
});

export const finalizeManualLoanDisbursement = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));
    if (!req.loanApplication)
      return next(new AppError("Missing loan context", 500));

    if (req.loanApplication.status !== "approved") {
      return next(
        new AppError(
          "Loan application must be approved before disbursement",
          400,
        ),
      );
    }
    if (String(req.loanApplication.payoutGateway || "").toLowerCase() !== "manual") {
      return next(new AppError("No pending manual disbursement found", 400));
    }
    if (String(req.loanApplication.payoutStatus || "").toLowerCase() !== "otp") {
      return next(
        new AppError("Manual disbursement is not currently awaiting OTP", 400),
      );
    }

    const otpRaw = req.body?.otp ? String(req.body.otp).trim() : "";
    if (!otpRaw) return next(new AppError("otp is required", 400));

    const otpLoan = await loadLoanWithManualOtpState(req.loanApplication._id);
    if (!otpLoan) {
      return next(new AppError("Loan application not found", 404));
    }
    ensureManualDisbursementInitiator(otpLoan, req);

    if (
      !otpLoan.manualDisbursementOtpHash ||
      !otpLoan.manualDisbursementOtpExpiresAt
    ) {
      return next(
        new AppError(
          "Manual disbursement OTP has not been issued. Please request a new OTP.",
          400,
        ),
      );
    }
    if (otpLoan.manualDisbursementOtpExpiresAt.getTime() <= Date.now()) {
      clearManualDisbursementAuthorization(otpLoan);
      await otpLoan.save();
      return next(
        new AppError(
          "OTP has expired. Please request a new code to finalize this manual disbursement.",
          400,
        ),
      );
    }
    if (sha256(otpRaw) !== otpLoan.manualDisbursementOtpHash) {
      return next(new AppError("Invalid OTP", 400));
    }

    const manualDisbursement = otpLoan.manualDisbursement || {};
    const repaymentStartDate = resolveLoanDisbursementRepaymentStartDate(
      otpLoan,
      manualDisbursement.repaymentStartDate || req.body?.repaymentStartDate,
    );

    otpLoan.payoutGateway = "manual";
    otpLoan.payoutStatus = "success";
    otpLoan.payoutTransferCode = null;
    otpLoan.payoutReference =
      otpLoan.payoutReference || buildManualDisbursementReference(otpLoan);
    otpLoan.payoutOtpResentAt = null;
    otpLoan.manualDisbursement = {
      ...manualDisbursement,
      status: "completed",
      authorizedBy: req.user.profileId,
      completedAt: new Date(),
      repaymentStartDate,
      otpSentAt: manualDisbursement.otpSentAt || new Date(),
    };
    clearManualDisbursementAuthorization(otpLoan);

    await persistLoanDisbursementOutcome({
      loan: otpLoan,
      transferStatus: "success",
      repaymentStartDate,
      disbursedBy: req.user.profileId,
    });

    return sendSuccess(res, {
      statusCode: 200,
      message: "Manual loan disbursement finalized successfully.",
      data: { application: otpLoan },
    });
  },
);

export const verifyLoanDisbursementTransfer = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (req.loanApplication.status !== "approved") {
    return next(
      new AppError(
        "Loan application must be approved before disbursement",
        400,
      ),
    );
  }
  if (
    req.loanApplication.payoutGateway &&
    String(req.loanApplication.payoutGateway).toLowerCase() !== "paystack"
  ) {
    return next(
      new AppError(
        "Manual disbursement does not use Paystack transfer verification.",
        400,
      ),
    );
  }

  const referenceRaw = req.loanApplication.payoutReference;
  if (!referenceRaw) {
    return next(new AppError("payoutReference is required to verify transfer", 400));
  }

  const repaymentStartDate = resolveLoanDisbursementRepaymentStartDate(
    req.loanApplication,
    req.body?.repaymentStartDate,
  );

  await verifyAndSyncLoanDisbursementTransfer({
    loan: req.loanApplication,
    repaymentStartDate,
    disbursedBy: req.user.profileId,
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const disburseLoan = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (req.loanApplication.status === "disbursed") {
    return next(new AppError("Loan has already been disbursed", 400));
  }
  if (
    String(req.loanApplication.payoutGateway || "").toLowerCase() === "manual" &&
    String(req.loanApplication.payoutStatus || "").toLowerCase() === "otp"
  ) {
    clearPendingManualDisbursementState(req.loanApplication);
  }

  const { loanType, principal, rate, termMonths, interestCfg } =
    await buildDisbursementContext(req.loanApplication);

  const repaymentStartDate =
    parseDateOrNull(req.body?.repaymentStartDate) ||
    req.loanApplication.repaymentStartDate ||
    addMonths(new Date(), 1);

  const rateType =
    req.loanApplication.interestRateType || interestCfg.rateType || "annual";

  const { items, totalRepayable, monthlyPayment } = buildDisbursementSchedule({
    principal,
    rate,
    termMonths,
    rateType,
    repaymentStartDate,
    loanType,
  });

  const gateway = String(req.body?.gateway || "paystack").trim().toLowerCase();
  const requestedReference = req.body?.reference
    ? String(req.body.reference).trim()
    : null;

  if (gateway === "manual") {
    return next(
      new AppError(
        "Manual disbursement requires OTP authorization. Use the manual disbursement initiation endpoint.",
        400,
      ),
    );
  }

  if (gateway !== "paystack") {
    const reference = requestedReference || `LOAN-${randomId(8)}`;
    req.loanApplication.payoutReference = reference;
    req.loanApplication.payoutGateway = gateway;
    req.loanApplication.payoutStatus = "success";
    req.loanApplication.repaymentStartDate = repaymentStartDate;
    req.loanApplication.interestRateType = rateType;

    await applyLoanDisbursement({
      loan: req.loanApplication,
      items,
      repaymentStartDate,
      rateType,
      monthlyPayment,
      totalRepayable,
      principal,
      disbursedBy: req.user.profileId,
    });

    return sendSuccess(res, {
      statusCode: 200,
      data: { application: req.loanApplication },
    });
  }

  const bankAccountId =
    req.body?.bankAccountId ||
    req.body?.bank_account_id ||
    req.loanApplication.disbursementBankAccountId ||
    null;
  const account = await selectDisbursementAccount(
    req.loanApplication,
    bankAccountId,
  );
  if (!account) {
    return next(
      new AppError(
        "Borrower has no bank account on file. Please add one before disbursement.",
        400,
      ),
    );
  }

  const bankCode = await resolveBankCode({
    bankCode: account.bankCode,
    bankName: account.bankName,
  });
  if (!bankCode) {
    return next(
      new AppError(
        "Bank code is required for Paystack transfers. Please update the bank account.",
        400,
      ),
    );
  }
  if (!account.bankCode) {
    account.bankCode = bankCode;
    await account.save();
  }

  req.loanApplication.disbursementBankAccountId = account._id;
  req.loanApplication.disbursementBankName = account.bankName;
  req.loanApplication.disbursementBankCode = bankCode;
  req.loanApplication.disbursementAccountNumber = account.accountNumber;
  req.loanApplication.disbursementAccountName = account.accountName;
  req.loanApplication.repaymentStartDate = repaymentStartDate;
  req.loanApplication.interestRateType = rateType;

  const existingReference =
    String(req.loanApplication.payoutGateway || "").toLowerCase() ===
      "paystack" &&
    req.loanApplication.payoutReference &&
    !["failed", "reversed"].includes(
      String(req.loanApplication.payoutStatus || "").toLowerCase(),
    )
      ? req.loanApplication.payoutReference
      : null;

  if (existingReference) {
    await verifyAndSyncLoanDisbursementTransfer({
      loan: req.loanApplication,
      repaymentStartDate,
      disbursedBy: req.user.profileId,
    });

    return sendSuccess(res, {
      statusCode: 200,
      data: { application: req.loanApplication },
    });
  }

  const reference = sanitizeTransferReference(requestedReference);

  const recipientRes = await createTransferRecipient({
    type: "nuban",
    name: account.accountName,
    account_number: account.accountNumber,
    bank_code: bankCode,
    currency: "NGN",
  });
  const recipientCode = recipientRes?.data?.recipient_code;
  if (!recipientCode) {
    return next(new AppError("Unable to create transfer recipient", 502));
  }

  const transferRes = await initiateTransfer({
    source: "balance",
    amount: Math.round(Number(principal || 0) * 100),
    recipient: recipientCode,
    reference,
    reason: `Loan ${req.loanApplication._id}`,
  });
  const transfer = transferRes?.data;
  if (!transfer) {
    return next(new AppError("Invalid Paystack transfer response", 502));
  }

  const { status } = syncLoanPayoutFromTransfer(req.loanApplication, transfer, {
    fallbackReference: reference,
  });

  if (status === "success") {
    await applyLoanDisbursement({
      loan: req.loanApplication,
      items,
      repaymentStartDate,
      rateType,
      monthlyPayment,
      totalRepayable,
      principal,
      disbursedBy: req.user.profileId,
    });
  } else {
    await req.loanApplication.save();
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const listLoanSchedule = catchAsync(async (req, res, next) => {
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const schedule = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: req.loanApplication._id,
  }).sort({ installmentNumber: 1 });

  return sendSuccess(res, {
    statusCode: 200,
    results: schedule.length,
    data: { schedule },
  });
});

export const recordLoanRepayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (!["disbursed", "defaulted"].includes(req.loanApplication.status)) {
    return next(new AppError("Loan is not active", 400));
  }

  const amount = Number(req.body?.amount);
  const reference = String(req.body?.reference || "").trim();
  if (!amount || amount <= 0)
    return next(new AppError("amount is required", 400));
  if (!reference) return next(new AppError("reference is required", 400));

  const result = await applyLoanRepayment({
    application: req.loanApplication,
    amount,
    reference,
    channel: req.body?.channel || null,
    description: `Loan repayment for ${req.loanApplication.loanCode}`,
    gateway: req.body?.gateway || "manual",
    metadata: {
      recordedBy: req.user.profileId,
      manual: req.body?.gateway !== "paystack",
    },
    paidAt: new Date(),
  });
  const firstAffectedScheduleItem =
    result.allocations.length > 0
      ? result.scheduleItems.find(
          (item) =>
            String(item._id) === String(result.allocations[0].scheduleItemId),
        ) || null
      : null;

  createNotification({
    userId: req.loanApplication.userId,
    title: "Payment received",
    message: `We received your loan repayment for ${req.loanApplication.loanCode}.`,
    type: "payment_received",
    metadata: {
      loanId: req.loanApplication._id,
      loanCode: req.loanApplication.loanCode,
      amount,
      reference,
      allocations: result.allocations.map((allocation) => ({
        installmentNumber: allocation.installmentNumber,
        appliedAmount: allocation.appliedAmount,
      })),
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create repayment notification", err);
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      transaction: result.transaction,
      scheduleItem: firstAffectedScheduleItem,
      allocations: result.allocations,
      application: result.application,
    },
  });
});

