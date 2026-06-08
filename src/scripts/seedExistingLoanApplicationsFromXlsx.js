import dotenv from "dotenv";

dotenv.config();

import ExcelJS from "exceljs";
import mongoose from "mongoose";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { connectMongo } from "../db.js";
import {
  FormPaymentModel,
  GroupMembershipModel,
  GroupModel,
  LoanApplicationModel,
  LoanGuarantorModel,
  LoanRepaymentScheduleItemModel,
  ProfileModel,
  TransactionModel,
  UserModel,
} from "../models/index.js";
import {
  getLoanFacility,
  resolveInterestRate,
} from "../utils/loanPolicy.js";
import {
  resolveLoanFormPaymentConfig,
} from "../services/formPaymentService.js";
import { syncLoanRepaymentState } from "../services/loanRepaymentService.js";

const CONFIRMATION_TOKEN = "SEED_EXISTING_LOANS_2026";
const DEFAULT_YEAR = 2026;
const DEFAULT_TERM_MONTHS = 12;
const DEFAULT_DISBURSEMENT_METHOD = "bank_transfer";
const DEFAULT_BATCH_SIZE = 50;
const LOAN_CODE_PREFIX = "CRC-SEED-2026";
const SOURCE_FILE_LABEL = "CRC_Existing_Loan_Data.xlsx";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_INPUT = path.resolve(
  SCRIPT_DIR,
  "../seed-data/loan/CRC_Existing_Loan_Data.xlsx",
);

const MONTH_ALIASES = new Map([
  ["jan", 1],
  ["january", 1],
  ["feb", 2],
  ["february", 2],
  ["mar", 3],
  ["march", 3],
  ["apr", 4],
  ["april", 4],
  ["may", 5],
  ["jun", 6],
  ["june", 6],
  ["jul", 7],
  ["july", 7],
  ["aug", 8],
  ["august", 8],
  ["sep", 9],
  ["sept", 9],
  ["september", 9],
  ["oct", 10],
  ["october", 10],
  ["nov", 11],
  ["november", 11],
  ["dec", 12],
  ["december", 12],
]);

const LOAN_TYPE_ALIASES = new Map([
  ["revolving", "revolving"],
  ["revolving loan", "revolving"],
  ["special", "special"],
  ["special loan", "special"],
  ["bridging", "bridging"],
  ["bridging loan", "bridging"],
  ["soft", "soft"],
  ["soft loan", "soft"],
]);

const ManualDisbursementMethods = new Set([
  "cash",
  "bank_transfer",
  "bank_settlement",
  "cheque",
  "pos",
  "other",
]);

function printHelp() {
  // eslint-disable-next-line no-console
  console.log(`
Seed existing 2026 CRC loan applications from an XLSX tracker.

Usage:
  npm run seed:existing-loans -- [options]

Options:
  --confirm ${CONFIRMATION_TOKEN}   Write records. Without this, the script is a dry run.
  --dry-run                         Force dry-run mode.
  --input <path>                    XLSX path. Defaults to ${DEFAULT_INPUT}
  --sheet <name>                    Worksheet name. Defaults to the first worksheet.
  --year <yyyy>                     Loan year. Defaults to ${DEFAULT_YEAR}.
  --as-of <date>                    Date used to materialize due schedules. Defaults to now.
  --limit <n>                       Process only the first n parsed rows.
  --batch-size <n>                  Progress batch size. Defaults to ${DEFAULT_BATCH_SIZE}.
  --disbursement-method <method>    Manual method: cash, bank_transfer, bank_settlement, cheque, pos, other.
  --force-reset-repayment-state     Allow schedule/loan balance refresh even if repayments already exist.
  --help                            Show this help.
`);
}

function parseArgs(args) {
  const output = {};
  for (let index = 0; index < args.length; index += 1) {
    const current = args[index];
    if (!current.startsWith("--")) continue;
    const key = current.replace(/^--/, "");
    const next = args[index + 1];
    if (!next || next.startsWith("--")) {
      output[key] = true;
    } else {
      output[key] = next;
      index += 1;
    }
  }
  return output;
}

function parsePositiveInteger(value, label, fallback = null) {
  if (value === undefined || value === null || value === true || value === "") {
    return fallback;
  }
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`Invalid --${label} value`);
  }
  return parsed;
}

function parseDateArg(value, label, fallback = null) {
  if (value === undefined || value === null || value === true || value === "") {
    return fallback;
  }
  const raw = String(value).trim();
  const parsed = /^\d{4}-\d{2}-\d{2}$/.test(raw)
    ? new Date(`${raw}T00:00:00.000Z`)
    : new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid --${label} date`);
  }
  return parsed;
}

function resolveDisbursementMethod(value) {
  const method = String(value || DEFAULT_DISBURSEMENT_METHOD)
    .trim()
    .toLowerCase();
  if (!ManualDisbursementMethods.has(method)) {
    throw new Error(
      `Invalid --disbursement-method. Use one of: ${Array.from(
        ManualDisbursementMethods,
      ).join(", ")}.`,
    );
  }
  return method;
}

function normalizeWhitespace(value) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHeader(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .replace(/[₦#,()]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractCellValue(cell) {
  const value = cell?.value;
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value;
  if (typeof value !== "object") return value;
  if (Object.prototype.hasOwnProperty.call(value, "result")) {
    return value.result;
  }
  if (Array.isArray(value.richText)) {
    return value.richText.map((part) => part.text || "").join("");
  }
  if (typeof value.text === "string") return value.text;
  if (typeof value.hyperlink === "string" && typeof value.text === "string") {
    return value.text;
  }
  return value;
}

function getCellText(row, columnNumber) {
  return normalizeWhitespace(extractCellValue(row.getCell(columnNumber)));
}

function findColumn(headers, matcher, label) {
  const match = headers.find((header) => matcher(header.normalized));
  if (!match) throw new Error(`Could not find required "${label}" column`);
  return match.index;
}

function tryFindColumn(headers, matcher) {
  return headers.find((header) => matcher(header.normalized))?.index ?? null;
}

function normalizeSerial(value) {
  return normalizeWhitespace(value).toUpperCase();
}

function parseMemberSerialParts(value) {
  const match = normalizeSerial(value).match(/^CRC\/G(\d+)\/0*(\d+)$/);
  if (!match) return null;
  return {
    groupNumber: Number(match[1]),
    memberNumber: Number(match[2]),
  };
}

function normalizeLoanType(value) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  const key = normalized.replace(/\s+/g, " ");
  const withoutLoan = key.replace(/\s+loan$/, "");
  return LOAN_TYPE_ALIASES.get(key) || LOAN_TYPE_ALIASES.get(withoutLoan) || null;
}

function parseMonthNumber(value) {
  if (value instanceof Date) return value.getUTCMonth() + 1;
  const text = normalizeWhitespace(value).toLowerCase();
  if (!text) return null;
  if (MONTH_ALIASES.has(text)) return MONTH_ALIASES.get(text);
  const numeric = Number.parseInt(text, 10);
  if (Number.isInteger(numeric) && numeric >= 1 && numeric <= 12) {
    return numeric;
  }
  return null;
}

function parseMoney(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") {
    return Number.isFinite(value) ? roundCurrency(value) : null;
  }
  const cleaned = String(value).replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? roundCurrency(parsed) : null;
}

function roundCurrency(value) {
  return Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;
}

function addMonths(date, months) {
  const next = new Date(date);
  const day = next.getUTCDate();
  next.setUTCMonth(next.getUTCMonth() + months);
  if (next.getUTCDate() < day) next.setUTCDate(0);
  return next;
}

function buildMonthDate(year, monthNumber) {
  return new Date(Date.UTC(year, monthNumber - 1, 1, 9, 0, 0, 0));
}

function slug(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function buildImportKey({ serial, monthNumber, loanType, principal }) {
  const monthPart = String(monthNumber).padStart(2, "0");
  return [
    LOAN_CODE_PREFIX,
    slug(serial),
    `M${monthPart}`,
    slug(loanType),
    String(Math.round(Number(principal || 0) * 100)),
  ].join("-");
}

function hashString(value) {
  let hash = 2166136261;
  const text = String(value || "");
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function deterministicSortBySeed(items, seed) {
  return [...items].sort((left, right) => {
    const leftScore = hashString(`${seed}:${left.profileId}`);
    const rightScore = hashString(`${seed}:${right.profileId}`);
    if (leftScore !== rightScore) return leftScore - rightScore;
    return String(left.profileId).localeCompare(String(right.profileId));
  });
}

function buildRepaymentSchedule({
  principal,
  ratePct,
  rateType,
  months,
  startDate,
}) {
  const loanPrincipal = Math.max(0, roundCurrency(principal));
  const termMonths = Math.max(1, Math.floor(Number(months) || 0));
  const rate = Math.max(0, Number(ratePct) || 0);
  const normalizedType = String(rateType || "annual").trim().toLowerCase();
  const monthlyRate =
    normalizedType === "monthly"
      ? rate / 100
      : normalizedType === "total"
        ? rate / 100 / termMonths
        : rate / 100 / 12;

  const items = [];
  let remainingPrincipal = loanPrincipal;

  for (let index = 1; index <= termMonths; index += 1) {
    const cyclesRemaining = Math.max(1, termMonths - index + 1);
    const openingPrincipalBalance = roundCurrency(remainingPrincipal);
    const principalAmount =
      index === termMonths
        ? openingPrincipalBalance
        : roundCurrency(openingPrincipalBalance / cyclesRemaining);
    const interestAmount = roundCurrency(openingPrincipalBalance * monthlyRate);
    const totalAmount = roundCurrency(principalAmount + interestAmount);
    const dueDate = addMonths(startDate, index - 1);

    items.push({
      installmentNumber: index,
      dueDate,
      openingPrincipalBalance,
      principalAmount,
      interestAmount,
      totalAmount,
      paidPrincipalAmount: 0,
      paidInterestAmount: 0,
      paidAmount: 0,
      isProjected: true,
      status: "upcoming",
      paidAt: null,
      transactionId: null,
      reference: null,
    });

    remainingPrincipal = Math.max(
      0,
      roundCurrency(remainingPrincipal - principalAmount),
    );
  }

  const projectedInterest = items.reduce(
    (sum, item) => sum + Number(item.interestAmount || 0),
    0,
  );

  return {
    items,
    totalRepayable: roundCurrency(loanPrincipal + projectedInterest),
    monthlyPayment: roundCurrency(items[0]?.totalAmount ?? loanPrincipal),
  };
}

function buildHeaders(worksheet) {
  const row1 = worksheet.getRow(1);
  const row2 = worksheet.getRow(2);
  const headers = [];

  for (let index = 1; index <= worksheet.columnCount; index += 1) {
    const top = extractCellValue(row1.getCell(index));
    const bottom = extractCellValue(row2.getCell(index));
    const combined = normalizeHeader(`${top || ""} ${bottom || ""}`);
    headers.push({ index, normalized: combined });
  }

  return headers;
}

function parseWorkbookRows({ inputPath, sheetName, year }) {
  return new ExcelJS.Workbook().xlsx.readFile(inputPath).then((workbook) => {
    const worksheet = sheetName
      ? workbook.getWorksheet(sheetName)
      : workbook.worksheets[0];
    if (!worksheet) {
      throw new Error(
        sheetName
          ? `Worksheet "${sheetName}" was not found`
          : "Workbook does not contain any worksheets",
      );
    }

    const headers = buildHeaders(worksheet);
    const serialColumn = findColumn(
      headers,
      (header) => /\bserial\b/.test(header),
      "Serial",
    );
    const nameColumn = findColumn(
      headers,
      (header) => /\bname\b/.test(header),
      "Name",
    );
    const principalColumn = findColumn(
      headers,
      (header) =>
        !/form/.test(header) &&
        (/principal/.test(header) || /\bloan amount\b/.test(header)),
      "Principal Loan Amount",
    );
    const loanMonthColumn = findColumn(
      headers,
      (header) => /loan.*month/.test(header) || header === "month",
      "Loan Month",
    );
    const loanTypeColumn = findColumn(
      headers,
      (header) => /loan.*type/.test(header) || header === "type",
      "Loan Type",
    );
    const loanFormPaymentColumn = tryFindColumn(
      headers,
      (header) => /loan.*form.*payment/.test(header) || /form.*payment/.test(header),
    );

    const rows = [];
    const warnings = [];

    for (let rowNumber = 3; rowNumber <= worksheet.rowCount; rowNumber += 1) {
      const row = worksheet.getRow(rowNumber);
      const serial = normalizeSerial(getCellText(row, serialColumn));
      const name = getCellText(row, nameColumn);
      const principal = parseMoney(extractCellValue(row.getCell(principalColumn)));
      const monthRaw = extractCellValue(row.getCell(loanMonthColumn));
      const loanTypeRaw = getCellText(row, loanTypeColumn);
      const formPaymentAmount = loanFormPaymentColumn
        ? parseMoney(extractCellValue(row.getCell(loanFormPaymentColumn)))
        : null;

      if (!serial && !name && !principal) continue;
      if (!serial && !name) continue;

      const monthNumber = parseMonthNumber(monthRaw);
      const loanType = normalizeLoanType(loanTypeRaw);

      if (!serial || !principal || principal <= 0 || !monthNumber || !loanType) {
        warnings.push({
          rowNumber,
          serial: serial || null,
          name: name || null,
          message: "Row skipped because serial, principal, month, or loan type is invalid.",
        });
        continue;
      }

      const disbursedAt = buildMonthDate(year, monthNumber);
      const repaymentStartDate = addMonths(disbursedAt, 1);
      const importKey = buildImportKey({
        serial,
        monthNumber,
        loanType,
        principal,
      });

      rows.push({
        rowNumber,
        serial,
        name,
        principal,
        monthRaw: normalizeWhitespace(monthRaw),
        monthNumber,
        loanType,
        loanTypeRaw,
        formPaymentAmount,
        importKey,
        loanCode: importKey,
        disbursedAt,
        repaymentStartDate,
      });
    }

    return {
      worksheetName: worksheet.name,
      rows,
      warnings,
      hasLoanFormPaymentColumn: Boolean(loanFormPaymentColumn),
    };
  });
}

async function getNextLoanNumberSeed() {
  const last = await LoanApplicationModel.findOne({
    loanNumber: { $ne: null },
  })
    .sort({ loanNumber: -1 })
    .select("loanNumber")
    .lean();
  return Number(last?.loanNumber ?? 0) + 1;
}

async function loadContext(rows) {
  const serials = Array.from(new Set(rows.map((row) => row.serial)));
  let memberships = await GroupMembershipModel.find({
    memberSerial: { $in: serials },
  }).lean();
  const membershipBySerial = new Map(
    memberships.map((membership) => [
      normalizeSerial(membership.memberSerial),
      membership,
    ]),
  );

  const missingSerialParts = serials
    .filter((serial) => !membershipBySerial.has(serial))
    .map((serial) => ({ serial, parts: parseMemberSerialParts(serial) }))
    .filter((entry) => entry.parts);

  if (missingSerialParts.length > 0) {
    const groupNumbers = Array.from(
      new Set(missingSerialParts.map((entry) => entry.parts.groupNumber)),
    );
    const fallbackGroups = await GroupModel.find({
      groupNumber: { $in: groupNumbers },
    })
      .select("_id groupNumber groupName")
      .lean();
    const groupByNumber = new Map(
      fallbackGroups.map((group) => [Number(group.groupNumber), group]),
    );
    const fallbackFilters = missingSerialParts
      .map((entry) => {
        const group = groupByNumber.get(entry.parts.groupNumber);
        if (!group?._id) return null;
        return {
          serial: entry.serial,
          filter: {
            groupId: group._id,
            memberNumber: entry.parts.memberNumber,
          },
        };
      })
      .filter(Boolean);

    if (fallbackFilters.length > 0) {
      const fallbackMemberships = await GroupMembershipModel.find({
        $or: fallbackFilters.map((entry) => entry.filter),
      }).lean();
      memberships = [...memberships, ...fallbackMemberships];

      for (const entry of fallbackFilters) {
        const found = fallbackMemberships.find(
          (membership) =>
            String(membership.groupId) === String(entry.filter.groupId) &&
            Number(membership.memberNumber) === Number(entry.filter.memberNumber),
        );
        if (found && !membershipBySerial.has(entry.serial)) {
          membershipBySerial.set(entry.serial, found);
        }
      }
    }
  }

  const profileIds = Array.from(
    new Set(memberships.map((membership) => String(membership.userId)).filter(Boolean)),
  );
  const groupIds = Array.from(
    new Set(memberships.map((membership) => String(membership.groupId)).filter(Boolean)),
  );

  const [profiles, groups, users, activeMemberships] = await Promise.all([
    profileIds.length
      ? ProfileModel.find({ _id: { $in: profileIds } }).lean()
      : [],
    groupIds.length ? GroupModel.find({ _id: { $in: groupIds } }).lean() : [],
    profileIds.length
      ? UserModel.find({ profileId: { $in: profileIds } })
          .select("_id profileId email phone")
          .lean()
      : [],
    GroupMembershipModel.find({
      status: "active",
      userId: { $ne: null },
    })
      .select("_id userId groupId joinedAt memberSerial memberNumber totalContributed")
      .lean(),
  ]);

  const activeProfileIds = Array.from(
    new Set(activeMemberships.map((membership) => String(membership.userId))),
  );
  const activeProfiles = activeProfileIds.length
    ? await ProfileModel.find({
        _id: { $in: activeProfileIds },
        membershipStatus: "active",
      }).lean()
    : [];

  const profileById = new Map(profiles.map((profile) => [String(profile._id), profile]));
  for (const profile of activeProfiles) {
    profileById.set(String(profile._id), profile);
  }

  const groupById = new Map(groups.map((group) => [String(group._id), group]));
  const userByProfileId = new Map(
    users.map((user) => [String(user.profileId), user]),
  );

  const activeMembershipByProfileAndGroup = new Map();
  const activeMembershipsByGroupId = new Map();
  for (const membership of activeMemberships) {
    const profileId = String(membership.userId);
    const groupId = String(membership.groupId);
    activeMembershipByProfileAndGroup.set(`${profileId}:${groupId}`, membership);
    if (!activeMembershipsByGroupId.has(groupId)) {
      activeMembershipsByGroupId.set(groupId, []);
    }
    activeMembershipsByGroupId.get(groupId).push(membership);
  }

  const candidateProfiles = activeProfiles.map((profile) => ({
    profileId: String(profile._id),
    profile,
  }));

  return {
    membershipBySerial,
    profileById,
    groupById,
    userByProfileId,
    activeMembershipByProfileAndGroup,
    activeMembershipsByGroupId,
    candidateProfiles,
  };
}

function buildGuarantorCandidates({
  applicantProfileId,
  groupId,
  context,
}) {
  const groupMemberships = context.activeMembershipsByGroupId.get(String(groupId)) || [];
  const groupCandidates = groupMemberships
    .map((membership) => {
      const profileId = String(membership.userId);
      const profile = context.profileById.get(profileId);
      if (!profile || profileId === String(applicantProfileId)) return null;
      return { profileId, profile, membership };
    })
    .filter(Boolean);

  if (groupCandidates.length > 0) return groupCandidates;

  return context.candidateProfiles
    .filter((candidate) => candidate.profileId !== String(applicantProfileId))
    .map((candidate) => ({ ...candidate, membership: null }));
}

async function chooseGuarantor({
  loan,
  row,
  applicantProfileId,
  groupId,
  context,
  existingSeedContext,
  usedGuarantorIds,
}) {
  if (loan?._id) {
    const existing = existingSeedContext.guarantorByLoanId.get(String(loan._id));
    if (existing?.guarantorUserId) {
      usedGuarantorIds.add(String(existing.guarantorUserId));
      const profile = context.profileById.get(String(existing.guarantorUserId));
      const membership =
        context.activeMembershipByProfileAndGroup.get(
          `${String(existing.guarantorUserId)}:${String(groupId)}`,
        ) || null;
      return profile
        ? {
            profileId: String(existing.guarantorUserId),
            profile,
            membership,
          }
        : null;
    }
  }

  const candidates = deterministicSortBySeed(
    buildGuarantorCandidates({
      applicantProfileId,
      groupId,
      context,
    }),
    row.importKey,
  );

  const fresh =
    candidates.find((candidate) => !usedGuarantorIds.has(candidate.profileId)) ||
    candidates[0] ||
    null;

  if (fresh) usedGuarantorIds.add(fresh.profileId);
  return fresh;
}

function buildGuarantorSnapshot(candidate, disbursedAt) {
  const profile = candidate?.profile || {};
  const membership = candidate?.membership || {};
  return {
    type: "member",
    profileId: candidate.profileId,
    name: profile.fullName || profile.email || "CRC Member",
    email: profile.email || "",
    phone: profile.phone || "",
    relationship: "CRC member",
    occupation: profile.occupation || "",
    address: profile.address || "",
    memberSince: membership.joinedAt
      ? new Date(membership.joinedAt).toISOString().slice(0, 10)
      : "",
    savingsBalance:
      membership.totalContributed === null ||
      membership.totalContributed === undefined
        ? null
        : Number(membership.totalContributed),
    liabilityPercentage: 100,
    signature: {
      method: "text",
      text: profile.fullName || "Accepted",
      font: "system",
      imageUrl: null,
      imagePublicId: null,
      signedAt: disbursedAt,
    },
  };
}

function buildLoanPayload({
  row,
  membership,
  profile,
  group,
  guarantorSnapshot,
  loanNumber,
  interest,
  schedule,
  disbursementMethod,
}) {
  const facility = getLoanFacility(row.loanType);
  const loanLabel = facility?.label || `${row.loanType} loan`;
  const createdAt = row.disbursedAt;

  return {
    userId: membership.userId,
    groupId: membership.groupId || null,
    groupName: group?.groupName || null,
    loanNumber,
    loanCode: row.loanCode,
    loanType: row.loanType,
    loanAmount: row.principal,
    loanPurpose: `${loanLabel} - historical 2026 import`,
    purposeDescription: [
      `Seeded from ${SOURCE_FILE_LABEL}.`,
      `Workbook row: ${row.rowNumber}.`,
      `Member serial: ${row.serial}.`,
      `Workbook name: ${row.name || profile?.fullName || "N/A"}.`,
    ].join(" "),
    repaymentPeriod: DEFAULT_TERM_MONTHS,
    interestRate: interest.rate,
    interestRateType: interest.rateType,
    monthlyIncome: null,
    documents: [],
    guarantors: [guarantorSnapshot],
    status: "disbursed",
    draftStep: 0,
    draftLastSavedAt: null,
    approvedAmount: row.principal,
    approvedInterestRate: interest.rate,
    approvedAt: createdAt,
    disbursementBankAccountId: null,
    disbursementBankName: null,
    disbursementBankCode: null,
    disbursementAccountNumber: null,
    disbursementAccountName: null,
    disbursedAt: row.disbursedAt,
    disbursedBy: null,
    repaymentStartDate: row.repaymentStartDate,
    nextInterestAccrualDate: row.repaymentStartDate,
    monthlyPayment: schedule.monthlyPayment,
    totalRepayable: row.principal,
    remainingBalance: row.principal,
    principalOutstanding: row.principal,
    accruedInterestBalance: 0,
    totalPrincipalPaid: 0,
    totalInterestPaid: 0,
    totalInterestAccrued: 0,
    payoutReference: `loan-disb-seed-${row.loanCode.toLowerCase()}`,
    payoutGateway: "manual",
    payoutTransferCode: null,
    payoutStatus: "success",
    payoutOtpResentAt: null,
    manualDisbursement: {
      status: "completed",
      method: disbursementMethod,
      amount: row.principal,
      externalReference: `manual-seed-${row.loanCode.toLowerCase()}`,
      occurredAt: row.disbursedAt,
      repaymentStartDate: row.repaymentStartDate,
      notes: `Historical manual loan disbursement seeded from ${SOURCE_FILE_LABEL} row ${row.rowNumber}.`,
      initiatedByUserId: null,
      initiatedBy: null,
      authorizedBy: null,
      initiatedAt: row.disbursedAt,
      completedAt: row.disbursedAt,
      otpChannel: null,
      otpRecipient: null,
      otpBackupChannels: [],
      otpSentAt: null,
    },
    reviewNotes: `Historical 2026 loan imported from ${SOURCE_FILE_LABEL} row ${row.rowNumber}. Import key: ${row.importKey}.`,
    reviewedBy: null,
    reviewedAt: createdAt,
  };
}

async function loadExistingSeedContext(rows) {
  const loanCodes = Array.from(new Set(rows.map((row) => row.loanCode)));
  const loans = loanCodes.length
    ? await LoanApplicationModel.find({ loanCode: { $in: loanCodes } })
    : [];
  const loanByCode = new Map(
    loans.map((loan) => [String(loan.loanCode), loan]),
  );
  const loanIds = loans.map((loan) => loan._id);

  const [guarantors, repaymentLoanIds, paidScheduleLoanIds] = loanIds.length
    ? await Promise.all([
        LoanGuarantorModel.find({
          loanApplicationId: { $in: loanIds },
        })
          .sort({ createdAt: 1, _id: 1 })
          .lean(),
        TransactionModel.distinct("loanId", {
          loanId: { $in: loanIds },
          type: "loan_repayment",
        }),
        LoanRepaymentScheduleItemModel.distinct("loanApplicationId", {
          loanApplicationId: { $in: loanIds },
          $or: [
            { paidAmount: { $gt: 0 } },
            { paidPrincipalAmount: { $gt: 0 } },
            { paidInterestAmount: { $gt: 0 } },
            { transactionId: { $ne: null } },
            { status: "paid" },
          ],
        }),
      ])
    : [[], [], []];

  const guarantorByLoanId = new Map();
  for (const guarantor of guarantors) {
    const loanId = String(guarantor.loanApplicationId);
    if (!guarantorByLoanId.has(loanId)) {
      guarantorByLoanId.set(loanId, guarantor);
    }
  }

  const repaymentActivityLoanIds = new Set(
    [...repaymentLoanIds, ...paidScheduleLoanIds]
      .filter(Boolean)
      .map((value) => String(value)),
  );

  return {
    loanByCode,
    guarantorByLoanId,
    repaymentActivityLoanIds,
  };
}

async function persistLoanTimestamps(loanId, createdAt) {
  await LoanApplicationModel.collection.updateOne(
    { _id: loanId },
    {
      $set: {
        createdAt,
      },
    },
  );
}

async function upsertLoan({
  existingLoan,
  hasExistingRepaymentActivity,
  row,
  membership,
  profile,
  group,
  guarantorSnapshot,
  interest,
  schedule,
  disbursementMethod,
  nextLoanNumber,
  dryRun,
  stats,
}) {
  const existing = existingLoan || null;
  const repaymentActivity = Boolean(hasExistingRepaymentActivity);

  if (repaymentActivity && !stats.forceResetRepaymentState) {
    stats.skippedRepaymentActivity += 1;
    stats.warnings.push({
      rowNumber: row.rowNumber,
      loanCode: row.loanCode,
      message:
        "Skipped because the matching loan already has repayment activity. Re-run with --force-reset-repayment-state only if this is intentional.",
    });
    return { loan: existing, skipped: true, created: false };
  }

  const loanNumber = existing?.loanNumber ?? nextLoanNumber.value;
  if (!existing?.loanNumber) nextLoanNumber.value += 1;

  const payload = buildLoanPayload({
    row,
    membership,
    profile,
    group,
    guarantorSnapshot,
    loanNumber,
    interest,
    schedule,
    disbursementMethod,
  });

  if (dryRun) {
    if (existing) stats.wouldUpdateLoans += 1;
    else stats.wouldCreateLoans += 1;
    return {
      loan: existing || { _id: null, ...payload },
      skipped: false,
      created: !existing,
    };
  }

  let loan;
  if (existing) {
    existing.set(payload);
    loan = await existing.save({ validateBeforeSave: true });
    stats.updatedLoans += 1;
  } else {
    loan = await LoanApplicationModel.create(payload);
    stats.createdLoans += 1;
  }

  await persistLoanTimestamps(loan._id, row.disbursedAt);
  return { loan, skipped: false, created: !existing };
}

async function upsertGuarantor({ loan, guarantorSnapshot, row, dryRun, stats }) {
  if (!loan?._id) {
    if (dryRun) stats.wouldUpsertGuarantors += 1;
    return;
  }

  if (dryRun) {
    stats.wouldUpsertGuarantors += 1;
    return;
  }

  await LoanGuarantorModel.findOneAndUpdate(
    {
      loanApplicationId: loan._id,
      guarantorUserId: guarantorSnapshot.profileId,
    },
    {
      $set: {
        guarantorName: guarantorSnapshot.name,
        guarantorEmail: guarantorSnapshot.email || null,
        guarantorPhone: guarantorSnapshot.phone || null,
        liabilityPercentage: 100,
        requestMessage: `Historical internal guarantor assignment for ${row.loanCode}.`,
        status: "accepted",
        responseComment: `Accepted as part of historical seed import from ${SOURCE_FILE_LABEL}.`,
        respondedAt: row.disbursedAt,
      },
      $setOnInsert: {
        loanApplicationId: loan._id,
        guarantorUserId: guarantorSnapshot.profileId,
      },
    },
    {
      upsert: true,
      new: true,
      runValidators: true,
      setDefaultsOnInsert: true,
    },
  );
  stats.upsertedGuarantors += 1;
}

async function rebuildScheduleAndSyncLoan({
  loan,
  row,
  schedule,
  dryRun,
  asOfDate,
  stats,
}) {
  if (!loan?._id) {
    if (dryRun) stats.wouldUpsertScheduleItems += schedule.items.length;
    return;
  }

  if (dryRun) {
    stats.wouldUpsertScheduleItems += schedule.items.length;
    return;
  }

  const installmentNumbers = schedule.items.map((item) => item.installmentNumber);
  await LoanRepaymentScheduleItemModel.deleteMany({
    loanApplicationId: loan._id,
    installmentNumber: { $nin: installmentNumbers },
    paidAmount: { $in: [null, 0] },
    paidPrincipalAmount: { $in: [null, 0] },
    paidInterestAmount: { $in: [null, 0] },
    transactionId: null,
  });

  for (const item of schedule.items) {
    await LoanRepaymentScheduleItemModel.findOneAndUpdate(
      {
        loanApplicationId: loan._id,
        installmentNumber: item.installmentNumber,
      },
      {
        $set: {
          ...item,
          loanApplicationId: loan._id,
        },
      },
      {
        upsert: true,
        new: true,
        runValidators: true,
        setDefaultsOnInsert: true,
      },
    );
  }

  stats.upsertedScheduleItems += schedule.items.length;
  await syncLoanRepaymentState(loan, { asOf: asOfDate });
  await persistLoanTimestamps(loan._id, row.disbursedAt);
}

async function upsertLoanDisbursementTransaction({
  loan,
  row,
  dryRun,
  stats,
}) {
  const reference = `CRC-LOAN-DISB-SEED-${row.loanCode}`;
  if (dryRun) {
    stats.wouldUpsertDisbursementTransactions += 1;
    return null;
  }

  const metadata = {
    seedData: true,
    sourceFile: SOURCE_FILE_LABEL,
    sourceRow: row.rowNumber,
    importKey: row.importKey,
    loanApplicationId: loan._id,
    loanCode: loan.loanCode,
    approvedAmount: row.principal,
    payoutGateway: "manual",
    payoutStatus: "success",
    disbursedAt: row.disbursedAt.toISOString(),
    manualDisbursement: {
      method: loan.manualDisbursement?.method || DEFAULT_DISBURSEMENT_METHOD,
      externalReference: loan.manualDisbursement?.externalReference || null,
      occurredAt: row.disbursedAt.toISOString(),
    },
  };

  const existing = await TransactionModel.findOne({ reference });
  const payload = {
    userId: loan.userId,
    reference,
    amount: row.principal,
    type: "loan_disbursement",
    status: "success",
    description: `Manual loan disbursement for ${loan.loanCode}`,
    channel: loan.manualDisbursement?.method || DEFAULT_DISBURSEMENT_METHOD,
    groupId: loan.groupId || null,
    groupName: loan.groupName || null,
    loanId: loan._id,
    loanName: loan.loanCode,
    metadata,
    gateway: "manual",
  };

  let transaction;
  if (existing) {
    existing.set(payload);
    transaction = await existing.save();
  } else {
    transaction = await TransactionModel.create(payload);
  }

  await TransactionModel.collection.updateOne(
    { _id: transaction._id },
    {
      $set: {
        date: row.disbursedAt,
        updatedAt: row.disbursedAt,
      },
    },
  );
  stats.upsertedDisbursementTransactions += 1;
  return transaction;
}

function buildFormPaymentDetails({ row, loan, profile, group }) {
  const facility = getLoanFacility(row.loanType);
  return {
    seedData: true,
    sourceFile: SOURCE_FILE_LABEL,
    sourceRow: row.rowNumber,
    importKey: row.importKey,
    applicationId: loan._id,
    loanCode: loan.loanCode,
    loanNumber: loan.loanNumber,
    loanType: loan.loanType,
    loanLabel: facility?.label || loan.loanType,
    loanAmount: row.principal,
    loanPurpose: loan.loanPurpose,
    repaymentPeriod: DEFAULT_TERM_MONTHS,
    interestRate: loan.interestRate,
    interestRateType: loan.interestRateType,
    status: loan.status,
    member: {
      fullName: profile?.fullName || null,
      email: profile?.email || null,
      phone: profile?.phone || null,
    },
    group: {
      id: group?._id || loan.groupId || null,
      name: group?.groupName || loan.groupName || null,
      number: group?.groupNumber || null,
    },
  };
}

async function upsertLoanFormPaymentAndTransaction({
  loan,
  row,
  profile,
  userAccount,
  group,
  dryRun,
  stats,
}) {
  const config = resolveLoanFormPaymentConfig(row.loanType);
  if (!config) {
    stats.skippedUnsupportedFormPaymentType += 1;
    return null;
  }

  const amount =
    row.formPaymentAmount !== null && row.formPaymentAmount !== undefined
      ? row.formPaymentAmount
      : config.amount;

  if (dryRun) {
    stats.wouldUpsertFormPayments += 1;
    stats.wouldUpsertFormPaymentTransactions += 1;
    return null;
  }

  const payment = await FormPaymentModel.findOneAndUpdate(
    {
      sourceModel: "LoanApplication",
      sourceId: loan._id,
      formType: config.formType,
    },
    {
      $set: {
        userId: loan.userId,
        userAccountId: userAccount?._id || null,
        groupId: loan.groupId || group?._id || null,
        groupName: group?.groupName || loan.groupName || null,
        memberName: profile?.fullName || null,
        memberEmail: profile?.email || userAccount?.email || null,
        memberPhone: profile?.phone || userAccount?.phone || null,
        formCategory: "loan",
        formLabel: config.formLabel,
        amount,
        currency: "NGN",
        paymentStatus: "paid",
        sourceReference: loan.loanCode,
        submittedAt: row.disbursedAt,
        reviewedAt: row.disbursedAt,
        reviewedBy: null,
        notes: `Historical loan form payment seeded from ${SOURCE_FILE_LABEL} row ${row.rowNumber}.`,
        formDetails: buildFormPaymentDetails({ row, loan, profile, group }),
      },
      $setOnInsert: {
        sourceModel: "LoanApplication",
        sourceId: loan._id,
        formType: config.formType,
      },
    },
    {
      upsert: true,
      new: true,
      runValidators: true,
      setDefaultsOnInsert: true,
    },
  );

  await FormPaymentModel.collection.updateOne(
    { _id: payment._id },
    {
      $set: {
        createdAt: row.disbursedAt,
        updatedAt: row.disbursedAt,
      },
    },
  );

  stats.upsertedFormPayments += 1;

  const reference = `CRC-FORM-SEED-${row.loanCode}`;
  const metadata = {
    seedData: true,
    paymentMethod: "manual",
    manual: true,
    sourceFile: SOURCE_FILE_LABEL,
    sourceRow: row.rowNumber,
    importKey: row.importKey,
    paymentType: "form_payment",
    formPaymentId: payment._id,
    formType: payment.formType,
    formCategory: payment.formCategory,
    formLabel: payment.formLabel,
    paymentStatus: payment.paymentStatus,
    sourceModel: payment.sourceModel,
    sourceId: payment.sourceId,
    sourceReference: payment.sourceReference,
    submittedAt: payment.submittedAt,
    reviewedAt: payment.reviewedAt,
  };

  const existingTransaction = await TransactionModel.findOne({
    $or: [
      { reference },
      { "metadata.formPaymentId": payment._id },
    ],
  });

  const txPayload = {
    userId: payment.userId,
    reference,
    amount: payment.amount,
    type: "form_payment",
    status: "success",
    description: `${payment.formLabel} payment - ${
      payment.memberName || "Member"
    }`,
    channel: "manual",
    groupId: payment.groupId || null,
    groupName: payment.groupName || null,
    loanId: loan._id,
    loanName: loan.loanCode,
    metadata,
    gateway: "manual",
  };

  let transaction;
  if (existingTransaction) {
    existingTransaction.set(txPayload);
    transaction = await existingTransaction.save();
  } else {
    transaction = await TransactionModel.create(txPayload);
  }

  await TransactionModel.collection.updateOne(
    { _id: transaction._id },
    {
      $set: {
        date: row.disbursedAt,
        updatedAt: row.disbursedAt,
      },
    },
  );
  await FormPaymentModel.updateOne(
    { _id: payment._id },
    {
      $set: {
        transactionId: transaction._id,
        transactionReference: transaction.reference,
      },
    },
  );
  await FormPaymentModel.collection.updateOne(
    { _id: payment._id },
    {
      $set: {
        updatedAt: row.disbursedAt,
      },
    },
  );

  stats.upsertedFormPaymentTransactions += 1;
  return payment;
}

function createStats({ dryRun, args, workbook }) {
  return {
    dryRun,
    confirmationRequired: dryRun ? CONFIRMATION_TOKEN : null,
    input: args.input || DEFAULT_INPUT,
    worksheet: workbook.worksheetName,
    hasLoanFormPaymentColumn: workbook.hasLoanFormPaymentColumn,
    forceResetRepaymentState: Boolean(args["force-reset-repayment-state"]),
    workbookWarnings: workbook.warnings,
    parsedRows: workbook.rows.length,
    scannedRows: 0,
    wouldCreateLoans: 0,
    wouldUpdateLoans: 0,
    createdLoans: 0,
    updatedLoans: 0,
    skippedMissingMembership: 0,
    skippedMissingProfile: 0,
    skippedMissingGroup: 0,
    skippedMissingGuarantor: 0,
    skippedRepaymentActivity: 0,
    skippedUnsupportedFormPaymentType: 0,
    wouldUpsertGuarantors: 0,
    upsertedGuarantors: 0,
    wouldUpsertScheduleItems: 0,
    upsertedScheduleItems: 0,
    wouldUpsertDisbursementTransactions: 0,
    upsertedDisbursementTransactions: 0,
    wouldUpsertFormPayments: 0,
    upsertedFormPayments: 0,
    wouldUpsertFormPaymentTransactions: 0,
    upsertedFormPaymentTransactions: 0,
    failed: 0,
    warnings: [],
    errors: [],
  };
}

async function processRows({
  rows,
  context,
  dryRun,
  asOfDate,
  disbursementMethod,
  batchSize,
  stats,
}) {
  const nextLoanNumber = { value: await getNextLoanNumberSeed() };
  const existingSeedContext = await loadExistingSeedContext(rows);
  const usedGuarantorIds = new Set(
    Array.from(existingSeedContext.guarantorByLoanId.values())
      .map((guarantor) => String(guarantor.guarantorUserId))
      .filter(Boolean),
  );

  for (const row of rows) {
    stats.scannedRows += 1;
    try {
      const membership = context.membershipBySerial.get(row.serial);
      if (!membership) {
        stats.skippedMissingMembership += 1;
        stats.warnings.push({
          rowNumber: row.rowNumber,
          serial: row.serial,
          message: "No GroupMembership was found for the workbook serial.",
        });
        continue;
      }

      const profile = context.profileById.get(String(membership.userId));
      if (!profile) {
        stats.skippedMissingProfile += 1;
        stats.warnings.push({
          rowNumber: row.rowNumber,
          serial: row.serial,
          profileId: String(membership.userId),
          message: "No Profile was found for the matched membership.",
        });
        continue;
      }

      const group = context.groupById.get(String(membership.groupId));
      if (!group) {
        stats.skippedMissingGroup += 1;
        stats.warnings.push({
          rowNumber: row.rowNumber,
          serial: row.serial,
          groupId: String(membership.groupId),
          message: "No Group was found for the matched membership.",
        });
        continue;
      }

      const interest = resolveInterestRate(row.loanType);
      const schedule = buildRepaymentSchedule({
        principal: row.principal,
        ratePct: interest.rate,
        rateType: interest.rateType,
        months: DEFAULT_TERM_MONTHS,
        startDate: row.repaymentStartDate,
      });

      const preExistingLoan = existingSeedContext.loanByCode.get(row.loanCode);
      const guarantor = await chooseGuarantor({
        loan: preExistingLoan,
        row,
        applicantProfileId: membership.userId,
        groupId: membership.groupId,
        context,
        existingSeedContext,
        usedGuarantorIds,
      });

      if (!guarantor) {
        stats.skippedMissingGuarantor += 1;
        stats.warnings.push({
          rowNumber: row.rowNumber,
          serial: row.serial,
          message:
            "No eligible internal guarantor was found. Applicant cannot guarantee their own loan.",
        });
        continue;
      }

      const guarantorSnapshot = buildGuarantorSnapshot(
        guarantor,
        row.disbursedAt,
      );

      const { loan, skipped } = await upsertLoan({
        existingLoan: preExistingLoan,
        hasExistingRepaymentActivity: preExistingLoan?._id
          ? existingSeedContext.repaymentActivityLoanIds.has(
              String(preExistingLoan._id),
            )
          : false,
        row,
        membership,
        profile,
        group,
        guarantorSnapshot,
        interest,
        schedule,
        disbursementMethod,
        nextLoanNumber,
        dryRun,
        stats,
      });

      if (skipped) continue;
      if (!dryRun && loan?._id) {
        existingSeedContext.loanByCode.set(row.loanCode, loan);
      }

      await upsertGuarantor({
        loan,
        guarantorSnapshot,
        row,
        dryRun,
        stats,
      });
      await rebuildScheduleAndSyncLoan({
        loan,
        row,
        schedule,
        dryRun,
        asOfDate,
        stats,
      });
      await upsertLoanDisbursementTransaction({
        loan,
        row,
        dryRun,
        stats,
      });
      await upsertLoanFormPaymentAndTransaction({
        loan,
        row,
        profile,
        userAccount: context.userByProfileId.get(String(membership.userId)),
        group,
        dryRun,
        stats,
      });
    } catch (error) {
      stats.failed += 1;
      if (stats.errors.length < 30) {
        stats.errors.push({
          rowNumber: row.rowNumber,
          serial: row.serial,
          loanCode: row.loanCode,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }

    if (stats.scannedRows % batchSize === 0) {
      // eslint-disable-next-line no-console
      console.log(
        JSON.stringify({
          progress: {
            scannedRows: stats.scannedRows,
            parsedRows: stats.parsedRows,
            createdLoans: stats.createdLoans,
            updatedLoans: stats.updatedLoans,
            failed: stats.failed,
          },
        }),
      );
    }
  }
}

const args = parseArgs(process.argv.slice(2));

if (args.help) {
  printHelp();
  process.exit(0);
}

const dryRun = Boolean(args["dry-run"]) || args.confirm !== CONFIRMATION_TOKEN;
const mongoUri = process.env.MONGO_URI;

try {
  if (!mongoUri) throw new Error("Missing MONGO_URI");

  const year = parsePositiveInteger(args.year, "year", DEFAULT_YEAR);
  const limit = parsePositiveInteger(args.limit, "limit", null);
  const batchSize = parsePositiveInteger(
    args["batch-size"],
    "batch-size",
    DEFAULT_BATCH_SIZE,
  );
  const asOfDate = parseDateArg(args["as-of"], "as-of", new Date());
  const disbursementMethod = resolveDisbursementMethod(
    args["disbursement-method"],
  );
  const inputPath = path.resolve(String(args.input || DEFAULT_INPUT));

  const workbook = await parseWorkbookRows({
    inputPath,
    sheetName: args.sheet ? String(args.sheet) : null,
    year,
  });

  if (limit) workbook.rows = workbook.rows.slice(0, limit);

  const stats = createStats({ dryRun, args, workbook });
  if (!workbook.hasLoanFormPaymentColumn) {
    stats.warnings.push({
      message:
        'Workbook does not contain a "Loan Form Payment" column; using current configured loan-form fees by loan type.',
    });
  }

  await connectMongo({ mongoUri });
  const context = await loadContext(workbook.rows);

  await processRows({
    rows: workbook.rows,
    context,
    dryRun,
    asOfDate,
    disbursementMethod,
    batchSize,
    stats,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: stats.failed === 0 ? 1 : 0,
        message: dryRun
          ? `Dry run only. Re-run with --confirm ${CONFIRMATION_TOKEN} to write records.`
          : "Existing loan applications seeded successfully.",
        stats,
      },
      null,
      2,
    ),
  );

  if (stats.failed > 0) process.exitCode = 1;
} catch (error) {
  // eslint-disable-next-line no-console
  console.error(
    JSON.stringify(
      {
        ok: 0,
        error: error instanceof Error ? error.message : String(error),
      },
      null,
      2,
    ),
  );
  process.exitCode = 1;
} finally {
  await mongoose.disconnect();
}
