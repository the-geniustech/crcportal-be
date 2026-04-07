import crypto from "crypto";
import path from "path";
import {
  calculateContributionInterestForType,
  normalizeContributionType,
} from "../../../utils/contributionPolicy.js";

const DEFAULT_PHONE_PREFIXES = ["080", "081", "070", "090", "091"];

const normalizeText = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).replace(/\s+/g, " ").trim();
};

const slugifyName = (name) =>
  normalizeText(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.|\.$/g, "")
    .replace(/\.\.+/g, ".")
    .slice(0, 50);

const makeObjectId = (namespace, seed) => {
  const hash = crypto
    .createHash("md5")
    .update(`${namespace}:${seed}`)
    .digest("hex");
  return hash.slice(0, 24);
};

const median = (values) => {
  const cleaned = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  if (!cleaned.length) return null;
  cleaned.sort((a, b) => a - b);
  const middle = Math.floor(cleaned.length / 2);
  if (cleaned.length % 2 === 0) {
    return (cleaned[middle - 1] + cleaned[middle]) / 2;
  }
  return cleaned[middle];
};

const parseGroupInfoFromFilename = (inputPath) => {
  const base = path.basename(inputPath, path.extname(inputPath));
  const clean = base.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  const match = clean.match(/group\s*(\d+)\s*(.*)/i);
  if (match) {
    const groupNumber = Number(match[1]);
    const namePart = normalizeText(match[2]);
    return {
      groupNumber: Number.isFinite(groupNumber) ? groupNumber : null,
      groupName: namePart || null,
    };
  }

  const leadingNumber = clean.match(/^(\d+)\s*(.*)/);
  if (leadingNumber) {
    const groupNumber = Number(leadingNumber[1]);
    const namePart = normalizeText(leadingNumber[2]);
    return {
      groupNumber: Number.isFinite(groupNumber) ? groupNumber : null,
      groupName: namePart || null,
    };
  }

  return { groupNumber: null, groupName: clean || null };
};

const createUniqueEmailFactory = () => {
  const used = new Set();
  return (fullName) => {
    const base = slugifyName(fullName) || "member";
    let suffix = 0;
    let candidate = `${base}@crc.local`;
    while (used.has(candidate)) {
      suffix += 1;
      candidate = `${base}.${suffix}@crc.local`;
    }
    used.add(candidate);
    return candidate;
  };
};

const createUniquePhoneFactory = () => {
  const used = new Set();
  let counter = 0;
  return () => {
    let candidate = "";
    do {
      const prefix =
        DEFAULT_PHONE_PREFIXES[counter % DEFAULT_PHONE_PREFIXES.length];
      const body = String(10000000 + counter).slice(-8);
      candidate = `${prefix}${body}`;
      counter += 1;
    } while (used.has(candidate));
    used.add(candidate);
    return candidate;
  };
};

const resolvePaymentDate = (year, month, day) => {
  const safeDay = Number.isFinite(day) ? Math.max(1, Math.min(day, 28)) : 5;
  const date = new Date(Date.UTC(year, month - 1, safeDay));
  return date.toISOString();
};

const ensureNumber = (value, fallback = null) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const formatGroupMemberSerial = ({ groupNumber, memberNumber }) => {
  const groupPart = String(groupNumber ?? "").trim() || "0";
  const memberPart = String(memberNumber ?? 0).padStart(4, "0");
  return `CRC/G${groupPart}/${memberPart}`;
};

export function transformContributionSheet(parsed, options = {}) {
  const now = new Date();
  const year = ensureNumber(options.year, now.getUTCFullYear());
  const paymentDay = ensureNumber(options.paymentDay, 5);
  const defaultUnits = ensureNumber(options.defaultUnits, 1);
  const defaultMonthlyContribution = ensureNumber(
    options.defaultMonthlyContribution,
    5000,
  );
  const contributionType =
    normalizeContributionType(options.contributionType) || "revolving";
  const cycleDuration = ensureNumber(
    options.cycleDuration,
    parsed.meta.monthsDetected.length || 12,
  );
  const membershipStatus = options.membershipStatus || "active";
  const groupStatus = options.groupStatus || "active";
  const joinedAt = options.joinedAt || "2024-01-01T00:00:00.000Z";
  const defaultPassword = options.defaultPassword || "ChangeMe123!";
  const verifiedAt = options.verifiedAt || now.toISOString();

  const inferredGroup = parseGroupInfoFromFilename(parsed.meta.inputPath);
  const groupNumber = ensureNumber(
    options.groupNumber,
    inferredGroup.groupNumber || 0,
  );
  const groupName =
    normalizeText(options.groupName || inferredGroup.groupName) ||
    `Group ${groupNumber || ""}`.trim();

  const groupSeedKey = `group-${String(groupNumber || 0).padStart(2, "0")}`;
  const groupId = makeObjectId("group", groupSeedKey);

  const emailForName = createUniqueEmailFactory();
  const phoneForMember = createUniquePhoneFactory();

  const monthNumbers = parsed.meta.monthsDetected.length
    ? parsed.meta.monthsDetected
    : Array.from({ length: 12 }, (_, i) => i + 1);

  const perUnitCandidates = [];
  const memberMonthlyMedians = [];
  const unitValues = [];

  parsed.rows.forEach((row) => {
    const amounts = monthNumbers
      .map((month) => row.contributions[month])
      .filter((value) => Number.isFinite(value) && value > 0);
    if (amounts.length) {
      const rowMedian = median(amounts);
      if (rowMedian !== null) memberMonthlyMedians.push(rowMedian);
    }
    if (Number.isFinite(row.units) && row.units > 0) {
      unitValues.push(row.units);
      monthNumbers.forEach((month) => {
        const amount = row.contributions[month];
        if (Number.isFinite(amount) && amount > 0) {
          perUnitCandidates.push(amount / row.units);
        }
      });
    }
  });

  const unitAmount = median(perUnitCandidates) || null;
  const medianUnits = median(unitValues) || defaultUnits;
  let groupMonthlyContribution = median(memberMonthlyMedians);
  if (!groupMonthlyContribution && unitAmount) {
    groupMonthlyContribution = unitAmount * medianUnits;
  }
  if (!groupMonthlyContribution) {
    groupMonthlyContribution = defaultMonthlyContribution;
  }

  const profiles = [];
  const users = [];
  const groups = [];
  const groupMembers = [];
  const contributions = [];
  const transactions = [];
  const contributionSettings = [];
  const warnings = [...(parsed.meta.warnings || [])];

  groups.push({
    seedKey: groupSeedKey,
    _id: groupId,
    groupNumber: groupNumber || 0,
    groupName,
    description: `Contribution group for ${groupName}.`,
    monthlyContribution: groupMonthlyContribution,
    maxMembers: parsed.rows.length || 1,
    memberCount: parsed.rows.length || 0,
    totalSavings: 0,
    status: groupStatus,
    isOpen: true,
    type: contributionType,
    cycleDuration,
    createdAt: verifiedAt,
    updatedAt: verifiedAt,
  });

  let memberCounter = 0;

  parsed.rows.forEach((row) => {
    memberCounter += 1;
    const seedKey = `member-${String(memberCounter).padStart(4, "0")}`;
    const rawName = normalizeText(row.name);
    const fullName =
      rawName || `Member ${String(memberCounter).padStart(3, "0")}`;

    const email = emailForName(fullName);
    const phone = phoneForMember();

    const profileId = makeObjectId("profile", seedKey);
    const userId = makeObjectId("user", seedKey);
    const membershipId = makeObjectId(
      "membership",
      `${seedKey}:${groupSeedKey}`,
    );
    const settingId = makeObjectId(
      "contribution-setting",
      `${seedKey}:${groupSeedKey}`,
    );

    let resolvedUnits =
      Number.isFinite(row.units) && row.units > 0 ? row.units : null;
    if (!resolvedUnits && unitAmount) {
      const firstAmount = monthNumbers
        .map((month) => row.contributions[month])
        .find((value) => Number.isFinite(value) && value > 0);
      if (Number.isFinite(firstAmount) && unitAmount > 0) {
        resolvedUnits = Math.max(1, Math.round(firstAmount / unitAmount));
      }
    }
    if (!resolvedUnits) resolvedUnits = defaultUnits;

    const rowAmounts = monthNumbers
      .map((month) => row.contributions[month])
      .filter((value) => Number.isFinite(value) && value > 0);
    const rowMedian = median(rowAmounts);
    let expectedMonthlyAmount = null;
    if (unitAmount && resolvedUnits) {
      expectedMonthlyAmount = unitAmount * resolvedUnits;
    } else if (rowMedian) {
      expectedMonthlyAmount = rowMedian;
    } else {
      expectedMonthlyAmount = groupMonthlyContribution;
    }

    const totalActual = rowAmounts.reduce((sum, amount) => sum + amount, 0);
    const totalExpected = expectedMonthlyAmount * monthNumbers.length;
    const outstandingBalance = Math.max(totalExpected - totalActual, 0);

    profiles.push({
      seedKey,
      _id: profileId,
      fullName,
      email: email.toLowerCase(),
      phone,
      membershipStatus,
      contributionSettings: {
        year,
        units: resolvedUnits,
        updatedAt: verifiedAt,
      },
      createdAt: verifiedAt,
      updatedAt: verifiedAt,
    });

    users.push({
      seedKey,
      _id: userId,
      email: email.toLowerCase(),
      phone,
      password: defaultPassword,
      role: ["member"],
      profileId,
      emailVerifiedAt: verifiedAt,
      phoneVerifiedAt: verifiedAt,
      createdAt: verifiedAt,
      updatedAt: verifiedAt,
    });

    groupMembers.push({
      seedKey: `membership-${String(memberCounter).padStart(4, "0")}`,
      _id: membershipId,
      userId: profileId,
      groupId,
      role: "member",
      status: "active",
      joinedAt,
      totalContributed: totalActual,
      memberNumber: memberCounter,
      memberSerial: formatGroupMemberSerial({
        groupNumber,
        memberNumber: memberCounter,
      }),
      createdAt: verifiedAt,
      updatedAt: verifiedAt,
    });

    contributionSettings.push({
      seedKey: `settings-${String(memberCounter).padStart(4, "0")}`,
      _id: settingId,
      userId: profileId,
      groupId,
      year,
      contributionType,
      expectedMonthlyAmount,
      totalExpected,
      totalActual,
      outstandingBalance,
      units: resolvedUnits,
      createdAt: verifiedAt,
      updatedAt: verifiedAt,
    });

    monthNumbers.forEach((month) => {
      const amount = row.contributions[month];
      if (!Number.isFinite(amount) || amount <= 0) return;
      const contributionId = makeObjectId(
        "contribution",
        `${seedKey}:${groupSeedKey}:${year}:${month}`,
      );
      const paymentDate = resolvePaymentDate(year, month, paymentDay);
      const interestAmount = calculateContributionInterestForType(
        contributionType,
        amount,
      );

      contributions.push({
        seedKey: `contribution-${seedKey}-${year}-${String(month).padStart(2, "0")}`,
        _id: contributionId,
        userId: profileId,
        groupId,
        month,
        year,
        amount,
        contributionType,
        units: resolvedUnits,
        interestAmount,
        status: "verified",
        verifiedAt: paymentDate,
        createdAt: paymentDate,
        updatedAt: paymentDate,
      });
    });
  });

  contributions.forEach((contribution) => {
    const reference = `GC-${contribution._id}`;
    const transactionId = makeObjectId("transaction", reference);
    const date =
      contribution.createdAt || contribution.verifiedAt || verifiedAt;
    transactions.push({
      seedKey: `transaction-${contribution.seedKey}`,
      _id: transactionId,
      userId: contribution.userId,
      reference,
      amount: contribution.amount,
      type: "group_contribution",
      status: "success",
      description: `Group contribution - ${groupName}`,
      channel: "seed",
      groupId,
      groupName,
      metadata: {
        contributionId: contribution._id,
        month: contribution.month,
        year: contribution.year,
        contributionType: contribution.contributionType,
      },
      gateway: "internal",
      date,
      updatedAt: date,
    });
  });

  if (!groupNumber) {
    warnings.push("Group number not detected; defaulted to 0.");
  }

  return {
    meta: {
      inputPath: parsed.meta.inputPath,
      sheetName: parsed.meta.sheetName,
      groupSeedKey,
      groupName,
      groupNumber,
      year,
      monthsDetected: monthNumbers,
      unitAmount,
      groupMonthlyContribution,
      cycleDuration,
      warnings,
    },
    users,
    profiles,
    groups,
    groupMembers,
    contributions,
    transactions,
    contributionSettings,
  };
}
