import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import dotenv from "dotenv";
import mongoose from "mongoose";
import { fileURLToPath } from "url";
import { connectMongo } from "../../db.js";
import "../../models/Profile.js";
import "../../models/Group.js";
import { GroupMembershipModel } from "../../models/GroupMembership.js";
import {
  ContributionUnitBase,
  normalizeContributionType,
} from "../../utils/contributionPolicy.js";
import { parseSpecialContributionWorkbook } from "./parser/specialContributionWorkbookParser.js";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDir = path.dirname(currentFilePath);

dotenv.config({ path: path.resolve(currentDir, "../../../.env") });

const DEFAULT_ENDWELL_DIR = path.resolve(
  currentDir,
  "../ENDWELL_CONTRIBUTION",
);
const DEFAULT_FESTIVAL_DIR = path.resolve(
  currentDir,
  "../FESTIVAL_CONTRIBUTION",
);
const DEFAULT_OUTPUT_DIR = path.resolve(
  currentDir,
  "../../seed-data/special-contributions",
);
const DEFAULT_PAYMENT_DAY = 5;

const CONTRIBUTION_TYPES = [
  {
    type: "endwell",
    contributionType: "endwell",
    inputDir: DEFAULT_ENDWELL_DIR,
  },
  {
    type: "festival",
    contributionType: "festive",
    inputDir: DEFAULT_FESTIVAL_DIR,
  },
];

function parseArgs(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    if (!current.startsWith("--")) continue;
    const key = current.replace(/^--/, "");
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      parsed[key] = true;
      continue;
    }
    parsed[key] = next;
    index += 1;
  }
  return parsed;
}

const normalizeText = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).replace(/\s+/g, " ").trim();
};

const normalizeNameKey = (value) =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const parseNumber = (value, fallback = null) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const roundMoney = (value) =>
  Math.round((parseNumber(value, 0) || 0) * 100) / 100;

const makeObjectId = (namespace, seed) =>
  crypto
    .createHash("md5")
    .update(`${namespace}:${seed}`)
    .digest("hex")
    .slice(0, 24);

const buildProfileSeedKey = (profileId) => `profile-${String(profileId)}`;
const buildMembershipSeedKey = (membershipId) =>
  `membership-${String(membershipId)}`;
const buildGroupSeedKey = (groupNumber, groupId) => {
  const normalizedGroupNumber = parseNumber(groupNumber);
  if (normalizedGroupNumber && normalizedGroupNumber > 0) {
    return `group-${String(normalizedGroupNumber).padStart(2, "0")}`;
  }
  return `group-${String(groupId)}`;
};

const toIsoMonthDate = (year, month, day = DEFAULT_PAYMENT_DAY) => {
  const safeDay = Math.max(1, Math.min(parseNumber(day, DEFAULT_PAYMENT_DAY), 28));
  return new Date(Date.UTC(Number(year), Number(month) - 1, safeDay)).toISOString();
};

const buildTimestamp = (date = new Date()) =>
  date.toISOString().replace(/[:.]/g, "-");

const normalizeSerial = (value) => {
  const raw = normalizeText(value).toUpperCase();
  if (!raw) return "";
  const cleaned = raw
    .replace(/[_\s-]+/g, "")
    .replace(/\\/g, "/")
    .replace(/\/+/g, "/");

  const match = cleaned.match(/^(?:CRC\/)?G?(\d{1,3})\/(\d{1,4})$/i);
  if (!match) return cleaned;

  const [, groupNumber, memberNumber] = match;
  return `CRC/G${Number(groupNumber)}/${String(memberNumber).padStart(4, "0")}`;
};

const roughlyMatchesName = (left, right) => {
  const leftKey = normalizeNameKey(left);
  const rightKey = normalizeNameKey(right);
  if (!leftKey || !rightKey) return true;
  return leftKey === rightKey;
};

const sumPositiveAmounts = (contributions = {}) =>
  Object.values(contributions).reduce((sum, amount) => {
    const numeric = parseNumber(amount);
    if (!numeric || numeric <= 0) return sum;
    return roundMoney(sum + numeric);
  }, 0);

const inferUnitsFromRow = (row) => {
  const explicitUnits = parseNumber(row.units);
  if (explicitUnits && explicitUnits > 0) {
    return explicitUnits;
  }

  const positiveAmounts = Object.values(row.contributions || {})
    .map((amount) => parseNumber(amount))
    .filter((amount) => amount && amount > 0)
    .sort((a, b) => b - a);
  if (!positiveAmounts.length) return null;

  const inferredUnits = positiveAmounts[0] / ContributionUnitBase;
  if (Number.isInteger(inferredUnits) && inferredUnits > 0) {
    return inferredUnits;
  }

  return null;
};

const parseGroupInfoFromFileName = (fileName) => {
  const withoutExtension = path.basename(fileName, path.extname(fileName));
  const normalized = withoutExtension.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  const groupNumberMatch = normalized.match(/group\s*(\d+)/i);
  const groupNumber = groupNumberMatch ? Number(groupNumberMatch[1]) : null;
  let groupName = normalized
    .replace(/group\s*\d+/i, "")
    .replace(/\b(endwell|festival|updated|new)\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
  groupName = groupName.replace(/^\W+|\W+$/g, "");

  return {
    groupNumber,
    groupName: groupName || null,
  };
};

const normalizeUnitsForYear = (rawUnits, storedYear, targetYear) => {
  const base = {
    revolving: null,
    endwell: null,
    festive: null,
  };

  if (storedYear !== targetYear) return base;

  if (typeof rawUnits === "number" || typeof rawUnits === "string") {
    const numeric = parseNumber(rawUnits);
    if (numeric && numeric > 0) {
      base.revolving = numeric;
    }
    return base;
  }

  if (!rawUnits || typeof rawUnits !== "object") return base;

  Object.keys(base).forEach((key) => {
    const numeric = parseNumber(rawUnits[key]);
    if (numeric && numeric > 0) {
      base[key] = numeric;
    }
  });

  return base;
};

async function listWorkbookFiles(dirPath) {
  const entries = await fs.readdir(dirPath, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && /\.xlsx$/i.test(entry.name))
    .map((entry) => path.join(dirPath, entry.name))
    .sort((left, right) => left.localeCompare(right));
}

function createWarningCollector() {
  const warnings = [];
  return {
    add(code, payload) {
      warnings.push({ code, ...payload });
    },
    all() {
      return warnings;
    },
  };
}

async function loadMembershipContext(serials) {
  const mongoUri = process.env.MONGO_URI;
  if (!mongoUri) {
    throw new Error("Missing MONGO_URI");
  }

  await connectMongo({ mongoUri });

  const memberships = await GroupMembershipModel.find({
    memberSerial: { $in: serials },
  })
    .select("_id userId groupId memberSerial memberNumber joinedAt status role totalContributed")
    .populate({
      path: "userId",
      select: "_id fullName email phone contributionSettings membershipStatus",
    })
    .populate({
      path: "groupId",
      select: "_id groupNumber groupName monthlyContribution status",
    })
    .lean();

  const membershipBySerial = new Map();
  memberships.forEach((membership) => {
    const serial = normalizeSerial(membership.memberSerial);
    if (serial && !membershipBySerial.has(serial)) {
      membershipBySerial.set(serial, membership);
    }
  });

  return membershipBySerial;
}

async function collectWorkbookRows(workbookConfigs, warningCollector) {
  const parsedFiles = [];
  for (const config of workbookConfigs) {
    const files = await listWorkbookFiles(config.inputDir);
    for (const filePath of files) {
      const parsed = await parseSpecialContributionWorkbook({ inputPath: filePath });
      parsedFiles.push({
        ...config,
        inputPath: filePath,
        parsed,
      });

      parsed.meta.warnings.forEach((message) => {
        warningCollector.add("workbook_parser_warning", {
          type: config.type,
          fileName: parsed.meta.fileName,
          inputPath: parsed.meta.inputPath,
          message,
        });
      });
    }
  }
  return parsedFiles;
}

function buildContributionSeed({
  row,
  month,
  amount,
  year,
  type,
  contributionType,
  paymentDay,
  membership,
  profile,
  group,
  fileInfo,
}) {
  const groupNumber = parseNumber(group?.groupNumber, 0);
  const groupSeedKey = buildGroupSeedKey(groupNumber, group?._id);
  const profileSeedKey = buildProfileSeedKey(profile?._id);
  const membershipSeedKey = buildMembershipSeedKey(membership?._id);
  const seedKey = [
    "special",
    type,
    groupNumber || "x",
    normalizeSerial(row.serial),
    year,
    String(month).padStart(2, "0"),
  ].join("-");
  const contributionId = makeObjectId("special-contribution", seedKey);
  const date = toIsoMonthDate(year, month, paymentDay);

  return {
    seedKey,
    _id: contributionId,
    profileId: String(profile?._id),
    profileSeedKey,
    groupId: String(group?._id),
    groupSeedKey,
    membershipId: String(membership?._id),
    membershipSeedKey,
    memberSerial: normalizeSerial(row.serial),
    memberName: profile?.fullName || row.name || null,
    fileGroupName: fileInfo.groupNameFromFile,
    fileGroupNumber: fileInfo.groupNumberFromFile,
    sourceFile: fileInfo.fileName,
    sourcePath: fileInfo.inputPath,
    sourceSheetName: fileInfo.sheetName,
    type,
    contributionType,
    amount: roundMoney(amount),
    units: parseNumber(row.units, 0) || inferUnitsFromRow(row) || 0,
    month,
    year,
    date,
    paymentDate: date,
    status: "verified",
    paymentReference: `SPC-${type.toUpperCase()}-${contributionId}`,
    paymentMethod: "seed_import",
    interestAmount: 0,
    verifiedAt: date,
    createdAt: date,
    updatedAt: date,
  };
}

function buildTransactionSeed({
  contribution,
  type,
  contributionType,
  group,
  fileInfo,
}) {
  const transactionSeedKey = `transaction-${contribution.seedKey}`;
  const reference = `TX-${type.toUpperCase()}-${contribution._id}`;
  return {
    seedKey: transactionSeedKey,
    _id: makeObjectId("special-transaction", transactionSeedKey),
    profileId: contribution.profileId,
    profileSeedKey: contribution.profileSeedKey,
    userId: contribution.profileId,
    reference,
    amount: contribution.amount,
    type: "group_contribution",
    contributionType,
    status: "success",
    description: `${type} contribution - ${group?.groupName || fileInfo.groupNameFromFile || "Group"}`,
    channel: "seed",
    groupId: contribution.groupId,
    groupSeedKey: contribution.groupSeedKey,
    groupName: group?.groupName || fileInfo.groupNameFromFile || null,
    memberSerial: contribution.memberSerial,
    memberName: contribution.memberName,
    gateway: "internal",
    date: contribution.date,
    updatedAt: contribution.updatedAt,
    metadata: {
      contributionId: contribution._id,
      month: contribution.month,
      year: contribution.year,
      contributionType,
      sourceType: type,
      sourceFile: contribution.sourceFile,
      sourceSheetName: contribution.sourceSheetName,
      memberSerial: contribution.memberSerial,
    },
  };
}

function buildProfileContributionSettingSeed({
  aggregate,
  generatedAt,
}) {
  const nextContributionSettings = {
    year: aggregate.year,
    units: aggregate.nextUnits,
    updatedAt: generatedAt,
  };

  return {
    seedKey: aggregate.seedKey,
    profileId: aggregate.profileId,
    profileSeedKey: aggregate.profileSeedKey,
    groupId: aggregate.groupId,
    groupSeedKey: aggregate.groupSeedKey,
    membershipId: aggregate.membershipId,
    membershipSeedKey: aggregate.membershipSeedKey,
    memberSerial: aggregate.memberSerial,
    memberName: aggregate.memberName,
    type: aggregate.type,
    contributionType: aggregate.contributionType,
    year: aggregate.year,
    units: aggregate.units,
    expectedMonthlyAmount: aggregate.expectedMonthlyAmount,
    monthsWithContributions: Array.from(aggregate.monthsWithContributions).sort(
      (left, right) => left - right,
    ),
    sourceFiles: Array.from(aggregate.sourceFiles).sort((left, right) =>
      left.localeCompare(right),
    ),
    currentContributionSettings: aggregate.currentContributionSettings,
    nextContributionSettings,
    updateFilter: { _id: aggregate.profileId },
    updateDocument: {
      $set: {
        contributionSettings: nextContributionSettings,
      },
    },
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const year = parseNumber(args.year, new Date().getUTCFullYear());
  const paymentDay = parseNumber(args.paymentDay, DEFAULT_PAYMENT_DAY);
  const outputDir = path.resolve(args.outputDir || DEFAULT_OUTPUT_DIR);
  const generatedAt = new Date().toISOString();
  const warningCollector = createWarningCollector();

  const workbookConfigs = CONTRIBUTION_TYPES.map((config) => ({
    ...config,
    inputDir:
      config.type === "endwell"
        ? path.resolve(args.endwellDir || config.inputDir)
        : path.resolve(args.festivalDir || config.inputDir),
  }));

  const parsedFiles = await collectWorkbookRows(workbookConfigs, warningCollector);

  const serials = [...new Set(
    parsedFiles
      .flatMap((file) => file.parsed.rows.map((row) => normalizeSerial(row.serial)))
      .filter(Boolean),
  )];

  const membershipBySerial = await loadMembershipContext(serials);

  const contributions = [];
  const transactions = [];
  const profileContributionSettings = [];
  const fileSummaries = [];

  const contributionKeySet = new Set();
  const settingAggregates = new Map();

  for (const file of parsedFiles) {
    const fileInfo = parseGroupInfoFromFileName(file.parsed.meta.fileName);
    const fileSummary = {
      type: file.type,
      contributionType: file.contributionType,
      fileName: file.parsed.meta.fileName,
      inputPath: file.parsed.meta.inputPath,
      sheetName: file.parsed.meta.sheetName,
      groupNumberFromFile: fileInfo.groupNumber,
      groupNameFromFile: fileInfo.groupName,
      parsedRows: file.parsed.rows.length,
      matchedRows: 0,
      skippedRows: 0,
      generatedContributions: 0,
      generatedTransactions: 0,
      generatedProfileContributionSettings: 0,
    };
    const profileSettingKeysTouched = new Set();

    for (const row of file.parsed.rows) {
      const normalizedSerial = normalizeSerial(row.serial);

      if (!normalizedSerial) {
        fileSummary.skippedRows += 1;
        warningCollector.add("missing_serial", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberName: row.name,
          message: "Row skipped because the serial column is empty.",
        });
        continue;
      }

      const membership = membershipBySerial.get(normalizedSerial);
      if (!membership) {
        fileSummary.skippedRows += 1;
        warningCollector.add("membership_not_found", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          memberName: row.name,
          message: "Row skipped because no GroupMembership record matched the serial.",
        });
        continue;
      }

      const profile =
        membership.userId && typeof membership.userId === "object"
          ? membership.userId
          : null;
      const group =
        membership.groupId && typeof membership.groupId === "object"
          ? membership.groupId
          : null;

      if (!profile || !group) {
        fileSummary.skippedRows += 1;
        warningCollector.add("membership_context_incomplete", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          message:
            "Row skipped because the matching membership did not resolve both profile and group references.",
        });
        continue;
      }

      fileSummary.matchedRows += 1;

      if (
        fileInfo.groupNumber &&
        parseNumber(group.groupNumber) &&
        Number(fileInfo.groupNumber) !== Number(group.groupNumber)
      ) {
        warningCollector.add("group_number_mismatch", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          fileGroupNumber: fileInfo.groupNumber,
          membershipGroupNumber: Number(group.groupNumber),
          message:
            "The workbook group number does not match the resolved membership group number. The membership group will be used.",
        });
      }

      if (!roughlyMatchesName(row.name, profile.fullName)) {
        warningCollector.add("member_name_mismatch", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          workbookName: row.name,
          profileName: profile.fullName,
          message:
            "Workbook member name differs from the resolved profile full name.",
        });
      }

      const resolvedUnits = inferUnitsFromRow(row);
      if (!resolvedUnits) {
        warningCollector.add("units_missing", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          memberName: profile.fullName || row.name,
          message:
            "Unit value could not be resolved from the row. Profile contribution settings will not be updated from this row.",
        });
      }

      const computedTotal = sumPositiveAmounts(row.contributions);
      if (
        parseNumber(row.total) !== null &&
        roundMoney(row.total) > 0 &&
        roundMoney(row.total) !== roundMoney(computedTotal)
      ) {
        warningCollector.add("row_total_mismatch", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          workbookTotal: roundMoney(row.total),
          computedTotal,
          message:
            "Workbook total column does not match the sum of monthly values used for seeding.",
        });
      }
      if (
        parseNumber(row.grandTotal) !== null &&
        roundMoney(row.grandTotal) > 0 &&
        roundMoney(row.grandTotal) !== roundMoney(computedTotal)
      ) {
        warningCollector.add("row_grand_total_mismatch", {
          type: file.type,
          fileName: file.parsed.meta.fileName,
          rowIndex: row.rowIndex,
          memberSerial: normalizedSerial,
          workbookGrandTotal: roundMoney(row.grandTotal),
          computedTotal,
          message:
            "Workbook grand total column does not match the sum of monthly values used for seeding.",
        });
      }

      if (resolvedUnits) {
        const expectedMonthlyAmount = roundMoney(resolvedUnits * ContributionUnitBase);
        const sampleAmount = Object.values(row.contributions || {})
          .map((amount) => parseNumber(amount))
          .find((amount) => amount && amount > 0);
        if (
          sampleAmount &&
          roundMoney(sampleAmount) !== expectedMonthlyAmount
        ) {
          warningCollector.add("unit_amount_mismatch", {
            type: file.type,
            fileName: file.parsed.meta.fileName,
            rowIndex: row.rowIndex,
            memberSerial: normalizedSerial,
            units: resolvedUnits,
            expectedMonthlyAmount,
            sampleAmount: roundMoney(sampleAmount),
            message:
              "The monthly amount implied by the unit column differs from at least one monthly contribution value.",
          });
        }

        const existingSettings =
          profile?.contributionSettings && typeof profile.contributionSettings === "object"
            ? profile.contributionSettings
            : null;
        const normalizedExistingUnits = normalizeUnitsForYear(
          existingSettings?.units,
          parseNumber(existingSettings?.year),
          year,
        );
        const settingKey = [
          String(profile._id),
          String(group._id),
          file.contributionType,
          year,
        ].join("|");
        const previous = settingAggregates.get(settingKey);
        const chosenUnits =
          previous && previous.units !== resolvedUnits
            ? Math.max(previous.units, resolvedUnits)
            : resolvedUnits;

        if (previous && previous.units !== resolvedUnits) {
          warningCollector.add("conflicting_units_for_profile_type", {
            type: file.type,
            fileName: file.parsed.meta.fileName,
            rowIndex: row.rowIndex,
            memberSerial: normalizedSerial,
            previousUnits: previous.units,
            nextUnits: resolvedUnits,
            selectedUnits: chosenUnits,
            message:
              "Multiple rows resolved to the same profile/type/year with different unit values. The larger unit value was retained.",
          });
        }

        const nextUnits = {
          ...normalizedExistingUnits,
          [file.contributionType]: chosenUnits,
        };

        const aggregate = previous || {
          seedKey: `profile-setting-${file.type}-${profile._id}-${year}`,
          profileId: String(profile._id),
          profileSeedKey: buildProfileSeedKey(profile._id),
          groupId: String(group._id),
          groupSeedKey: buildGroupSeedKey(group.groupNumber, group._id),
          membershipId: String(membership._id),
          membershipSeedKey: buildMembershipSeedKey(membership._id),
          memberSerial: normalizedSerial,
          memberName: profile.fullName || row.name || null,
          type: file.type,
          contributionType: file.contributionType,
          year,
          units: chosenUnits,
          expectedMonthlyAmount: roundMoney(chosenUnits * ContributionUnitBase),
          monthsWithContributions: new Set(),
          sourceFiles: new Set(),
          currentContributionSettings: existingSettings || null,
          nextUnits,
        };

        aggregate.units = chosenUnits;
        aggregate.expectedMonthlyAmount = roundMoney(
          chosenUnits * ContributionUnitBase,
        );
        aggregate.nextUnits = nextUnits;
        aggregate.sourceFiles.add(file.parsed.meta.fileName);

        settingAggregates.set(settingKey, aggregate);
        profileSettingKeysTouched.add(settingKey);
      }

      for (const month of file.parsed.meta.monthsDetected) {
        const amount = parseNumber(row.contributions[month]);
        if (!amount || amount <= 0) continue;

        const contributionSeed = buildContributionSeed({
          row,
          month,
          amount,
          year,
          type: file.type,
          contributionType: file.contributionType,
          paymentDay,
          membership,
          profile,
          group,
          fileInfo: {
            fileName: file.parsed.meta.fileName,
            inputPath: file.parsed.meta.inputPath,
            sheetName: file.parsed.meta.sheetName,
            groupNumberFromFile: fileInfo.groupNumber,
            groupNameFromFile: fileInfo.groupName,
          },
        });

        const contributionKey = [
          contributionSeed.profileId,
          contributionSeed.groupId,
          contributionSeed.contributionType,
          contributionSeed.year,
          contributionSeed.month,
        ].join("|");

        if (contributionKeySet.has(contributionKey)) {
          warningCollector.add("duplicate_contribution_record", {
            type: file.type,
            fileName: file.parsed.meta.fileName,
            rowIndex: row.rowIndex,
            memberSerial: normalizedSerial,
            month,
            amount: contributionSeed.amount,
            message:
              "A duplicate contribution record key was encountered and skipped.",
          });
          continue;
        }

        contributionKeySet.add(contributionKey);
        contributions.push(contributionSeed);
        transactions.push(
          buildTransactionSeed({
            contribution: contributionSeed,
            type: file.type,
            contributionType: file.contributionType,
            group,
            fileInfo: {
              fileName: file.parsed.meta.fileName,
              groupNameFromFile: fileInfo.groupName,
            },
          }),
        );

        fileSummary.generatedContributions += 1;
        fileSummary.generatedTransactions += 1;

        if (resolvedUnits) {
          const settingKey = [
            String(profile._id),
            String(group._id),
            file.contributionType,
            year,
          ].join("|");
          const aggregate = settingAggregates.get(settingKey);
          if (aggregate) {
            aggregate.monthsWithContributions.add(month);
          }
        }
      }
    }

    fileSummary.generatedProfileContributionSettings =
      profileSettingKeysTouched.size;

    fileSummaries.push(fileSummary);
  }

  settingAggregates.forEach((_value, key) => {
    const aggregate = settingAggregates.get(key);
    if (!aggregate) return;
    profileContributionSettings.push(
      buildProfileContributionSettingSeed({
        aggregate,
        generatedAt,
      }),
    );
  });
  const meta = {
    generatedAt,
    year,
    paymentDay,
    inputDirectories: workbookConfigs.map((config) => ({
      type: config.type,
      contributionType: config.contributionType,
      inputDir: config.inputDir,
    })),
    outputDir,
    totals: {
      workbooks: parsedFiles.length,
      matchedSerials: membershipBySerial.size,
      contributions: contributions.length,
      transactions: transactions.length,
      profileContributionSettings: profileContributionSettings.length,
      warnings: warningCollector.all().length,
    },
    files: fileSummaries,
  };

  if (args["dry-run"]) {
    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          ok: 1,
          dryRun: true,
          outputDir,
          meta,
          sample: {
            contributions: contributions.slice(0, 3),
            transactions: transactions.slice(0, 3),
            profileContributionSettings: profileContributionSettings.slice(0, 3),
            warnings: warningCollector.all().slice(0, 10),
          },
        },
        null,
        2,
      ),
    );
    return;
  }

  await fs.mkdir(outputDir, { recursive: true });

  const contributionsPath = path.join(outputDir, "contributions.json");
  const transactionsPath = path.join(outputDir, "transactions.json");
  const profileContributionSettingsPath = path.join(
    outputDir,
    "profileContributionSettings.json",
  );
  const warningsPath = path.join(outputDir, "warnings.json");
  const metaPath = path.join(outputDir, "meta.json");

  await Promise.all([
    fs.writeFile(contributionsPath, `${JSON.stringify(contributions, null, 2)}\n`),
    fs.writeFile(transactionsPath, `${JSON.stringify(transactions, null, 2)}\n`),
    fs.writeFile(
      profileContributionSettingsPath,
      `${JSON.stringify(profileContributionSettings, null, 2)}\n`,
    ),
    fs.writeFile(
      warningsPath,
      `${JSON.stringify(warningCollector.all(), null, 2)}\n`,
    ),
    fs.writeFile(metaPath, `${JSON.stringify(meta, null, 2)}\n`),
  ]);

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: 1,
        outputDir,
        contributionsPath,
        transactionsPath,
        profileContributionSettingsPath,
        warningsPath,
        metaPath,
        summary: meta.totals,
      },
      null,
      2,
    ),
  );
}

main()
  .catch(async (error) => {
    const args = parseArgs(process.argv.slice(2));
    const outputDir = path.resolve(args.outputDir || DEFAULT_OUTPUT_DIR);
    const errorPayload = {
      ok: 0,
      error: error?.message || String(error),
      stack: error?.stack || null,
      generatedAt: new Date().toISOString(),
    };

    try {
      await fs.mkdir(outputDir, { recursive: true });
      const errorPath = path.join(
        outputDir,
        `error-${buildTimestamp()}.json`,
      );
      await fs.writeFile(errorPath, `${JSON.stringify(errorPayload, null, 2)}\n`);
      // eslint-disable-next-line no-console
      console.error(
        JSON.stringify(
          {
            ok: 0,
            error: errorPayload.error,
            errorPath,
          },
          null,
          2,
        ),
      );
    } catch (writeError) {
      // eslint-disable-next-line no-console
      console.error(JSON.stringify(errorPayload, null, 2));
      // eslint-disable-next-line no-console
      console.error(writeError);
    }
    process.exitCode = 1;
  })
  .finally(async () => {
    if (mongoose.connection.readyState !== 0) {
      await mongoose.disconnect();
    }
  });
