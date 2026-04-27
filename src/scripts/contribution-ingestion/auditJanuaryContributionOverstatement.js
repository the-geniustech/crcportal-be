import dotenv from "dotenv";
import mongoose from "mongoose";
import path from "path";
import { fileURLToPath } from "url";
import { connectMongo } from "../../db.js";
import {
  buildJanuaryContributionDiagnostics,
  buildRepairDeltaMaps,
  loadAffectedGroupDocs,
  loadAffectedMembershipDocs,
  loadAffectedSettingDocs,
  loadJanuaryContributionDbScope,
  aggregateCurrentGroupTotals,
  aggregateCurrentMembershipTotals,
  aggregateCurrentSettingTotals,
  applyDeltaToTotals,
  roundMoney,
  summarizeDiagnostics,
  deriveSettingCycleLength,
  toId,
  toNumber,
} from "./repairers/januaryContributionDbTools.js";
import { formatScriptError, parseArgs } from "../utils/userDataCleanup.js";
import {
  resolveDefaultContributionReportDir,
  writeStructuredReport,
} from "./utils/reportWriter.js";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDir = path.dirname(currentFilePath);
dotenv.config({ path: path.resolve(currentDir, "../../../.env") });
const args = parseArgs(process.argv.slice(2));
const defaultReportOutputDir = resolveDefaultContributionReportDir(import.meta.url);

function uniqueBy(items, selector) {
  const map = new Map();
  items.forEach((item) => {
    map.set(selector(item), item);
  });
  return Array.from(map.values());
}

function buildValueMismatch(expected, actual) {
  return {
    expected: roundMoney(expected),
    actual: roundMoney(actual),
    delta: roundMoney(toNumber(actual) - toNumber(expected)),
  };
}

function buildScopeLabel(groupNumbers) {
  const normalized = String(groupNumbers ?? "").trim();
  return normalized ? `groups-${normalized.replace(/[^0-9,]+/g, "-")}` : "all-groups";
}

async function executeAudit({ year, month, groupNumbers }) {
  const mongoUri = process.env.MONGO_URI;
  if (!mongoUri) {
    throw new Error("Missing MONGO_URI");
  }

  await connectMongo({ mongoUri });

  try {
    const scope = await loadJanuaryContributionDbScope({
      year,
      month,
      groupNumbers,
      session: null,
    });
    const diagnostics = buildJanuaryContributionDiagnostics(scope);
    const diagnosticsSummary = summarizeDiagnostics(diagnostics);
    const repairable = diagnostics.filter((item) => item.repairable);
    const safeSettingDiagnostics = diagnostics.filter(
      (item) =>
        item.expectedAmount !== null &&
        ["uncorrected", "clean_without_marker", "corrected"].includes(
          item.status,
        ),
    );
    const reviewOnly = diagnostics.filter((item) =>
      ["ambiguous", "mismatch"].includes(item.status),
    );

    const transactionMismatches = repairable
      .filter((item) => roundMoney(item.transactionAmount) !== roundMoney(item.expectedAmount))
      .map((item) => ({
        contributionId: item.contributionId,
        transactionId: item.transactionId,
        reference: item.transactionReference,
        ...buildValueMismatch(item.expectedAmount, item.transactionAmount),
      }));

    const membershipKeys = uniqueBy(
      repairable
        .filter((item) => item.counted)
        .map((item) => ({
          userId: item.userId,
          groupId: item.groupId,
        })),
      (item) => `${item.userId}|${item.groupId}`,
    );
    const groupIds = [...new Set(
      repairable
        .filter((item) => item.counted)
        .map((item) => item.groupId)
        .filter(Boolean),
    )];
    const settingKeys = uniqueBy(
      safeSettingDiagnostics
        .map((item) => ({
          userId: item.userId,
          groupId: item.groupId,
          year: item.year,
          contributionType: item.contributionType,
        })),
      (item) =>
        `${item.userId}|${item.groupId}|${item.year}|${item.contributionType}`,
    );
    const settingDiagnosticByKey = new Map(
      safeSettingDiagnostics.map((item) => [
        `${item.userId}|${item.groupId}|${item.year}|${item.contributionType}`,
        item,
      ]),
    );

    const { membershipDeltas, groupDeltas, settingDeltas } =
      buildRepairDeltaMaps(repairable);

    const [currentMembershipTotals, currentGroupTotals, currentSettingTotals] =
      await Promise.all([
        aggregateCurrentMembershipTotals(membershipKeys, null),
        aggregateCurrentGroupTotals(groupIds, null),
        aggregateCurrentSettingTotals(settingKeys, null),
      ]);

    const expectedMembershipTotals = applyDeltaToTotals(
      currentMembershipTotals,
      membershipDeltas,
    );
    const expectedGroupTotals = applyDeltaToTotals(currentGroupTotals, groupDeltas);
    const expectedSettingTotals = applyDeltaToTotals(
      currentSettingTotals,
      settingDeltas,
    );

    const [membershipDocs, groupDocs, settingDocs] = await Promise.all([
      loadAffectedMembershipDocs(membershipKeys, null),
      loadAffectedGroupDocs(groupIds, null),
      loadAffectedSettingDocs(settingKeys, null),
    ]);

    const membershipMismatches = membershipDocs
      .map((doc) => {
        const key = `${doc.userId}|${doc.groupId}`;
        const expected = roundMoney(expectedMembershipTotals.get(key));
        const actual = roundMoney(doc.totalContributed);
        if (expected === actual) return null;
        return {
          membershipId: toId(doc._id),
          userId: toId(doc.userId),
          groupId: toId(doc.groupId),
          ...buildValueMismatch(expected, actual),
        };
      })
      .filter(Boolean);

    const groupMismatches = groupDocs
      .map((doc) => {
        const expected = roundMoney(expectedGroupTotals.get(toId(doc._id)));
        const actual = roundMoney(doc.totalSavings);
        if (expected === actual) return null;
        return {
          groupId: toId(doc._id),
          groupNumber: Number(doc.groupNumber),
          groupName: doc.groupName,
          ...buildValueMismatch(expected, actual),
        };
      })
      .filter(Boolean);

    const settingMismatches = settingDocs
      .map((doc) => {
        const key = `${doc.userId}|${doc.groupId}|${doc.year}|${doc.contributionType}`;
        const diagnostic = settingDiagnosticByKey.get(key);
        if (!diagnostic || diagnostic.expectedAmount === null) {
          return null;
        }
        const expectedMonthlyAmount = roundMoney(diagnostic.expectedAmount);
        const cycleLength = deriveSettingCycleLength(doc, 12);
        const expectedTotalExpected = roundMoney(
          expectedMonthlyAmount * cycleLength,
        );
        const expectedTotalActual = roundMoney(expectedSettingTotals.get(key));
        const expectedOutstandingBalance = roundMoney(
          Math.max(expectedTotalExpected - expectedTotalActual, 0),
        );
        const actualTotalActual = roundMoney(doc.totalActual);
        const actualOutstandingBalance = roundMoney(doc.outstandingBalance);
        if (
          expectedMonthlyAmount === roundMoney(doc.expectedMonthlyAmount) &&
          expectedTotalExpected === roundMoney(doc.totalExpected) &&
          expectedTotalActual === actualTotalActual &&
          expectedOutstandingBalance === actualOutstandingBalance
        ) {
          return null;
        }
        return {
          contributionSettingId: toId(doc._id),
          userId: toId(doc.userId),
          groupId: toId(doc.groupId),
          year: Number(doc.year),
          contributionType: doc.contributionType,
          expectedMonthlyAmount: buildValueMismatch(
            expectedMonthlyAmount,
            doc.expectedMonthlyAmount,
          ),
          totalExpected: buildValueMismatch(
            expectedTotalExpected,
            doc.totalExpected,
          ),
          totalActual: buildValueMismatch(expectedTotalActual, actualTotalActual),
          outstandingBalance: buildValueMismatch(
            expectedOutstandingBalance,
            actualOutstandingBalance,
          ),
        };
      })
      .filter(Boolean);

    return {
      year,
      month,
      scope: {
        requestedGroupNumbers: scope.groupScope.requestedGroupNumbers,
        foundGroupNumbers: scope.groupScope.foundGroups.map((group) =>
          Number(group.groupNumber),
        ),
        missingGroupNumbers: scope.groupScope.missingGroupNumbers,
      },
      diagnostics: diagnosticsSummary,
      records: {
        uncorrected: diagnostics.filter((item) => item.status === "uncorrected"),
        driftedAfterRepair: diagnostics.filter(
          (item) => item.status === "drifted_after_repair",
        ),
        cleanWithoutMarker: diagnostics.filter(
          (item) => item.status === "clean_without_marker",
        ),
        corrected: diagnostics.filter((item) => item.status === "corrected"),
        reviewOnly: reviewOnly.map((item) => ({
          contributionId: item.contributionId,
          transactionId: item.transactionId,
          userId: item.userId,
          groupId: item.groupId,
          status: item.status,
          currentAmount: item.currentAmount,
          expectedAmount: item.expectedAmount,
          reason: item.expectedReason,
          expectedSource: item.expectedSource,
        })),
      },
      downstream: {
        transactions: {
          mismatchCount: transactionMismatches.length,
          mismatches: transactionMismatches,
        },
        groupMembers: {
          mismatchCount: membershipMismatches.length,
          mismatches: membershipMismatches,
        },
        groups: {
          mismatchCount: groupMismatches.length,
          mismatches: groupMismatches,
        },
        contributionSettings: {
          mismatchCount: settingMismatches.length,
          mismatches: settingMismatches,
        },
      },
    };
  } finally {
    await mongoose.disconnect();
  }
}

const runCli = async () => {
  if (args.help || args.h) {
    // eslint-disable-next-line no-console
    console.log(
      [
        "Usage:",
        "  node src/scripts/contribution-ingestion/auditJanuaryContributionOverstatement.js [--year <year>] [--month <month>] [--groupNumbers <csv>] [--outputDir <path>]",
        "",
        "What it audits directly in the database:",
        "  1. Seeded January contribution records that still match the doubled pattern",
        "  2. Seeded January transactions that do not match their corrected contribution amount",
        "  3. GroupMembership.totalContributed drift",
        "  4. Group.totalSavings drift",
        "  5. ContributionSetting.expectedMonthlyAmount / totalExpected / totalActual / outstandingBalance drift",
        "",
        "Safety rules:",
        "  The audit is read-only.",
        "  Only seeded migration transactions are inspected: type=group_contribution, channel=seed, gateway=internal.",
        "  Ambiguous records are reported separately and are not considered auto-repairable.",
      ].join("\n"),
    );
    process.exit(0);
  }

  const year = Number(args.year ?? 2026);
  const month = Number(args.month ?? 1);
  const groupNumbers = args.groupNumbers ?? args.groupNumber ?? "";
  const outputDir = args.outputDir ?? defaultReportOutputDir;

  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    throw new Error("Invalid --year");
  }
  if (!Number.isFinite(month) || month < 1 || month > 12) {
    throw new Error("Invalid --month");
  }

  const result = await executeAudit({
    year,
    month,
    groupNumbers,
  });

  const scopeLabel = buildScopeLabel(groupNumbers);
  const payload = { ok: 1, result };
  const reportPath = await writeStructuredReport({
    outputDir,
    prefix: "audit-january-contribution-overstatement",
    payload,
    year,
    month,
    scope: scopeLabel,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: 1,
        reportPath,
        outputDir,
        summary: {
          diagnostics: result.diagnostics,
          scope: result.scope,
          downstream: {
            transactions: result.downstream.transactions.mismatchCount,
            groupMembers: result.downstream.groupMembers.mismatchCount,
            groups: result.downstream.groups.mismatchCount,
            contributionSettings: result.downstream.contributionSettings.mismatchCount,
          },
          reviewOnlyCount: result.records.reviewOnly.length,
        },
      },
      null,
      2,
    ),
  );
};

runCli().catch((error) => {
  const year = Number(args.year ?? 2026);
  const month = Number(args.month ?? 1);
  const groupNumbers = args.groupNumbers ?? args.groupNumber ?? "";
  const outputDir = args.outputDir ?? defaultReportOutputDir;
  const payload = {
    ok: 0,
    error: formatScriptError(error),
    context: {
      year,
      month,
      groupNumbers,
    },
  };

  writeStructuredReport({
    outputDir,
    prefix: "audit-january-contribution-overstatement-error",
    payload,
    year: Number.isFinite(year) ? year : undefined,
    month: Number.isFinite(month) ? month : undefined,
    scope: buildScopeLabel(groupNumbers),
  })
    .then((reportPath) => {
      // eslint-disable-next-line no-console
      console.error(
        JSON.stringify(
          {
            ok: 0,
            error: payload.error,
            reportPath,
            outputDir,
          },
          null,
          2,
        ),
      );
      process.exit(1);
    })
    .catch(() => {
      // eslint-disable-next-line no-console
      console.error(JSON.stringify(payload, null, 2));
      process.exit(1);
    });
});
