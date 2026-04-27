import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import { ContributionModel } from "../../models/Contribution.js";
import { ContributionSettingModel } from "../../models/ContributionSetting.js";
import { GroupMembershipModel } from "../../models/GroupMembership.js";
import { GroupModel } from "../../models/Group.js";
import { TransactionModel } from "../../models/Transaction.js";
import {
  formatScriptError,
  mongoOptions,
  parseArgs,
  runWithOptionalTransaction,
} from "../utils/userDataCleanup.js";
import {
  aggregateCurrentGroupTotals,
  aggregateCurrentMembershipTotals,
  aggregateCurrentSettingTotals,
  applyDeltaToTotals,
  buildJanuaryContributionDiagnostics,
  buildRepairDeltaMaps,
  buildTransactionMetadataWithRepairMarker,
  deriveSettingCycleLength,
  loadAffectedGroupDocs,
  loadAffectedMembershipDocs,
  loadAffectedSettingDocs,
  loadJanuaryContributionDbScope,
  roundMoney,
  summarizeDiagnostics,
  toId,
  toNumber,
} from "./repairers/januaryContributionDbTools.js";
import {
  resolveDefaultContributionReportDir,
  writeStructuredReport,
} from "./utils/reportWriter.js";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDir = path.dirname(currentFilePath);
dotenv.config({ path: path.resolve(currentDir, "../../../.env") });

function uniqueBy(items, selector) {
  const map = new Map();
  items.forEach((item) => {
    map.set(selector(item), item);
  });
  return Array.from(map.values());
}

const args = parseArgs(process.argv.slice(2));
const defaultReportOutputDir = resolveDefaultContributionReportDir(import.meta.url);

function buildScopeLabel(groupNumbers) {
  const normalized = String(groupNumbers ?? "").trim();
  return normalized ? `groups-${normalized.replace(/[^0-9,]+/g, "-")}` : "all-groups";
}

async function executeRepair({
  year,
  month,
  groupNumbers,
  dryRun = false,
  useTransaction = true,
}) {
  const correctedAt = new Date().toISOString();

  return runWithOptionalTransaction({
    useTransaction,
    work: async (session) => {
      const scope = await loadJanuaryContributionDbScope({
        year,
        month,
        groupNumbers,
        session,
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
      const skipped = diagnostics.filter((item) =>
        ["ambiguous", "mismatch"].includes(item.status),
      );

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
          aggregateCurrentMembershipTotals(membershipKeys, session),
          aggregateCurrentGroupTotals(groupIds, session),
          aggregateCurrentSettingTotals(settingKeys, session),
        ]);

      const expectedMembershipTotals = applyDeltaToTotals(
        currentMembershipTotals,
        membershipDeltas,
      );
      const expectedGroupTotals = applyDeltaToTotals(
        currentGroupTotals,
        groupDeltas,
      );
      const expectedSettingTotals = applyDeltaToTotals(
        currentSettingTotals,
        settingDeltas,
      );

      const [membershipDocs, groupDocs, settingDocs] = await Promise.all([
        loadAffectedMembershipDocs(membershipKeys, session),
        loadAffectedGroupDocs(groupIds, session),
        loadAffectedSettingDocs(settingKeys, session),
      ]);

      const transactionByContributionId = scope.transactionByContributionId;

      const contributionOps = repairable.map((item) => ({
        updateOne: {
          filter: { _id: item.contributionId },
          update: {
            $set: {
              amount: roundMoney(item.expectedAmount),
            },
          },
        },
      }));

      const transactionOps = repairable
        .map((item) => {
          const transaction = transactionByContributionId.get(item.contributionId);
          if (!transaction) return null;
          return {
            updateOne: {
              filter: { _id: transaction._id },
              update: {
                $set: {
                  amount: roundMoney(item.expectedAmount),
                  metadata: buildTransactionMetadataWithRepairMarker(transaction, {
                    correctedAt,
                    originalAmount: item.currentAmount,
                    expectedAmount: item.expectedAmount,
                    year,
                    month,
                  }),
                },
              },
            },
          };
        })
        .filter(Boolean);

      const membershipOps = membershipDocs
        .map((doc) => {
          const key = `${doc.userId}|${doc.groupId}`;
          const expected = roundMoney(expectedMembershipTotals.get(key));
          if (roundMoney(doc.totalContributed) === expected) return null;
          return {
            updateOne: {
              filter: { _id: doc._id },
              update: {
                $set: {
                  totalContributed: expected,
                },
              },
            },
          };
        })
        .filter(Boolean);

      const groupOps = groupDocs
        .map((doc) => {
          const expected = roundMoney(expectedGroupTotals.get(toId(doc._id)));
          if (roundMoney(doc.totalSavings) === expected) return null;
          return {
            updateOne: {
              filter: { _id: doc._id },
              update: {
                $set: {
                  totalSavings: expected,
                },
              },
            },
          };
        })
        .filter(Boolean);

      const settingOps = settingDocs
        .map((doc) => {
          const key = `${doc.userId}|${doc.groupId}|${doc.year}|${doc.contributionType}`;
          const diagnostic = settingDiagnosticByKey.get(key);
          if (!diagnostic || diagnostic.expectedAmount === null) {
            return null;
          }

          const expectedMonthlyAmount = roundMoney(diagnostic.expectedAmount);
          const cycleLength = deriveSettingCycleLength(doc, 12);
          const totalExpected = roundMoney(expectedMonthlyAmount * cycleLength);
          const totalActual = roundMoney(expectedSettingTotals.get(key));
          const outstandingBalance = roundMoney(
            Math.max(totalExpected - totalActual, 0),
          );
          if (
            roundMoney(doc.expectedMonthlyAmount) === expectedMonthlyAmount &&
            roundMoney(doc.totalExpected) === totalExpected &&
            roundMoney(doc.totalActual) === totalActual &&
            roundMoney(doc.outstandingBalance) === outstandingBalance
          ) {
            return null;
          }
          return {
            updateOne: {
              filter: { _id: doc._id },
              update: {
                $set: {
                  expectedMonthlyAmount,
                  totalExpected,
                  totalActual,
                  outstandingBalance,
                },
              },
            },
          };
        })
        .filter(Boolean);

      const result = {
        year,
        month,
        correctedAt,
        scope: {
          requestedGroupNumbers: scope.groupScope.requestedGroupNumbers,
          foundGroupNumbers: scope.groupScope.foundGroups.map((group) =>
            Number(group.groupNumber),
          ),
          missingGroupNumbers: scope.groupScope.missingGroupNumbers,
        },
        diagnostics: diagnosticsSummary,
        repairPlan: {
          contributions: contributionOps.length,
          transactions: transactionOps.length,
          groupMembers: membershipOps.length,
          groups: groupOps.length,
          contributionSettings: settingOps.length,
          skipped: skipped.length,
        },
        skippedRecords: skipped.map((item) => ({
          contributionId: item.contributionId,
          transactionId: item.transactionId,
          userId: item.userId,
          groupId: item.groupId,
          status: item.status,
          currentAmount: item.currentAmount,
          expectedAmount: item.expectedAmount,
          groupExpectedAmount: item.groupExpectedAmount,
          siblingExpectedAmount: item.siblingExpectedAmount,
          settingExpectedAmount: item.settingExpectedAmount,
          reason: item.expectedReason,
          expectedSource: item.expectedSource,
        })),
        dryRun,
        repairOutcome: dryRun
          ? {
              status: "dry_run",
              message:
                repairable.length > 0
                  ? "Dry run completed. No database writes were made."
                  : "Dry run completed. No safe doubled January records were found in the selected database scope.",
            }
          : repairable.length > 0
            ? {
                status: "ready_to_apply",
                message: "Safe doubled January records were found and will be repaired in this run.",
              }
            : {
                status: "no_safe_changes",
                message:
                  "No safe doubled January records were found in the selected database scope, so no database changes were applied.",
              },
      };

      if (dryRun || repairable.length === 0) {
        return result;
      }

      if (contributionOps.length > 0) {
        await ContributionModel.bulkWrite(contributionOps, {
          ordered: false,
          ...mongoOptions(session),
        });
      }
      if (transactionOps.length > 0) {
        await TransactionModel.bulkWrite(transactionOps, {
          ordered: false,
          ...mongoOptions(session),
        });
      }
      if (membershipOps.length > 0) {
        await GroupMembershipModel.bulkWrite(membershipOps, {
          ordered: false,
          ...mongoOptions(session),
        });
      }
      if (groupOps.length > 0) {
        await GroupModel.bulkWrite(groupOps, {
          ordered: false,
          ...mongoOptions(session),
        });
      }
      if (settingOps.length > 0) {
        await ContributionSettingModel.bulkWrite(settingOps, {
          ordered: false,
          ...mongoOptions(session),
        });
      }

      return {
        ...result,
        repairOutcome: {
          status: "applied",
          message: "January contribution repair changes were applied successfully to the selected database scope.",
        },
        applied: {
          repairedContributions: contributionOps.length,
          repairedTransactions: transactionOps.length,
          repairedGroupMembers: membershipOps.length,
          repairedGroups: groupOps.length,
          repairedContributionSettings: settingOps.length,
        },
      };
    },
  });
}

const runCli = async () => {
  if (args.help || args.h) {
    // eslint-disable-next-line no-console
    console.log(
      [
        "Usage:",
        "  node src/scripts/contribution-ingestion/repairJanuaryContributionOverstatement.js [--year <year>] [--month <month>] [--groupNumbers <csv>] [--dry-run] [--no-transaction] [--outputDir <path>]",
        "",
        "What it repairs directly in the database:",
        "  1. Seeded January Contribution.amount records",
        "  2. Matching seed Transaction.amount records",
        "  3. GroupMembership.totalContributed",
        "  4. Group.totalSavings",
        "  5. ContributionSetting.expectedMonthlyAmount / totalExpected / totalActual / outstandingBalance",
        "",
        "Safety rules:",
        "  Only seeded migration transactions are considered: type=group_contribution, channel=seed, gateway=internal.",
        "  Records are repaired only when the expected amount can be derived safely and the current amount matches the doubled pattern.",
        "  Ambiguous or mismatched records are reported and skipped.",
      ].join("\n"),
    );
    process.exit(0);
  }

  const year = Number(args.year ?? 2026);
  const month = Number(args.month ?? 1);
  const groupNumbers = args.groupNumbers ?? args.groupNumber ?? "";
  const dryRun = Boolean(args["dry-run"]);
  const useTransaction = !Boolean(args["no-transaction"]);
  const outputDir = args.outputDir ?? defaultReportOutputDir;

  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    throw new Error("Invalid --year");
  }
  if (!Number.isFinite(month) || month < 1 || month > 12) {
    throw new Error("Invalid --month");
  }

  const result = await executeRepair({
    year,
    month,
    groupNumbers,
    dryRun,
    useTransaction,
  });

  const scopeLabel = buildScopeLabel(groupNumbers);
  const payload = { ok: 1, result };
  const reportPath = await writeStructuredReport({
    outputDir,
    prefix: dryRun
      ? "repair-january-contribution-overstatement-dry-run"
      : "repair-january-contribution-overstatement",
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
          repairPlan: result.repairPlan,
          repairOutcome: result.repairOutcome,
          scope: result.scope,
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
    prefix: "repair-january-contribution-overstatement-error",
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
