import { TransactionModel } from "../models/Transaction.js";
import { WithdrawalRequestModel } from "../models/WithdrawalRequest.js";
import mongoose from "mongoose";
import { ContributionModel } from "../models/Contribution.js";
import {
  getContributionTypeMatch,
  normalizeContributionType,
} from "./contributionPolicy.js";
import {
  computeInterestAllocation,
  getMonthlyInterestRates,
  resolveMonthsToCompute,
  roundMoney,
} from "./contributionInterest.js";

export async function computeSavingsBalances(profileId) {
  const [creditsAgg, debitsAgg, reservedAgg] = await Promise.all([
    TransactionModel.aggregate([
      {
        $match: {
          userId: profileId,
          status: "success",
          type: { $in: ["deposit", "interest"] },
        },
      },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
    TransactionModel.aggregate([
      {
        $match: {
          userId: profileId,
          status: "success",
          type: "withdrawal",
        },
      },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
    WithdrawalRequestModel.aggregate([
      {
        $match: {
          userId: profileId,
          status: { $in: ["pending", "approved", "processing"] },
        },
      },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
  ]);

  const credits = Number(creditsAgg?.[0]?.sum ?? 0);
  const debits = Number(debitsAgg?.[0]?.sum ?? 0);
  const reserved = Number(reservedAgg?.[0]?.sum ?? 0);

  const ledgerBalance = Math.max(0, credits - debits);
  const availableBalance = Math.max(0, ledgerBalance - reserved);

  return { ledgerBalance, availableBalance, reservedBalance: reserved };
}

export async function computeContributionBalances(
  profileId,
  { groupId = null, contributionType = null } = {},
) {
  const profileObjectId =
    profileId && mongoose.Types.ObjectId.isValid(profileId)
      ? new mongoose.Types.ObjectId(profileId)
      : profileId;
  const groupObjectId =
    groupId && mongoose.Types.ObjectId.isValid(groupId)
      ? new mongoose.Types.ObjectId(groupId)
      : groupId;

  const canonical =
    typeof contributionType === "string" && contributionType.trim()
      ? normalizeContributionType(contributionType)
      : null;
  const typeMatch = canonical
    ? getContributionTypeMatch(canonical) || [canonical]
    : null;

  const contributionMatch = {
    userId: profileObjectId,
    status: { $in: ["completed", "verified"] },
  };
  if (groupId) contributionMatch.groupId = groupObjectId;
  if (typeMatch) contributionMatch.contributionType = { $in: typeMatch };

  const withdrawalCompletedMatch = {
    userId: profileObjectId,
    status: "completed",
  };
  const withdrawalReservedMatch = {
    userId: profileObjectId,
    status: { $in: ["pending", "approved", "processing"] },
  };
  if (groupId) {
    withdrawalCompletedMatch.groupId = groupObjectId;
    withdrawalReservedMatch.groupId = groupObjectId;
  }
  if (typeMatch) {
    withdrawalCompletedMatch.contributionType = { $in: typeMatch };
    withdrawalReservedMatch.contributionType = { $in: typeMatch };
  }

  const [creditsAgg, debitsAgg, reservedAgg] = await Promise.all([
    ContributionModel.aggregate([
      { $match: contributionMatch },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
    WithdrawalRequestModel.aggregate([
      { $match: withdrawalCompletedMatch },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
    WithdrawalRequestModel.aggregate([
      { $match: withdrawalReservedMatch },
      { $group: { _id: null, sum: { $sum: "$amount" } } },
    ]),
  ]);

  const credits = Number(creditsAgg?.[0]?.sum ?? 0);
  const debits = Number(debitsAgg?.[0]?.sum ?? 0);
  const reserved = Number(reservedAgg?.[0]?.sum ?? 0);

  const interestEarned = await computeContributionInterestForUser(profileId, {
    groupId,
    contributionType,
  });

  const ledgerBalance = Math.max(0, credits + interestEarned - debits);
  const availableBalance = Math.max(0, ledgerBalance - reserved);

  return {
    ledgerBalance,
    availableBalance,
    reservedBalance: reserved,
    totalContributions: credits,
    totalWithdrawals: debits,
    totalInterest: interestEarned,
  };
}

async function computeContributionInterestForUser(
  profileId,
  { groupId = null, contributionType = null } = {},
) {
  if (!profileId) return 0;
  const now = new Date();
  const year = now.getFullYear();
  const monthsToCompute = resolveMonthsToCompute({ year, now });

  const groupObjectId =
    groupId && mongoose.Types.ObjectId.isValid(groupId)
      ? new mongoose.Types.ObjectId(groupId)
      : groupId;

  const canonical =
    typeof contributionType === "string" && contributionType.trim()
      ? normalizeContributionType(contributionType)
      : null;
  const typeMatch = canonical
    ? getContributionTypeMatch(canonical) || [canonical]
    : null;

  const match = {
    year,
    status: { $in: ["completed", "verified"] },
  };
  if (groupId) match.groupId = groupObjectId;
  if (typeMatch) {
    if (canonical === "revolving") {
      match.$or = [
        { contributionType: { $in: typeMatch } },
        { contributionType: { $exists: false } },
        { contributionType: null },
      ];
    } else {
      match.contributionType = { $in: typeMatch };
    }
  }

  const contributions = await ContributionModel.find(match, {
    userId: 1,
    groupId: 1,
    month: 1,
    amount: 1,
  }).lean();

  if (!contributions.length) return 0;

  const contributionsByGroup = new Map();
  contributions.forEach((contribution) => {
    const gid = contribution.groupId ? String(contribution.groupId) : "global";
    if (!contributionsByGroup.has(gid)) {
      contributionsByGroup.set(gid, new Map());
    }
    const byMember = contributionsByGroup.get(gid);
    const memberId = String(contribution.userId || "");
    if (!memberId) return;
    if (!byMember.has(memberId)) {
      byMember.set(memberId, Array(12).fill(0));
    }
    const months = byMember.get(memberId);
    const monthIdx = Number(contribution.month) - 1;
    if (!Number.isFinite(monthIdx) || monthIdx < 0 || monthIdx > 11) return;
    months[monthIdx] = Number(months[monthIdx] ?? 0) + Number(contribution.amount ?? 0);
    byMember.set(memberId, months);
  });

  const monthlyRates = await getMonthlyInterestRates(year);
  let totalInterest = 0;

  contributionsByGroup.forEach((byMember) => {
    const { memberInterestByMonth } = computeInterestAllocation({
      contributionsByMember: byMember,
      monthlyRates,
      monthsToCompute,
    });
    const memberInterest = memberInterestByMonth.get(String(profileId)) || [];
    totalInterest += memberInterest.reduce(
      (sum, value) => sum + Number(value ?? 0),
      0,
    );
  });

  return roundMoney(totalInterest);
}

export async function sumDepositsForMonth(profileId, year, month1to12) {
  const start = new Date(Date.UTC(year, month1to12 - 1, 1, 0, 0, 0));
  const end = new Date(Date.UTC(year, month1to12, 1, 0, 0, 0));

  const agg = await TransactionModel.aggregate([
    {
      $match: {
        userId: profileId,
        status: "success",
        type: "deposit",
        date: { $gte: start, $lt: end },
      },
    },
    { $group: { _id: null, sum: { $sum: "$amount" } } },
  ]);

  return Number(agg?.[0]?.sum ?? 0);
}

export async function sumInterestAllTime(profileId) {
  const agg = await TransactionModel.aggregate([
    {
      $match: {
        userId: profileId,
        status: "success",
        type: "interest",
      },
    },
    { $group: { _id: null, sum: { $sum: "$amount" } } },
  ]);

  return Number(agg?.[0]?.sum ?? 0);
}
