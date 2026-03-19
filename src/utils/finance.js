import { TransactionModel } from "../models/Transaction.js";
import { WithdrawalRequestModel } from "../models/WithdrawalRequest.js";

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

