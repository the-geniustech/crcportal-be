import AppError from "../utils/AppError.js";

import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { TransactionModel } from "../models/Transaction.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";

function roundCurrency(value) {
  return Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;
}

function parseDate(value, fallback = null) {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return date;
}

function addMonths(date, months) {
  const next = new Date(date);
  const day = next.getDate();
  next.setMonth(next.getMonth() + months);
  if (next.getDate() < day) next.setDate(0);
  return next;
}

function addFrequency(date, frequency) {
  const next = new Date(date);
  if (frequency === "weekly") next.setDate(next.getDate() + 7);
  else if (frequency === "bi-weekly") next.setDate(next.getDate() + 14);
  else next.setMonth(next.getMonth() + 1);
  return next;
}

function isMongooseDocument(value) {
  return Boolean(value && typeof value.save === "function");
}

function isoDateKey(value) {
  const date = parseDate(value, null);
  if (!date) return "";
  return date.toISOString().slice(0, 10);
}

function sortScheduleItems(a, b) {
  const dateA = parseDate(a?.dueDate, new Date(0)).getTime();
  const dateB = parseDate(b?.dueDate, new Date(0)).getTime();
  if (dateA !== dateB) return dateA - dateB;
  return Number(a?.installmentNumber ?? 0) - Number(b?.installmentNumber ?? 0);
}

function getLoanOriginalPrincipal(application) {
  return Math.max(
    0,
    roundCurrency(application?.approvedAmount ?? application?.loanAmount ?? 0),
  );
}

function getLoanMonthlyRate(application) {
  const rate = Number(
    application?.approvedInterestRate ?? application?.interestRate ?? 0,
  );
  if (!Number.isFinite(rate) || rate <= 0) return 0;

  const rateType = String(application?.interestRateType || "annual")
    .trim()
    .toLowerCase();
  const termMonths = Math.max(1, Number(application?.repaymentPeriod ?? 1));

  if (rateType === "monthly") return rate / 100;
  if (rateType === "total") return rate / 100 / termMonths;
  return rate / 100 / 12;
}

function getActualPaidInterest(item) {
  const explicit = Number(item?.paidInterestAmount);
  if (Number.isFinite(explicit) && explicit >= 0) {
    return roundCurrency(explicit);
  }
  const paidAmount = roundCurrency(item?.paidAmount ?? 0);
  const interestAmount = roundCurrency(item?.interestAmount ?? 0);
  return Math.min(paidAmount, interestAmount);
}

function getActualPaidPrincipal(item) {
  const explicit = Number(item?.paidPrincipalAmount);
  if (Number.isFinite(explicit) && explicit >= 0) {
    return roundCurrency(explicit);
  }
  const paidAmount = roundCurrency(item?.paidAmount ?? 0);
  const interestPaid = getActualPaidInterest(item);
  const principalAmount = roundCurrency(item?.principalAmount ?? 0);
  return Math.min(principalAmount, Math.max(0, paidAmount - interestPaid));
}

export function getLoanScheduleOutstandingInterestAmount(item) {
  const interest = roundCurrency(item?.interestAmount ?? 0);
  const paid = getActualPaidInterest(item);
  return Math.max(0, roundCurrency(interest - paid));
}

export function getLoanScheduleOutstandingPrincipalAmount(item) {
  const principal = roundCurrency(item?.principalAmount ?? 0);
  const paid = getActualPaidPrincipal(item);
  return Math.max(0, roundCurrency(principal - paid));
}

export function getLoanScheduleOutstandingAmount(item) {
  return roundCurrency(
    getLoanScheduleOutstandingInterestAmount(item) +
      getLoanScheduleOutstandingPrincipalAmount(item),
  );
}

function getScheduleStatusForDueDate(dueDate, now, { projected = false } = {}) {
  if (projected) return "upcoming";
  if (dueDate.getTime() < now.getTime()) return "overdue";
  return "pending";
}

function getScheduledPrincipalDue(outstandingPrincipal, remainingTermCycles) {
  const principal = Math.max(0, roundCurrency(outstandingPrincipal));
  if (principal <= 0) return 0;
  if (!Number.isFinite(remainingTermCycles) || remainingTermCycles <= 1) {
    return principal;
  }
  return roundCurrency(principal / remainingTermCycles);
}

function buildProjectedInstallment({
  loanApplicationId,
  installmentNumber,
  dueDate,
  openingPrincipalBalance,
  monthlyRate,
  remainingTermCycles,
}) {
  const openingPrincipal = Math.max(
    0,
    roundCurrency(openingPrincipalBalance ?? 0),
  );
  const principalAmount = getScheduledPrincipalDue(
    openingPrincipal,
    remainingTermCycles,
  );
  const interestAmount = roundCurrency(openingPrincipal * monthlyRate);

  return {
    loanApplicationId,
    installmentNumber,
    dueDate,
    openingPrincipalBalance: openingPrincipal,
    principalAmount,
    interestAmount,
    totalAmount: roundCurrency(principalAmount + interestAmount),
    paidPrincipalAmount: 0,
    paidInterestAmount: 0,
    paidAmount: 0,
    status: "upcoming",
    isProjected: true,
    paidAt: null,
    transactionId: null,
    reference: null,
  };
}

function projectNextInstallment({
  application,
  actualCycleCount,
  dueDate,
  principalOutstanding,
}) {
  const monthlyRate = getLoanMonthlyRate(application);
  const remainingTermCycles = Math.max(
    Number(application?.repaymentPeriod ?? 0) - actualCycleCount,
    0,
  );
  const installmentNumber = actualCycleCount + 1;
  const projected = buildProjectedInstallment({
    loanApplicationId: application?._id,
    installmentNumber,
    dueDate,
    openingPrincipalBalance: principalOutstanding,
    monthlyRate,
    remainingTermCycles,
  });

  return {
    ...projected,
    amountDue: projected.totalAmount,
  };
}

function buildProjectedScheduleData(application, { actualCycleCount }) {
  const principalOutstanding = Math.max(
    0,
    roundCurrency(application?.principalOutstanding ?? 0),
  );
  const startDate = parseDate(application?.nextInterestAccrualDate, null);
  if (principalOutstanding <= 0 || !startDate) return [];

  const remainingTermCycles = Math.max(
    Number(application?.repaymentPeriod ?? 0) - actualCycleCount,
    0,
  );
  const projectionCount = remainingTermCycles > 0 ? remainingTermCycles : 1;
  const monthlyRate = getLoanMonthlyRate(application);
  let projectedPrincipal = principalOutstanding;

  return Array.from({ length: projectionCount }, (_value, index) => {
    const installmentNumber = actualCycleCount + index + 1;
    const dueDate = addMonths(startDate, index);
    const cyclesRemaining = Math.max(
      Number(application?.repaymentPeriod ?? 0) - (actualCycleCount + index),
      0,
    );
    const projected = buildProjectedInstallment({
      loanApplicationId: application?._id,
      installmentNumber,
      dueDate,
      openingPrincipalBalance: projectedPrincipal,
      monthlyRate,
      remainingTermCycles: cyclesRemaining,
    });
    projectedPrincipal = Math.max(
      0,
      roundCurrency(projectedPrincipal - projected.principalAmount),
    );
    return projected;
  });
}

function buildProjectionSignature(items = []) {
  return items
    .map((item) =>
      [
        Number(item.installmentNumber ?? 0),
        isoDateKey(item.dueDate),
        roundCurrency(item.openingPrincipalBalance ?? 0),
        roundCurrency(item.principalAmount ?? 0),
        roundCurrency(item.interestAmount ?? 0),
      ].join(":"),
    )
    .join("|");
}

function pickOutstandingScheduleItem(items = []) {
  const now = new Date();
  const sorted = [...items].sort(sortScheduleItems);
  return (
    sorted.find((item) => getLoanScheduleOutstandingAmount(item) > 0) || null
  );
}

function recalculateApplicationBalances(application) {
  const originalPrincipal = getLoanOriginalPrincipal(application);
  const principalOutstanding = Math.max(
    0,
    roundCurrency(application?.principalOutstanding ?? 0),
  );
  const accruedInterestBalance = Math.max(
    0,
    roundCurrency(application?.accruedInterestBalance ?? 0),
  );
  const totalPrincipalPaid = Math.max(
    0,
    roundCurrency(application?.totalPrincipalPaid ?? 0),
  );
  const totalInterestPaid = Math.max(
    0,
    roundCurrency(application?.totalInterestPaid ?? 0),
  );
  const totalInterestAccrued = Math.max(
    totalInterestPaid + accruedInterestBalance,
    roundCurrency(application?.totalInterestAccrued ?? 0),
  );
  const remainingBalance = roundCurrency(
    principalOutstanding + accruedInterestBalance,
  );
  const totalRepayable = roundCurrency(originalPrincipal + totalInterestAccrued);

  application.principalOutstanding = principalOutstanding;
  application.accruedInterestBalance = accruedInterestBalance;
  application.totalPrincipalPaid = totalPrincipalPaid;
  application.totalInterestPaid = totalInterestPaid;
  application.totalInterestAccrued = totalInterestAccrued;
  application.remainingBalance = remainingBalance;
  application.totalRepayable = totalRepayable;

  if (remainingBalance <= 0) {
    application.remainingBalance = 0;
    application.principalOutstanding = 0;
    application.accruedInterestBalance = 0;
    application.monthlyPayment = 0;
    application.nextInterestAccrualDate = null;
    if (String(application.status) !== "completed") {
      application.status = "completed";
    }
  } else if (String(application.status) === "completed") {
    application.status = "disbursed";
  }
}

function attachNormalizedPaymentFields(item) {
  const interestPaid = getActualPaidInterest(item);
  const principalPaid = getActualPaidPrincipal(item);
  item.paidInterestAmount = interestPaid;
  item.paidPrincipalAmount = principalPaid;
  item.paidAmount = roundCurrency(interestPaid + principalPaid);
}

async function initializeLegacyLoanRepaymentState(
  application,
  scheduleItems,
  asOfDate,
) {
  const originalPrincipal = getLoanOriginalPrincipal(application);
  const now = parseDate(asOfDate, new Date()) || new Date();
  const sortedItems = [...scheduleItems].sort(sortScheduleItems);

  let runningPrincipal = originalPrincipal;
  let totalPrincipalPaid = 0;
  let totalInterestPaid = 0;
  let totalInterestAccrued = 0;
  let accruedInterestBalance = 0;
  let nextInterestAccrualDate = null;
  let lastKnownDueDate = parseDate(
    application?.repaymentStartDate || application?.disbursedAt,
    null,
  );
  let itemsChanged = false;

  for (const item of sortedItems) {
    const dueDate = parseDate(item?.dueDate, null);
    if (dueDate) {
      lastKnownDueDate = dueDate;
    }

    if (
      item?.openingPrincipalBalance === null ||
      typeof item?.openingPrincipalBalance === "undefined"
    ) {
      item.openingPrincipalBalance = Math.max(0, roundCurrency(runningPrincipal));
      itemsChanged = true;
    }

    attachNormalizedPaymentFields(item);
    const principalPaid = roundCurrency(item.paidPrincipalAmount ?? 0);
    const interestPaid = roundCurrency(item.paidInterestAmount ?? 0);
    totalPrincipalPaid = roundCurrency(totalPrincipalPaid + principalPaid);
    totalInterestPaid = roundCurrency(totalInterestPaid + interestPaid);
    runningPrincipal = Math.max(0, roundCurrency(runningPrincipal - principalPaid));

    const isPastOrCurrentCycle =
      dueDate && dueDate.getTime() <= now.getTime()
        ? true
        : String(item.status) === "paid" || String(item.status) === "overdue";

    if (isPastOrCurrentCycle) {
      if (item.isProjected) {
        item.isProjected = false;
        itemsChanged = true;
      }
      totalInterestAccrued = roundCurrency(
        totalInterestAccrued + Number(item.interestAmount ?? 0),
      );
      accruedInterestBalance = roundCurrency(
        accruedInterestBalance + getLoanScheduleOutstandingInterestAmount(item),
      );
    } else {
      if (!item.isProjected) {
        item.isProjected = true;
        itemsChanged = true;
      }
      const itemDueDate = parseDate(item.dueDate, null);
      if (
        itemDueDate &&
        (!nextInterestAccrualDate || itemDueDate < nextInterestAccrualDate)
      ) {
        nextInterestAccrualDate = itemDueDate;
      }
    }
  }

  const principalOutstanding = Math.max(
    0,
    roundCurrency(originalPrincipal - totalPrincipalPaid),
  );
  totalInterestAccrued = Math.max(
    roundCurrency(totalInterestPaid + accruedInterestBalance),
    totalInterestAccrued,
  );

  if (!nextInterestAccrualDate && principalOutstanding > 0) {
    const fallbackBase =
      lastKnownDueDate ||
      parseDate(application?.repaymentStartDate || application?.disbursedAt, null) ||
      new Date();
    nextInterestAccrualDate = parseDate(
      application?.repaymentStartDate,
      addMonths(fallbackBase, lastKnownDueDate ? 1 : 0),
    );
    while (
      nextInterestAccrualDate &&
      nextInterestAccrualDate.getTime() <= now.getTime() &&
      principalOutstanding > 0
    ) {
      nextInterestAccrualDate = addMonths(nextInterestAccrualDate, 1);
    }
  }

  application.principalOutstanding = principalOutstanding;
  application.accruedInterestBalance = accruedInterestBalance;
  application.totalPrincipalPaid = totalPrincipalPaid;
  application.totalInterestPaid = totalInterestPaid;
  application.totalInterestAccrued = totalInterestAccrued;
  application.nextInterestAccrualDate =
    principalOutstanding > 0 ? nextInterestAccrualDate : null;

  recalculateApplicationBalances(application);

  if (itemsChanged) {
    await Promise.all(sortedItems.map((item) => item.save()));
  }
}

async function materializeDueCycles(application, scheduleItems, asOfDate) {
  const now = parseDate(asOfDate, new Date()) || new Date();
  const sorted = [...scheduleItems].sort(sortScheduleItems);
  const projectedItems = sorted.filter((item) => item.isProjected);
  const actualItems = sorted.filter((item) => !item.isProjected);
  const convertedItems = [];
  let changed = false;

  while (
    application.principalOutstanding > 0 &&
    application.nextInterestAccrualDate &&
    parseDate(application.nextInterestAccrualDate, null) &&
    parseDate(application.nextInterestAccrualDate, null).getTime() <=
      now.getTime()
  ) {
    const dueDate = parseDate(application.nextInterestAccrualDate, now);
    const actualCycleCount = actualItems.length;
    const nextInstallmentNumber = actualCycleCount + 1;
    const nextProjectionIndex = projectedItems.findIndex(
      (item) =>
        Number(item.installmentNumber ?? 0) === nextInstallmentNumber ||
        isoDateKey(item.dueDate) === isoDateKey(dueDate),
    );
    const item =
      nextProjectionIndex >= 0
        ? projectedItems.splice(nextProjectionIndex, 1)[0]
        : new LoanRepaymentScheduleItemModel({
            loanApplicationId: application._id,
          });

    const openingPrincipal = Math.max(
      0,
      roundCurrency(application.principalOutstanding ?? 0),
    );
    const remainingTermCycles = Math.max(
      Number(application.repaymentPeriod ?? 0) - actualCycleCount,
      0,
    );
    const interestAmount = roundCurrency(
      openingPrincipal * getLoanMonthlyRate(application),
    );
    const principalAmount = getScheduledPrincipalDue(
      openingPrincipal,
      remainingTermCycles,
    );

    item.loanApplicationId = application._id;
    item.installmentNumber = nextInstallmentNumber;
    item.dueDate = dueDate;
    item.openingPrincipalBalance = openingPrincipal;
    item.principalAmount = principalAmount;
    item.interestAmount = interestAmount;
    item.totalAmount = roundCurrency(principalAmount + interestAmount);
    item.paidPrincipalAmount = roundCurrency(item.paidPrincipalAmount ?? 0);
    item.paidInterestAmount = roundCurrency(item.paidInterestAmount ?? 0);
    item.paidAmount = roundCurrency(
      Number(item.paidPrincipalAmount ?? 0) + Number(item.paidInterestAmount ?? 0),
    );
    item.status = getScheduleStatusForDueDate(dueDate, now, {
      projected: false,
    });
    item.isProjected = false;

    application.accruedInterestBalance = roundCurrency(
      Number(application.accruedInterestBalance ?? 0) + interestAmount,
    );
    application.totalInterestAccrued = roundCurrency(
      Number(application.totalInterestAccrued ?? 0) + interestAmount,
    );
    application.nextInterestAccrualDate = addMonths(dueDate, 1);

    actualItems.push(item);
    convertedItems.push(item);
    changed = true;
  }

  actualItems.sort(sortScheduleItems);
  for (const item of actualItems) {
    attachNormalizedPaymentFields(item);
    const outstanding = getLoanScheduleOutstandingAmount(item);
    if (outstanding <= 0) {
      item.status = "paid";
      if (!item.paidAt) item.paidAt = item.updatedAt || new Date();
      continue;
    }

    const dueDate = parseDate(item.dueDate, now);
    item.status = getScheduleStatusForDueDate(dueDate, now, {
      projected: false,
    });
  }

  return { changed, actualItems, projectedItems, convertedItems };
}

async function rebuildProjectedSchedule(application, actualItems) {
  if (!application?._id) return { changed: false, projectionData: [] };

  const actualCycleCount = Array.isArray(actualItems) ? actualItems.length : 0;
  const projectionData = buildProjectedScheduleData(application, {
    actualCycleCount,
  });

  const existingProjectedItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: application._id,
    isProjected: true,
  }).sort({ installmentNumber: 1, dueDate: 1 });

  const existingSignature = buildProjectionSignature(existingProjectedItems);
  const nextSignature = buildProjectionSignature(projectionData);

  if (existingSignature === nextSignature) {
    return { changed: false, projectionData };
  }

  await LoanRepaymentScheduleItemModel.deleteMany({
    loanApplicationId: application._id,
    isProjected: true,
  });

  if (projectionData.length > 0) {
    await LoanRepaymentScheduleItemModel.insertMany(projectionData, {
      ordered: true,
    });
  }

  return { changed: true, projectionData };
}

async function syncLoanRecurringPaymentStats({
  userId,
  loanId,
  amount,
  paidAt,
  settledInstallmentCount,
} = {}) {
  if (!userId || !loanId) return;

  const target = await RecurringPaymentModel.findOne({
    userId,
    paymentType: "loan_repayment",
    loanId,
    isActive: true,
  }).sort({ nextPaymentDate: 1, createdAt: 1 });

  if (!target) return;

  const paidAtDate = parseDate(paidAt, new Date()) || new Date();
  target.totalPaymentsMade = Number(target.totalPaymentsMade ?? 0) + 1;
  target.totalAmountPaid =
    roundCurrency(target.totalAmountPaid ?? 0) + roundCurrency(amount ?? 0);
  target.lastPaymentDate = paidAtDate;
  target.lastPaymentStatus = "success";

  const completedCount = Math.max(0, Number(settledInstallmentCount || 0));
  if (completedCount > 0) {
    let nextDate =
      parseDate(target.nextPaymentDate, null) ||
      parseDate(target.startDate, null) ||
      paidAtDate;

    for (let i = 0; i < completedCount; i += 1) {
      nextDate = addFrequency(nextDate, target.frequency);
    }

    while (nextDate <= paidAtDate) {
      nextDate = addFrequency(nextDate, target.frequency);
    }

    target.nextPaymentDate = nextDate;
  }

  await target.save();
}

export function getLoanRepaymentToDate(application) {
  const totalPrincipalPaid = roundCurrency(application?.totalPrincipalPaid ?? 0);
  const totalInterestPaid = roundCurrency(application?.totalInterestPaid ?? 0);
  const explicit = totalPrincipalPaid + totalInterestPaid;
  if (explicit > 0 || application?.principalOutstanding != null) {
    return roundCurrency(explicit);
  }

  const totalRepayable = Number(application?.totalRepayable ?? 0);
  const remainingBalance = Number(application?.remainingBalance ?? 0);
  return Number.isFinite(totalRepayable) && totalRepayable > 0
    ? Math.max(0, roundCurrency(totalRepayable - remainingBalance))
    : 0;
}

export function getLoanRemainingBreakdown(application) {
  const principalOutstanding = Math.max(
    0,
    roundCurrency(application?.principalOutstanding ?? 0),
  );
  const accruedInterestBalance = Math.max(
    0,
    roundCurrency(application?.accruedInterestBalance ?? 0),
  );

  if (principalOutstanding > 0 || accruedInterestBalance > 0) {
    return {
      principalOutstanding,
      accruedInterestBalance,
    };
  }

  const remainingBalance = Math.max(
    0,
    roundCurrency(application?.remainingBalance ?? 0),
  );
  return {
    principalOutstanding: remainingBalance,
    accruedInterestBalance: 0,
  };
}

function setNextPaymentFields(application, nextPayment) {
  application.monthlyPayment = nextPayment?.amountDue
    ? roundCurrency(nextPayment.amountDue)
    : 0;
}

export async function syncLoanRepaymentState(
  application,
  { asOf = new Date(), scheduleItems = null } = {},
) {
  if (!application?._id) throw new AppError("Loan application not found", 404);

  const asOfDate = parseDate(asOf, new Date()) || new Date();
  const schedule =
    Array.isArray(scheduleItems) && scheduleItems.length > 0
      ? [...scheduleItems].sort(sortScheduleItems)
      : await LoanRepaymentScheduleItemModel.find({
          loanApplicationId: application._id,
        }).sort({ dueDate: 1, installmentNumber: 1 });

  if (
    application.principalOutstanding === null ||
    typeof application.principalOutstanding === "undefined" ||
    application.nextInterestAccrualDate === null ||
    typeof application.nextInterestAccrualDate === "undefined"
  ) {
    await initializeLegacyLoanRepaymentState(application, schedule, asOfDate);
  }

  const { actualItems } = await materializeDueCycles(application, schedule, asOfDate);

  recalculateApplicationBalances(application);

  const { projectionData } = await rebuildProjectedSchedule(application, actualItems);
  const nextPayment =
    pickOutstandingScheduleItem(actualItems) ||
    (projectionData.length > 0
      ? {
          ...projectionData[0],
          amountDue: projectionData[0].totalAmount,
        }
      : application.remainingBalance > 0 && application.nextInterestAccrualDate
        ? projectNextInstallment({
            application,
            actualCycleCount: actualItems.length,
            dueDate: parseDate(application.nextInterestAccrualDate, new Date()),
            principalOutstanding: application.principalOutstanding,
          })
        : null);

  setNextPaymentFields(application, nextPayment);
  recalculateApplicationBalances(application);

  const actualItemsToSave = actualItems.filter(
    (item) =>
      item &&
      typeof item.save === "function" &&
      (item.isNew || (typeof item.isModified === "function" && item.isModified())),
  );
  if (actualItemsToSave.length > 0) {
    await Promise.all(actualItemsToSave.map((item) => item.save()));
  }

  if (isMongooseDocument(application) && application.isModified()) {
    await application.save();
  }

  return {
    application,
    nextPayment,
  };
}

export async function buildLoanNextPaymentMap(applications) {
  if (!Array.isArray(applications) || applications.length === 0) {
    return new Map();
  }

  const applicationIds = applications
    .map((application) => application?._id)
    .filter(Boolean);
  const now = new Date();
  const appById = new Map(
    applications
      .filter((application) => application?._id)
      .map((application) => [String(application._id), application]),
  );

  const docs = applications.filter((application) => isMongooseDocument(application));
  if (docs.length > 0) {
    await Promise.all(
      docs.map((application) => syncLoanRepaymentState(application, { asOf: now })),
    );
    docs.forEach((application) => {
      appById.set(
        String(application._id),
        typeof application.toObject === "function"
          ? application.toObject()
          : application,
      );
    });
  }

  const docIds = new Set(docs.map((application) => String(application._id)));
  const activePlainIds = applications
    .filter((application) => {
      if (!application?._id) return false;
      if (docIds.has(String(application._id))) return false;
      return ["disbursed", "defaulted"].includes(String(application.status || ""));
    })
    .map((application) => application._id);

  if (activePlainIds.length > 0) {
    const plainDocs = await LoanApplicationModel.find({
      _id: { $in: activePlainIds },
    });
    await Promise.all(
      plainDocs.map((application) => syncLoanRepaymentState(application, { asOf: now })),
    );
    plainDocs.forEach((application) => {
      appById.set(String(application._id), application.toObject());
    });
  }

  const scheduleItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: { $in: applicationIds },
    status: { $in: ["pending", "upcoming", "overdue"] },
  })
    .sort({ dueDate: 1, installmentNumber: 1 })
    .lean();

  const map = new Map();

  for (const item of scheduleItems) {
    const loanId = String(item.loanApplicationId);
    if (map.has(loanId)) continue;

    const amountDue = getLoanScheduleOutstandingAmount(item);
    if (amountDue <= 0) continue;

    const dueDate = parseDate(item.dueDate, null);
    const status =
      dueDate && dueDate.getTime() < now.getTime()
        ? "overdue"
        : item.isProjected
          ? "upcoming"
          : "pending";

    map.set(loanId, {
      ...item,
      amountDue,
      status,
    });
  }

  for (const loanId of applicationIds.map((value) => String(value))) {
    if (map.has(loanId)) continue;

    const application = appById.get(loanId);
    if (!application) continue;

    const remainingBalance = Math.max(
      0,
      roundCurrency(application?.remainingBalance ?? 0),
    );
    if (remainingBalance <= 0) continue;

    const dueDate = parseDate(
      application?.nextInterestAccrualDate || application?.repaymentStartDate,
      null,
    );
    if (!dueDate) continue;

    const actualCycleCount = 0;
    const projected = projectNextInstallment({
      application,
      actualCycleCount,
      dueDate,
      principalOutstanding:
        application?.principalOutstanding ?? application?.remainingBalance ?? 0,
    });
    map.set(loanId, {
      ...projected,
      status: dueDate.getTime() < now.getTime() ? "overdue" : "upcoming",
    });
  }

  return map;
}

export async function applyLoanRepayment({
  application,
  amount,
  reference,
  channel = null,
  description = null,
  gateway = "manual",
  metadata = {},
  paidAt = new Date(),
  transaction = null,
} = {}) {
  if (!application?._id) throw new AppError("Loan application not found", 404);

  if (!["disbursed", "defaulted"].includes(String(application.status))) {
    throw new AppError("Loan is not active", 400);
  }

  const repaymentAmount = roundCurrency(amount);
  if (!Number.isFinite(repaymentAmount) || repaymentAmount <= 0) {
    throw new AppError("amount must be greater than 0", 400);
  }

  const paidAtDate = parseDate(paidAt, new Date()) || new Date();
  await syncLoanRepaymentState(application, { asOf: paidAtDate });

  const currentRemaining = roundCurrency(application.remainingBalance ?? 0);
  if (repaymentAmount > currentRemaining) {
    throw new AppError("Repayment amount cannot exceed the remaining balance", 400);
  }

  const scheduleItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: application._id,
    isProjected: { $ne: true },
    status: { $in: ["pending", "overdue"] },
  }).sort({ dueDate: 1, installmentNumber: 1 });

  const tx =
    transaction ||
    (await TransactionModel.create({
      userId: application.userId,
      reference,
      amount: repaymentAmount,
      type: "loan_repayment",
      status: "success",
      description:
        String(description || "").trim() ||
        `Loan repayment for ${application.loanCode || "loan"}`,
      channel,
      groupId: application.groupId || null,
      groupName: application.groupName || null,
      loanId: application._id,
      loanName: application.loanCode || null,
      metadata: metadata || null,
      gateway,
    }));

  let remainingToApply = repaymentAmount;
  let settledInstallmentCount = 0;
  const allocations = [];

  for (const item of scheduleItems) {
    if (remainingToApply <= 0) break;
    attachNormalizedPaymentFields(item);

    const interestDue = getLoanScheduleOutstandingInterestAmount(item);
    const principalDue = getLoanScheduleOutstandingPrincipalAmount(item);
    if (interestDue <= 0 && principalDue <= 0) {
      item.status = "paid";
      continue;
    }

    const interestApplied = roundCurrency(Math.min(interestDue, remainingToApply));
    if (interestApplied > 0) {
      item.paidInterestAmount = roundCurrency(
        Number(item.paidInterestAmount ?? 0) + interestApplied,
      );
      application.accruedInterestBalance = roundCurrency(
        Math.max(0, Number(application.accruedInterestBalance ?? 0) - interestApplied),
      );
      application.totalInterestPaid = roundCurrency(
        Number(application.totalInterestPaid ?? 0) + interestApplied,
      );
      remainingToApply = roundCurrency(remainingToApply - interestApplied);
    }

    const principalApplied = roundCurrency(
      Math.min(getLoanScheduleOutstandingPrincipalAmount(item), remainingToApply),
    );
    if (principalApplied > 0) {
      item.paidPrincipalAmount = roundCurrency(
        Number(item.paidPrincipalAmount ?? 0) + principalApplied,
      );
      application.principalOutstanding = roundCurrency(
        Math.max(0, Number(application.principalOutstanding ?? 0) - principalApplied),
      );
      application.totalPrincipalPaid = roundCurrency(
        Number(application.totalPrincipalPaid ?? 0) + principalApplied,
      );
      remainingToApply = roundCurrency(remainingToApply - principalApplied);
    }

    attachNormalizedPaymentFields(item);

    allocations.push({
      allocationType: "schedule_due",
      scheduleItemId: item._id,
      installmentNumber: item.installmentNumber,
      dueDate: item.dueDate,
      appliedAmount: roundCurrency(interestApplied + principalApplied),
      interestApplied,
      principalApplied,
      remainingInterestBalance: getLoanScheduleOutstandingInterestAmount(item),
      remainingPrincipalBalance: getLoanScheduleOutstandingPrincipalAmount(item),
      remainingInstallmentBalance: getLoanScheduleOutstandingAmount(item),
    });

    if (getLoanScheduleOutstandingAmount(item) <= 0) {
      item.status = "paid";
      item.paidAt = paidAtDate;
      item.transactionId = tx._id;
      item.reference = tx.reference;
      settledInstallmentCount += 1;
    } else {
      item.paidAt = null;
      item.transactionId = null;
      item.reference = null;
      item.status = getScheduleStatusForDueDate(
        parseDate(item.dueDate, paidAtDate),
        paidAtDate,
        { projected: false },
      );
    }
  }

  if (remainingToApply > 0) {
    const principalPrepayment = roundCurrency(
      Math.min(
        remainingToApply,
        Math.max(0, Number(application.principalOutstanding ?? 0)),
      ),
    );

    if (principalPrepayment > 0) {
      application.principalOutstanding = roundCurrency(
        Math.max(0, Number(application.principalOutstanding ?? 0) - principalPrepayment),
      );
      application.totalPrincipalPaid = roundCurrency(
        Number(application.totalPrincipalPaid ?? 0) + principalPrepayment,
      );
      remainingToApply = roundCurrency(remainingToApply - principalPrepayment);

      allocations.push({
        allocationType: "principal_prepayment",
        scheduleItemId: null,
        installmentNumber: null,
        dueDate: null,
        appliedAmount: principalPrepayment,
        interestApplied: 0,
        principalApplied: principalPrepayment,
        remainingInterestBalance: roundCurrency(
          application.accruedInterestBalance ?? 0,
        ),
        remainingPrincipalBalance: roundCurrency(
          application.principalOutstanding ?? 0,
        ),
        remainingInstallmentBalance: roundCurrency(
          Number(application.accruedInterestBalance ?? 0) +
            Number(application.principalOutstanding ?? 0),
        ),
      });
    }
  }

  if (remainingToApply > 0) {
    throw new AppError("Unable to fully apply repayment amount", 400);
  }

  await Promise.all(scheduleItems.map((item) => item.save()));

  recalculateApplicationBalances(application);
  await syncLoanRepaymentState(application, { asOf: paidAtDate });

  const updatedRemaining = Math.max(
    0,
    roundCurrency(application.remainingBalance ?? 0),
  );

  tx.status = "success";
  tx.gateway = gateway || tx.gateway || "manual";
  tx.channel = channel || tx.channel || null;
  tx.groupId = application.groupId || tx.groupId || null;
  tx.groupName = application.groupName || tx.groupName || null;
  tx.loanId = application._id;
  tx.loanName = application.loanCode || tx.loanName || null;
  tx.metadata = {
    ...(tx.metadata || {}),
    ...(metadata || {}),
    allocations: allocations.map((allocation) => ({
      ...allocation,
      scheduleItemId: allocation.scheduleItemId
        ? String(allocation.scheduleItemId)
        : null,
    })),
    settledInstallmentCount,
    principalPaid: roundCurrency(
      allocations.reduce(
        (sum, allocation) => sum + Number(allocation.principalApplied || 0),
        0,
      ),
    ),
    interestPaid: roundCurrency(
      allocations.reduce(
        (sum, allocation) => sum + Number(allocation.interestApplied || 0),
        0,
      ),
    ),
    remainingBalanceAfterPayment: updatedRemaining,
    remainingPrincipalAfterPayment: roundCurrency(
      application.principalOutstanding ?? 0,
    ),
    remainingInterestAfterPayment: roundCurrency(
      application.accruedInterestBalance ?? 0,
    ),
    paidAt: paidAtDate.toISOString(),
  };
  await tx.save();

  await syncLoanRecurringPaymentStats({
    userId: application.userId,
    loanId: application._id,
    amount: repaymentAmount,
    paidAt: paidAtDate,
    settledInstallmentCount,
  });

  const nextPaymentMap = await buildLoanNextPaymentMap([application]);
  const nextPaymentItem = nextPaymentMap.get(String(application._id)) || null;
  const refreshedScheduleItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: application._id,
  }).sort({ dueDate: 1, installmentNumber: 1 });

  return {
    application,
    transaction: tx,
    allocations,
    settledInstallmentCount,
    nextPayment: nextPaymentItem
      ? {
          scheduleItemId: nextPaymentItem._id || nextPaymentItem.scheduleItemId || null,
          installmentNumber: nextPaymentItem.installmentNumber ?? null,
          dueDate: nextPaymentItem.dueDate ?? null,
          status: nextPaymentItem.status,
          amountDue: roundCurrency(nextPaymentItem.amountDue ?? 0),
        }
      : null,
    scheduleItems: refreshedScheduleItems,
  };
}
