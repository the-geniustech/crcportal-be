import { ContributionModel } from "../models/Contribution.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { normalizeContributionType } from "./contributionPolicy.js";

function addFrequency(date, frequency) {
  const nextDate = new Date(date);
  if (frequency === "weekly") nextDate.setDate(nextDate.getDate() + 7);
  else if (frequency === "bi-weekly") nextDate.setDate(nextDate.getDate() + 14);
  else nextDate.setMonth(nextDate.getMonth() + 1);
  return nextDate;
}

function parseDate(value, fallback = null) {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return date;
}

function withSession(query, session) {
  if (session && typeof query?.session === "function") {
    query.session(session);
  }
  return query;
}

function buildRecurringContributionScheduleQuery({
  userId,
  groupId,
  contributionType,
  isActive = true,
} = {}) {
  if (!userId || !groupId) return null;

  const canonicalType = normalizeContributionType(contributionType) || "revolving";
  return {
    userId,
    paymentType: "group_contribution",
    groupId,
    contributionType: canonicalType,
    ...(typeof isActive === "boolean" ? { isActive } : {}),
  };
}

export async function findMatchingRecurringContributionSchedule({
  recurringPaymentId = null,
  userId,
  groupId,
  contributionType,
  amount,
  session = null,
  isActive = true,
} = {}) {
  if (recurringPaymentId) {
    const query = RecurringPaymentModel.findById(recurringPaymentId);
    const schedule = await withSession(query, session);
    return schedule || null;
  }

  const scheduleQuery = buildRecurringContributionScheduleQuery({
    userId,
    groupId,
    contributionType,
    isActive,
  });
  if (!scheduleQuery) return null;

  const schedules = await withSession(
    RecurringPaymentModel.find(scheduleQuery).sort({
      nextPaymentDate: 1,
      createdAt: 1,
    }),
    session,
  );
  if (!Array.isArray(schedules) || schedules.length === 0) return null;

  if (Number.isFinite(Number(amount))) {
    const roundedAmount = Math.round(Number(amount) * 100);
    const exactMatch = schedules.find(
      (schedule) =>
        Math.round(Number(schedule.amount ?? 0) * 100) === roundedAmount,
    );
    if (exactMatch) return exactMatch;
  }

  return schedules[0] || null;
}

export async function attachRecurringContributionSchedule({
  contribution = null,
  recurringPaymentId = null,
  userId,
  groupId,
  contributionType,
  amount,
  session = null,
  isActive = true,
} = {}) {
  const schedule = await findMatchingRecurringContributionSchedule({
    recurringPaymentId,
    userId,
    groupId,
    contributionType,
    amount,
    session,
    isActive,
  });

  if (contribution) {
    contribution.recurringPaymentId = schedule?._id || null;
  }

  return schedule || null;
}

export async function applyRecurringContributionSchedulePayment({
  recurringPaymentId = null,
  userId,
  groupId,
  contributionType,
  amount,
  count = 1,
  paidAt,
  session = null,
} = {}) {
  const schedule = await findMatchingRecurringContributionSchedule({
    recurringPaymentId,
    userId,
    groupId,
    contributionType,
    amount,
    session,
    isActive: true,
  });
  if (!schedule) return null;

  const paidAtDate = parseDate(paidAt, new Date());
  schedule.totalPaymentsMade = Number(schedule.totalPaymentsMade ?? 0) + count;
  schedule.totalAmountPaid =
    Number(schedule.totalAmountPaid ?? 0) + Number(amount ?? 0);
  schedule.lastPaymentDate = paidAtDate;
  schedule.lastPaymentStatus = "success";

  const baseDate =
    parseDate(schedule.nextPaymentDate, null) ||
    parseDate(schedule.startDate, null) ||
    paidAtDate;
  let nextDate = baseDate || paidAtDate;
  for (let i = 0; i < count; i += 1) {
    nextDate = addFrequency(nextDate, schedule.frequency);
  }
  while (nextDate <= paidAtDate) {
    nextDate = addFrequency(nextDate, schedule.frequency);
  }
  schedule.nextPaymentDate = nextDate;
  await schedule.save({ session: session || undefined });
  return schedule;
}

export async function rebuildRecurringContributionSchedule({
  recurringPaymentId,
  session = null,
} = {}) {
  if (!recurringPaymentId) return null;

  const schedule = await withSession(
    RecurringPaymentModel.findById(recurringPaymentId),
    session,
  );
  if (!schedule) return null;

  const contributions = await withSession(
    ContributionModel.find({
      recurringPaymentId: schedule._id,
      status: { $in: ["completed", "verified"] },
    }).sort({ verifiedAt: 1, createdAt: 1, _id: 1 }),
    session,
  );

  const totalPaymentsMade = contributions.length;
  const totalAmountPaid = contributions.reduce(
    (sum, contribution) => sum + Number(contribution.amount ?? 0),
    0,
  );
  const lastContribution =
    totalPaymentsMade > 0 ? contributions[totalPaymentsMade - 1] : null;
  const lastPaidAt = parseDate(
    lastContribution?.verifiedAt ||
      lastContribution?.updatedAt ||
      lastContribution?.createdAt,
    null,
  );

  schedule.totalPaymentsMade = totalPaymentsMade;
  schedule.totalAmountPaid = totalAmountPaid;
  schedule.lastPaymentDate = lastPaidAt;
  schedule.lastPaymentStatus = totalPaymentsMade > 0 ? "success" : null;

  let nextDate = parseDate(schedule.startDate, new Date()) || new Date();
  for (let i = 0; i < totalPaymentsMade; i += 1) {
    nextDate = addFrequency(nextDate, schedule.frequency);
  }

  const compareDate = lastPaidAt || new Date();
  while (nextDate <= compareDate) {
    nextDate = addFrequency(nextDate, schedule.frequency);
  }

  schedule.nextPaymentDate = nextDate;
  await schedule.save({ session: session || undefined });
  return schedule;
}
