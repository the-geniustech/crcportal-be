import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel } from "../models/Contribution.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { MeetingModel } from "../models/Meeting.js";
import { MeetingAttendanceModel } from "../models/MeetingAttendance.js";
import { MeetingRsvpModel } from "../models/MeetingRsvp.js";
import { normalizeContributionType } from "../utils/contributionPolicy.js";

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function roundInt(n) {
  return Math.round(Number.isFinite(n) ? n : 0);
}

function monthKeyUTC(date) {
  const y = date.getUTCFullYear();
  const m = date.getUTCMonth() + 1;
  return `${y}-${String(m).padStart(2, "0")}`;
}

function startOfMonthUTC(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1, 0, 0, 0, 0));
}

function endOfMonthUTC(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0, 23, 59, 59, 999));
}

function addMonthsUTC(date, months) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + months, 1, 0, 0, 0, 0));
}

function monthsBetweenUTC(fromDate, toDate) {
  const from = startOfMonthUTC(fromDate);
  const to = startOfMonthUTC(toDate);
  return (to.getUTCFullYear() - from.getUTCFullYear()) * 12 + (to.getUTCMonth() - from.getUTCMonth());
}

function formatNaira(amount) {
  const n = Number(amount);
  if (!Number.isFinite(n)) return "₦0";
  try {
    return new Intl.NumberFormat("en-NG", {
      style: "currency",
      currency: "NGN",
      maximumFractionDigits: 0,
    }).format(n);
  } catch {
    return `₦${Math.round(n).toLocaleString("en-NG")}`;
  }
}

function statusFromPercentage(pct) {
  if (pct >= 90) return "excellent";
  if (pct >= 75) return "good";
  if (pct >= 55) return "fair";
  return "poor";
}

function impactFromValue(val, goodThreshold, fairThreshold) {
  if (val >= goodThreshold) return "positive";
  if (val >= fairThreshold) return "neutral";
  return "negative";
}

function isRevolvingContribution(contribution) {
  if (!contribution) return false;
  const raw = contribution.contributionType;
  if (raw === undefined || raw === null || raw === "") return true;
  const canonical = normalizeContributionType(raw);
  return canonical === "revolving";
}

function loanImpactFromScore(score) {
  const tiers = [
    { min: 750, rate: 3.5, mult: 5.0 },
    { min: 650, rate: 4.0, mult: 4.0 },
    { min: 550, rate: 5.0, mult: 3.0 },
    { min: 300, rate: 6.0, mult: 2.0 },
  ];

  const currentTierIndex = tiers.findIndex((t) => score >= t.min);
  const currentTier = tiers[currentTierIndex === -1 ? tiers.length - 1 : currentTierIndex];
  const potentialTier = tiers[Math.max(0, currentTierIndex - 1)] || tiers[0];

  return {
    currentRate: currentTier.rate,
    potentialRate: potentialTier.rate,
    maxLoanMultiplier: currentTier.mult,
    potentialMultiplier: potentialTier.mult,
  };
}

async function loadCreditInputs({ profileId, userCreatedAt, windowStart, windowEnd }) {
  const memberships = await GroupMembershipModel.find(
    { userId: profileId },
    { groupId: 1, status: 1, joinedAt: 1, createdAt: 1 },
  ).lean();

  const groupIdsAll = [...new Set(memberships.map((m) => String(m.groupId)))];
  const groupIdsActive = memberships
    .filter((m) => m.status === "active")
    .map((m) => String(m.groupId));

  const [contributions, loans, guarantorRequests, meetings] = await Promise.all([
    ContributionModel.find(
      {
        userId: profileId,
        createdAt: { $gte: windowStart, $lte: windowEnd },
      },
      { amount: 1, status: 1, contributionType: 1, month: 1, year: 1, createdAt: 1 },
    ).lean(),
    LoanApplicationModel.find(
      { userId: profileId, createdAt: { $lte: windowEnd } },
      { status: 1, createdAt: 1, updatedAt: 1 },
    ).lean(),
    LoanGuarantorModel.find(
      { guarantorUserId: profileId, createdAt: { $gte: windowStart, $lte: windowEnd } },
      { status: 1, createdAt: 1 },
    ).lean(),
    groupIdsAll.length === 0
      ? Promise.resolve([])
      : MeetingModel.find(
          {
            groupId: { $in: groupIdsAll },
            scheduledDate: { $gte: windowStart, $lte: windowEnd },
          },
          { _id: 1, groupId: 1, scheduledDate: 1 },
        ).lean(),
  ]);

  const loanIds = loans.map((l) => l._id);
  const scheduleItems = loanIds.length
    ? await LoanRepaymentScheduleItemModel.find(
        { loanApplicationId: { $in: loanIds }, dueDate: { $gte: windowStart, $lte: windowEnd } },
        { loanApplicationId: 1, dueDate: 1, status: 1, paidAt: 1 },
      ).lean()
    : [];

  const meetingIds = meetings.map((m) => m._id);
  const [attendance, rsvps] = await Promise.all([
    meetingIds.length
      ? MeetingAttendanceModel.find(
          { userId: profileId, meetingId: { $in: meetingIds } },
          { meetingId: 1, status: 1, createdAt: 1 },
        ).lean()
      : Promise.resolve([]),
    meetingIds.length
      ? MeetingRsvpModel.find(
          { userId: profileId, meetingId: { $in: meetingIds } },
          { meetingId: 1, status: 1, createdAt: 1 },
        ).lean()
      : Promise.resolve([]),
  ]);

  const accountCreatedAt = userCreatedAt ? new Date(userCreatedAt) : windowEnd;
  const earliestMembership = memberships.reduce((acc, m) => {
    const dt = m.joinedAt ? new Date(m.joinedAt) : m.createdAt ? new Date(m.createdAt) : null;
    if (!dt) return acc;
    if (!acc || dt.getTime() < acc.getTime()) return dt;
    return acc;
  }, null);

  return {
    memberships,
    groupIdsAll,
    groupIdsActive,
    accountCreatedAt,
    earliestMembership: earliestMembership || accountCreatedAt,
    contributions,
    loans,
    scheduleItems,
    guarantorRequests,
    meetings,
    attendance,
    rsvps,
  };
}

function computeContributionFactor({ asOf, earliestMembership, contributions }) {
  const maxScore = 200;
  const windowMonths = 12;

  const monthEnd = endOfMonthUTC(asOf);
  const windowStartMonth = addMonthsUTC(startOfMonthUTC(asOf), -(windowMonths - 1));
  const effectiveStart = startOfMonthUTC(
    new Date(Math.max(windowStartMonth.getTime(), startOfMonthUTC(earliestMembership).getTime())),
  );

  const totalMonths = Math.max(1, monthsBetweenUTC(effectiveStart, startOfMonthUTC(asOf)) + 1);

  const monthKeys = [];
  for (let i = 0; i < totalMonths; i += 1) {
    monthKeys.push(monthKeyUTC(addMonthsUTC(effectiveStart, i)));
  }

  const byMonth = new Map(monthKeys.map((k) => [k, []]));

  for (const c of contributions) {
    const createdAt = c.createdAt ? new Date(c.createdAt) : null;
    if (!createdAt || createdAt.getTime() > monthEnd.getTime()) continue;
    const key = monthKeyUTC(createdAt);
    if (!byMonth.has(key)) continue;
    if (!isRevolvingContribution(c)) continue;
    byMonth.get(key).push(c);
  }

  const goodStatuses = new Set(["verified", "completed"]);
  const onTimeMonths = monthKeys.filter((k) =>
    byMonth.get(k).some((c) => goodStatuses.has(String(c.status))),
  ).length;
  const anyContributionMonths = monthKeys.filter((k) => byMonth.get(k).length > 0).length;

  const onTimeRatio = onTimeMonths / totalMonths;
  const consistencyRatio = anyContributionMonths / totalMonths;

  const recent6Start = addMonthsUTC(startOfMonthUTC(asOf), -5);
  const recentContribs = contributions.filter((c) => {
    if (!isRevolvingContribution(c)) return false;
    const dt = c.createdAt ? new Date(c.createdAt) : null;
    if (!dt) return false;
    if (dt.getTime() < recent6Start.getTime() || dt.getTime() > monthEnd.getTime()) return false;
    return goodStatuses.has(String(c.status));
  });
  const avgAmount =
    recentContribs.length === 0
      ? 0
      : recentContribs.reduce((sum, c) => sum + Number(c.amount || 0), 0) / recentContribs.length;

  const baselineAmount = 25_000;
  const avgAmountRatio = clamp(avgAmount / baselineAmount, 0, 1);

  const score =
    onTimeRatio * 120 +
    consistencyRatio * 60 +
    avgAmountRatio * 20;

  const scoreInt = clamp(roundInt(score), 0, maxScore);
  const percentage = (scoreInt / maxScore) * 100;

  return {
    score: scoreInt,
    maxScore,
    percentage,
    status: statusFromPercentage(percentage),
    details: [
      {
        name: "On-time contributions",
        value: `${onTimeMonths} of ${totalMonths}`,
        impact: impactFromValue(onTimeRatio, 0.85, 0.7),
      },
      {
        name: "Contribution consistency",
        value: `${roundInt(consistencyRatio * 100)}%`,
        impact: impactFromValue(consistencyRatio, 0.9, 0.75),
      },
      {
        name: "Average contribution amount",
        value: formatNaira(avgAmount),
        impact: impactFromValue(avgAmountRatio, 0.9, 0.6),
      },
    ],
  };
}

function computeRepaymentFactor({ asOf, loans, scheduleItems }) {
  const maxScore = 300;
  const asOfTime = asOf.getTime();

  const userLoans = loans.filter((l) => new Date(l.createdAt).getTime() <= asOfTime);
  const defaultedLoans = userLoans.filter((l) => l.status === "defaulted");

  const loanIds = new Set(userLoans.map((l) => String(l._id)));
  const dueItems = scheduleItems.filter((s) => loanIds.has(String(s.loanApplicationId)));

  const dueOrPast = dueItems.filter((s) => new Date(s.dueDate).getTime() <= asOfTime);
  const paidOnTime = dueOrPast.filter(
    (s) =>
      String(s.status) === "paid" &&
      s.paidAt &&
      new Date(s.paidAt).getTime() <= new Date(s.dueDate).getTime(),
  );

  const overdue = dueOrPast.filter((s) => {
    if (String(s.status) === "paid") return false;
    return new Date(s.dueDate).getTime() < asOfTime;
  });

  const paidLate = dueOrPast.filter(
    (s) =>
      String(s.status) === "paid" &&
      s.paidAt &&
      new Date(s.paidAt).getTime() > new Date(s.dueDate).getTime(),
  );

  const early = dueOrPast.filter((s) => {
    if (String(s.status) !== "paid" || !s.paidAt) return false;
    const delta = new Date(s.dueDate).getTime() - new Date(s.paidAt).getTime();
    return delta >= 7 * 24 * 60 * 60 * 1000;
  });

  const totalDue = dueOrPast.length;
  const basePct = totalDue === 0 ? 80 : clamp((paidOnTime.length / Math.max(1, totalDue)) * 100, 0, 100);
  let score = (basePct / 100) * maxScore;

  score -= defaultedLoans.length * 80;
  score -= clamp(overdue.length * 10, 0, 60);
  score -= clamp(paidLate.length * 6, 0, 40);
  score += clamp(early.length * 5, 0, 20);

  const scoreInt = clamp(roundInt(score), 0, maxScore);
  const percentage = (scoreInt / maxScore) * 100;

  const completedLoans = userLoans.filter((l) => l.status === "completed");
  const loansRepaidOnTime = completedLoans.filter((l) => {
    const id = String(l._id);
    const items = dueItems.filter((s) => String(s.loanApplicationId) === id);
    const anyLate = items.some(
      (s) =>
        String(s.status) === "paid" &&
        s.paidAt &&
        new Date(s.paidAt).getTime() > new Date(s.dueDate).getTime(),
    );
    const anyOverdue = items.some(
      (s) => String(s.status) !== "paid" && new Date(s.dueDate).getTime() < asOfTime,
    );
    return !anyLate && !anyOverdue;
  });

  return {
    score: scoreInt,
    maxScore,
    percentage,
    status: statusFromPercentage(percentage),
    details: [
      {
        name: "Loans repaid on time",
        value: `${loansRepaidOnTime.length} of ${completedLoans.length}`,
        impact: impactFromValue(
          completedLoans.length === 0 ? 0.8 : loansRepaidOnTime.length / Math.max(1, completedLoans.length),
          0.9,
          0.7,
        ),
      },
      {
        name: "Current loan status",
        value: defaultedLoans.length === 0 ? "No defaults" : `${defaultedLoans.length} default(s)`,
        impact: defaultedLoans.length === 0 ? "positive" : "negative",
      },
      {
        name: "Early repayments",
        value: `${early.length} installment(s)`,
        impact: impactFromValue(early.length, 2, 1),
      },
    ],
  };
}

function computeAttendanceFactor({ asOf, groupIdsAll, meetings, attendance, rsvps }) {
  const maxScore = 150;

  if (groupIdsAll.length === 0) {
    const score = roundInt(0.75 * maxScore);
    return {
      score,
      maxScore,
      percentage: (score / maxScore) * 100,
      status: "fair",
      details: [
        { name: "Meeting attendance", value: "0 of 0", impact: "neutral" },
        { name: "RSVP response rate", value: "0%", impact: "neutral" },
        { name: "Missed meetings", value: "0", impact: "neutral" },
      ],
    };
  }

  const asOfTime = asOf.getTime();
  const sixMonthsStart = addMonthsUTC(startOfMonthUTC(asOf), -5);

  const relevantMeetings = meetings.filter((m) => {
    const dt = m.scheduledDate ? new Date(m.scheduledDate) : null;
    if (!dt) return false;
    return dt.getTime() >= sixMonthsStart.getTime() && dt.getTime() <= asOfTime;
  });

  const meetingIds = new Set(relevantMeetings.map((m) => String(m._id)));

  const presentLike = new Set(["present", "late"]);
  const absentLike = new Set(["absent"]);

  const relevantAttendance = attendance.filter((a) => meetingIds.has(String(a.meetingId)));
  const presentCount = relevantAttendance.filter((a) => presentLike.has(String(a.status))).length;
  const absentCount = relevantAttendance.filter((a) => absentLike.has(String(a.status))).length;

  const relevantRsvps = rsvps.filter((r) => meetingIds.has(String(r.meetingId)));
  const respondedCount = relevantRsvps.filter((r) => String(r.status) !== "pending").length;

  const totalMeetings = relevantMeetings.length;
  const attendanceRate = totalMeetings === 0 ? 0.8 : presentCount / Math.max(1, totalMeetings);
  const rsvpRate = totalMeetings === 0 ? 0.8 : respondedCount / Math.max(1, totalMeetings);

  const score =
    clamp(attendanceRate, 0, 1) * 90 +
    clamp(rsvpRate, 0, 1) * 40 +
    clamp(1 - absentCount / Math.max(1, totalMeetings), 0, 1) * 20;

  const scoreInt = clamp(roundInt(score), 0, maxScore);
  const percentage = (scoreInt / maxScore) * 100;

  return {
    score: scoreInt,
    maxScore,
    percentage,
    status: statusFromPercentage(percentage),
    details: [
      {
        name: "Meeting attendance",
        value: `${presentCount} of ${totalMeetings}`,
        impact: impactFromValue(attendanceRate, 0.85, 0.7),
      },
      {
        name: "RSVP response rate",
        value: `${roundInt(rsvpRate * 100)}%`,
        impact: impactFromValue(rsvpRate, 0.9, 0.75),
      },
      {
        name: "Missed meetings",
        value: `${absentCount}`,
        impact: absentCount === 0 ? "positive" : absentCount <= 2 ? "neutral" : "negative",
      },
    ],
  };
}

function computeParticipationFactor({ asOf, contributions, guarantorRequests, groupIdsActive }) {
  const maxScore = 100;
  const asOfTime = asOf.getTime();

  const sixMonthsStart = addMonthsUTC(startOfMonthUTC(asOf), -5);
  const twelveMonthsStart = addMonthsUTC(startOfMonthUTC(asOf), -11);

  const recentContribs = contributions.filter((c) => {
    const dt = c.createdAt ? new Date(c.createdAt) : null;
    if (!dt) return false;
    if (dt.getTime() < sixMonthsStart.getTime() || dt.getTime() > asOfTime) return false;
    if (!isRevolvingContribution(c)) return false;
    return ["verified", "completed"].includes(String(c.status));
  });

  const acceptedGuarantor = guarantorRequests.filter((g) => {
    const dt = g.createdAt ? new Date(g.createdAt) : null;
    if (!dt) return false;
    if (dt.getTime() < twelveMonthsStart.getTime() || dt.getTime() > asOfTime) return false;
    return String(g.status) === "accepted";
  });

  const activityPoints = clamp(recentContribs.length / 6, 0, 1) * 50;
  const guarantorPoints = clamp(acceptedGuarantor.length * 10, 0, 30);
  const engagementLabel =
    groupIdsActive.length >= 3 ? "High" : groupIdsActive.length === 2 ? "Moderate" : "Low";
  const engagementPoints = groupIdsActive.length >= 3 ? 20 : groupIdsActive.length === 2 ? 14 : 8;

  const scoreInt = clamp(roundInt(activityPoints + guarantorPoints + engagementPoints), 0, maxScore);
  const percentage = (scoreInt / maxScore) * 100;

  return {
    score: scoreInt,
    maxScore,
    percentage,
    status: statusFromPercentage(percentage),
    details: [
      {
        name: "Group activities",
        value: `${recentContribs.length} participated`,
        impact: impactFromValue(recentContribs.length, 6, 3),
      },
      {
        name: "Guarantor requests accepted",
        value: `${acceptedGuarantor.length}`,
        impact: impactFromValue(acceptedGuarantor.length, 2, 1),
      },
      {
        name: "Community engagement",
        value: engagementLabel,
        impact: engagementLabel === "High" ? "positive" : engagementLabel === "Moderate" ? "neutral" : "neutral",
      },
    ],
  };
}

function computeTenureFactor({ asOf, earliestMembership, accountCreatedAt, groupIdsActive }) {
  const maxScore = 100;
  const monthsMembership = Math.max(0, monthsBetweenUTC(earliestMembership, asOf));
  const monthsAccount = Math.max(0, monthsBetweenUTC(accountCreatedAt, asOf));

  const membershipPoints = clamp(monthsMembership / 24, 0, 1) * 50;
  const groupsPoints = clamp(groupIdsActive.length / 5, 0, 1) * 30;
  const accountPoints = clamp(monthsAccount / 24, 0, 1) * 20;

  const scoreInt = clamp(roundInt(membershipPoints + groupsPoints + accountPoints), 0, maxScore);
  const percentage = (scoreInt / maxScore) * 100;

  return {
    score: scoreInt,
    maxScore,
    percentage,
    status: statusFromPercentage(percentage),
    details: [
      {
        name: "Membership duration",
        value: `${monthsMembership} months`,
        impact: impactFromValue(monthsMembership, 12, 6),
      },
      {
        name: "Groups joined",
        value: `${groupIdsActive.length} active`,
        impact: impactFromValue(groupIdsActive.length, 3, 2),
      },
      {
        name: "Account age",
        value: `${monthsAccount} months`,
        impact: impactFromValue(monthsAccount, 12, 6),
      },
    ],
  };
}

function buildSnapshot({ asOf, inputs, minScore = 300, maxScore = 850 }) {
  const contribution = computeContributionFactor({
    asOf,
    earliestMembership: inputs.earliestMembership,
    contributions: inputs.contributions,
  });

  const repayment = computeRepaymentFactor({
    asOf,
    loans: inputs.loans,
    scheduleItems: inputs.scheduleItems,
  });

  const attendance = computeAttendanceFactor({
    asOf,
    groupIdsAll: inputs.groupIdsAll,
    meetings: inputs.meetings,
    attendance: inputs.attendance,
    rsvps: inputs.rsvps,
  });

  const participation = computeParticipationFactor({
    asOf,
    contributions: inputs.contributions,
    guarantorRequests: inputs.guarantorRequests,
    groupIdsActive: inputs.groupIdsActive,
  });

  const tenure = computeTenureFactor({
    asOf,
    earliestMembership: inputs.earliestMembership,
    accountCreatedAt: inputs.accountCreatedAt,
    groupIdsActive: inputs.groupIdsActive,
  });

  const rawTotal =
    contribution.score +
    repayment.score +
    attendance.score +
    participation.score +
    tenure.score;

  const totalScore = clamp(rawTotal, minScore, maxScore);

  return {
    totalScore,
    minScore,
    maxScore,
    factors: { contribution, repayment, attendance, participation, tenure },
    loanImpact: loanImpactFromScore(totalScore),
  };
}

export const getMyCreditScore = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const data = await buildCreditScoreData({
    profileId: req.user.profileId,
    userCreatedAt: req.user.createdAt,
    historyMonths: req.query?.historyMonths,
  });

  return sendSuccess(res, {
    statusCode: 200,
    data,
  });
});

export async function buildCreditScoreData({ profileId, userCreatedAt, historyMonths }) {
  const asOf = new Date();
  const safeHistoryMonths = clamp(Number(historyMonths ?? 6), 3, 12);

  const windowStart = addMonthsUTC(startOfMonthUTC(asOf), -12);
  const windowEnd = endOfMonthUTC(asOf);

  const inputs = await loadCreditInputs({
    profileId,
    userCreatedAt,
    windowStart,
    windowEnd,
  });

  const current = buildSnapshot({ asOf, inputs });

  const history = [];
  for (let i = safeHistoryMonths - 1; i >= 0; i -= 1) {
    const month = addMonthsUTC(startOfMonthUTC(asOf), -i);
    const monthAsOf = endOfMonthUTC(month);
    const snap = buildSnapshot({ asOf: monthAsOf, inputs });
    history.push({ date: monthKeyUTC(month), score: snap.totalScore });
  }

  const prevMonth = addMonthsUTC(startOfMonthUTC(asOf), -1);
  const prevAsOf = endOfMonthUTC(prevMonth);
  const prev = buildSnapshot({ asOf: prevAsOf, inputs });

  const scoreChange = current.totalScore - prev.totalScore;
  const scoreChangeDirection = scoreChange >= 0 ? "up" : "down";

  return {
    totalScore: current.totalScore,
    maxScore: current.maxScore,
    minScore: current.minScore,
    lastUpdated: asOf.toISOString().slice(0, 10),
    scoreChange: Math.abs(scoreChange),
    scoreChangeDirection,
    factors: current.factors,
    history,
    loanImpact: current.loanImpact,
  };
}
