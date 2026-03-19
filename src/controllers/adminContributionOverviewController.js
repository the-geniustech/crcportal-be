import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel, ContributionTypes } from "../models/Contribution.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function parseYear(req) {
  const now = new Date();
  const year = Number(req.query?.year ?? now.getUTCFullYear());
  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    return { error: "Invalid year" };
  }
  return { year };
}

function parseContributionType(req) {
  const type = String(req.query?.contributionType ?? "regular").trim();
  if (!ContributionTypes.includes(type)) {
    return { error: `Invalid contributionType. Allowed: ${ContributionTypes.join(", ")}` };
  }
  return { type };
}

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (req.user.role === "admin") return null;

  if (req.user.role !== "groupCoordinator") {
    throw new AppError("Insufficient permissions", 403);
  }

  const coordinatorMemberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return coordinatorMemberships.map((m) => String(m.groupId));
}

export const getAdminContributionTracking = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const yearParsed = parseYear(req);
  if (yearParsed.error) return next(new AppError(yearParsed.error, 400));
  const typeParsed = parseContributionType(req);
  if (typeParsed.error) return next(new AppError(typeParsed.error, 400));

  const { year } = yearParsed;
  const { type: contributionType } = typeParsed;

  const manageableGroupIds = await getManageableGroupIds(req);
  const filter = {};
  if (manageableGroupIds) filter._id = { $in: manageableGroupIds };

  const groups = await GroupModel.find(filter).sort({ groupNumber: 1 }).lean();
  if (groups.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      results: 0,
      data: { year, contributionType, groups: [] },
    });
  }

  const groupIds = groups.map((g) => String(g._id));
  const groupObjectIds = groupIds
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  const [activeCounts, contribAgg] = await Promise.all([
    GroupMembershipModel.aggregate([
      { $match: { groupId: { $in: groupObjectIds }, status: "active" } },
      { $group: { _id: "$groupId", count: { $sum: 1 } } },
    ]),
    ContributionModel.aggregate([
      {
        $match: {
          groupId: { $in: groupObjectIds },
          year,
          contributionType,
        },
      },
      {
        $group: {
          _id: { groupId: "$groupId", month: "$month" },
          paidAmount: {
            $sum: {
              $cond: [{ $in: ["$status", ["verified", "completed"]] }, "$amount", 0],
            },
          },
          hasVerified: {
            $max: { $cond: [{ $eq: ["$status", "verified"] }, 1, 0] },
          },
        },
      },
    ]),
  ]);

  const activeByGroupId = new Map(activeCounts.map((r) => [String(r._id), Number(r.count || 0)]));
  const monthlyByKey = new Map(
    contribAgg.map((r) => [
      `${String(r._id.groupId)}|${Number(r._id.month)}`,
      {
        paidAmount: Number(r.paidAmount || 0),
        hasVerified: Boolean(r.hasVerified),
      },
    ]),
  );

  const months = Array.from({ length: 12 }, (_, idx) => idx + 1);

  const rows = groups.map((g) => {
    const gid = String(g._id);
    const activeMembers = activeByGroupId.get(gid) ?? Number(g.memberCount || 0);
    const expectedPerMonth = Number(g.monthlyContribution || 0) * Math.max(0, activeMembers);
    let totalPaid = 0;

    const monthStatuses = months.map((m) => {
      const key = `${gid}|${m}`;
      const entry = monthlyByKey.get(key);
      const paidAmount = entry?.paidAmount ?? 0;
      const hasVerified = entry?.hasVerified ?? false;
      totalPaid += paidAmount;

      let status = "pending";
      if (paidAmount >= expectedPerMonth && expectedPerMonth > 0) {
        status = hasVerified ? "verified" : "completed";
      } else if (paidAmount > 0) {
        status = "completed";
      }

      return {
        month: m,
        status,
        expectedAmount: expectedPerMonth,
        paidAmount,
        hasVerified,
      };
    });

    return {
      groupId: gid,
      groupNumber: g.groupNumber,
      groupName: g.groupName,
      isSpecial: Boolean(g.isSpecial),
      monthlyContribution: Number(g.monthlyContribution || 0),
      activeMembers,
      totalPaid,
      months: monthStatuses,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: rows.length,
    data: { year, contributionType, groups: rows },
  });
});

export const getAdminSpecialContributionSummary = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const yearParsed = parseYear(req);
  if (yearParsed.error) return next(new AppError(yearParsed.error, 400));
  const { year } = yearParsed;

  const manageableGroupIds = await getManageableGroupIds(req);
  const groupScopeFilter = manageableGroupIds ? { _id: { $in: manageableGroupIds } } : {};

  const specialGroup =
    (await GroupModel.findOne({ ...groupScopeFilter, groupNumber: 0 }).lean()) ||
    (await GroupModel.findOne({ ...groupScopeFilter, isSpecial: true }).lean());

  const match = {
    year,
    contributionType: { $in: ["festival", "end_well", "special_savings"] },
  };

  if (specialGroup) {
    match.groupId = specialGroup._id;
  } else if (manageableGroupIds) {
    const groupObjectIds = manageableGroupIds
      .filter((id) => mongoose.Types.ObjectId.isValid(id))
      .map((id) => new mongoose.Types.ObjectId(id));
    match.groupId = { $in: groupObjectIds };
  }

  const rows = await ContributionModel.aggregate([
    { $match: match },
    {
      $group: {
        _id: "$contributionType",
        paidAmount: {
          $sum: {
            $cond: [{ $in: ["$status", ["verified", "completed"]] }, "$amount", 0],
          },
        },
        contributors: { $addToSet: "$userId" },
      },
    },
  ]);

  const byType = new Map(
    rows.map((r) => [
      String(r._id),
      {
        totalCollected: Number(r.paidAmount || 0),
        contributors: Array.isArray(r.contributors) ? r.contributors.length : 0,
      },
    ]),
  );

  const summary = ["festival", "end_well", "special_savings"].map((type) => ({
    type,
    totalCollected: byType.get(type)?.totalCollected ?? 0,
    contributors: byType.get(type)?.contributors ?? 0,
  }));

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      year,
      group: specialGroup
        ? { id: String(specialGroup._id), groupNumber: specialGroup.groupNumber, groupName: specialGroup.groupName }
        : null,
      summary,
    },
  });
});

