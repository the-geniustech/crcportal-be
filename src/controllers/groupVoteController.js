import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupVoteModel, GroupVoteStatuses } from "../models/GroupVote.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";

export const listGroupVotes = catchAsync(async (req, res, next) => {
  const group = req.group;

  const filter = { groupId: group._id };
  if (req.query?.status) {
    const status = String(req.query.status).trim();
    if (!GroupVoteStatuses.includes(status)) {
      return next(
        new AppError(`Invalid status. Allowed: ${GroupVoteStatuses.join(", ")}`, 400),
      );
    }
    filter.status = status;
  }

  const [votes, totalMembers] = await Promise.all([
    GroupVoteModel.find(filter).sort({ createdAt: -1 }).lean(),
    GroupMembershipModel.countDocuments({ groupId: group._id, status: "active" }),
  ]);

  const normalized = votes.map((vote) => ({
    ...vote,
    totalVoters:
      typeof vote.totalVoters === "number" && vote.totalVoters > 0
        ? vote.totalVoters
        : totalMembers,
  }));

  return sendSuccess(res, {
    statusCode: 200,
    results: normalized.length,
    data: { votes: normalized },
  });
});

export const createGroupVote = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const group = req.group;
  const {
    title,
    description = "",
    status = "active",
    endsAt = null,
    yesVotes = 0,
    noVotes = 0,
    totalVoters,
  } = req.body || {};

  if (!title || !String(title).trim()) {
    return next(new AppError("title is required", 400));
  }
  if (status && !GroupVoteStatuses.includes(status)) {
    return next(
      new AppError(`Invalid status. Allowed: ${GroupVoteStatuses.join(", ")}`, 400),
    );
  }

  const parsedEndsAt = endsAt ? new Date(endsAt) : null;
  if (endsAt && Number.isNaN(parsedEndsAt.getTime())) {
    return next(new AppError("endsAt must be a valid date", 400));
  }

  const membersCount = await GroupMembershipModel.countDocuments({
    groupId: group._id,
    status: "active",
  });

  const vote = await GroupVoteModel.create({
    groupId: group._id,
    title: String(title).trim(),
    description: String(description || "").trim(),
    status,
    endsAt: parsedEndsAt,
    yesVotes: Math.max(0, Number(yesVotes) || 0),
    noVotes: Math.max(0, Number(noVotes) || 0),
    totalVoters:
      typeof totalVoters !== "undefined" && Number.isFinite(Number(totalVoters))
        ? Math.max(0, Number(totalVoters))
        : membersCount,
    createdBy: req.user.profileId,
  });

  return sendSuccess(res, {
    statusCode: 201,
    message: "Group vote created",
    data: { vote },
  });
});
