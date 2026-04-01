import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupVoteModel, GroupVoteStatuses } from "../models/GroupVote.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import {
  GroupVoteResponseModel,
  GroupVoteResponseChoices,
} from "../models/GroupVoteResponse.js";
import { ProfileModel } from "../models/Profile.js";
import { createNotificationsBulk } from "../services/notificationService.js";
import { sendEmail } from "../services/mail/resendClient.js";
import { sendSms } from "../services/sms/termiiClient.js";

function splitCsv(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function uniqueStrings(values) {
  return [...new Set(values.map((v) => String(v).trim()).filter(Boolean))];
}

function formatDateLabel(value) {
  if (!value) return "";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function buildVoteReminderMessage({ groupName, voteTitle, endsAt }) {
  const safeGroup = groupName?.trim() || "your group";
  const safeTitle = voteTitle?.trim() || "group vote";
  const endLabel = endsAt ? formatDateLabel(endsAt) : "";
  const suffix = endLabel ? ` Voting ends on ${endLabel}.` : "";
  return `Please vote on "${safeTitle}" for ${safeGroup}.${suffix}`;
}

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

  let myChoiceByVoteId = new Map();
  let myVoteByVoteId = new Map();
  if (req.user?.profileId && votes.length > 0) {
    const voteIds = votes.map((vote) => vote._id);
    const responses = await GroupVoteResponseModel.find(
      { voteId: { $in: voteIds }, userId: req.user.profileId },
      { voteId: 1, choice: 1, updatedAt: 1, createdAt: 1 },
    ).lean();
    myChoiceByVoteId = new Map(
      responses.map((response) => [String(response.voteId), response.choice]),
    );
    myVoteByVoteId = new Map(
      responses.map((response) => [
        String(response.voteId),
        {
          choice: response.choice,
          respondedAt: response.updatedAt ?? response.createdAt ?? null,
        },
      ]),
    );
  }

  const normalized = votes.map((vote) => ({
    ...vote,
    myChoice: myChoiceByVoteId.get(String(vote._id)) ?? null,
    myVote: myVoteByVoteId.get(String(vote._id)) ?? null,
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

export const respondToGroupVote = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const group = req.group;
  const voteId = String(req.params.voteId || "").trim();
  if (!voteId) return next(new AppError("Vote id is required", 400));

  const choice = String(req.body?.choice || "").toLowerCase();
  if (!GroupVoteResponseChoices.includes(choice)) {
    return next(new AppError("choice must be yes or no", 400));
  }

  const vote = await GroupVoteModel.findOne({ _id: voteId, groupId: group._id });
  if (!vote) return next(new AppError("Vote not found", 404));
  if (vote.status !== "active") {
    return next(new AppError("Voting is closed for this poll", 403));
  }

  const response = await GroupVoteResponseModel.findOneAndUpdate(
    { voteId: vote._id, groupId: group._id, userId: req.user.profileId },
    {
      $set: { choice },
      $setOnInsert: {
        voteId: vote._id,
        groupId: group._id,
        userId: req.user.profileId,
      },
    },
    { new: true, upsert: true, runValidators: true, setDefaultsOnInsert: true },
  ).lean();

  const [yesVotes, noVotes] = await Promise.all([
    GroupVoteResponseModel.countDocuments({ voteId: vote._id, choice: "yes" }),
    GroupVoteResponseModel.countDocuments({ voteId: vote._id, choice: "no" }),
  ]);

  const updatedVote = await GroupVoteModel.findByIdAndUpdate(
    vote._id,
    { $set: { yesVotes, noVotes } },
    { new: true },
  ).lean();

  return sendSuccess(res, {
    statusCode: 200,
    message: "Vote recorded",
    data: { vote: updatedVote ?? vote, response },
  });
});

export const deleteGroupVote = catchAsync(async (req, res, next) => {
  const group = req.group;
  const voteId = String(req.params.voteId || "").trim();
  if (!voteId) return next(new AppError("Vote id is required", 400));

  const vote = await GroupVoteModel.findOne({ _id: voteId, groupId: group._id });
  if (!vote) return next(new AppError("Vote not found", 404));

  await Promise.all([
    GroupVoteResponseModel.deleteMany({ voteId: vote._id }),
    GroupVoteModel.deleteOne({ _id: vote._id }),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Vote deleted",
    data: { voteId: String(vote._id) },
  });
});

export const listGroupVoteParticipants = catchAsync(async (req, res, next) => {
  const group = req.group;
  const voteId = String(req.params.voteId || "").trim();
  if (!voteId) return next(new AppError("Vote id is required", 400));

  const vote = await GroupVoteModel.findOne({ _id: voteId, groupId: group._id }).lean();
  if (!vote) return next(new AppError("Vote not found", 404));

  const memberships = await GroupMembershipModel.find(
    { groupId: group._id, status: "active" },
    { userId: 1, role: 1, memberSerial: 1 },
  ).lean();

  if (memberships.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      data: {
        vote,
        participants: [],
        totalMembers: 0,
        votedCount: 0,
        pendingCount: 0,
      },
    });
  }

  const userIds = uniqueStrings(memberships.map((m) => String(m.userId)));

  const [profiles, responses] = await Promise.all([
    ProfileModel.find(
      { _id: { $in: userIds } },
      { fullName: 1, email: 1, phone: 1, avatar: 1 },
    ).lean(),
    GroupVoteResponseModel.find(
      { voteId: vote._id, userId: { $in: userIds } },
      { userId: 1, choice: 1, updatedAt: 1, createdAt: 1 },
    ).lean(),
  ]);

  const profileById = new Map(profiles.map((p) => [String(p._id), p]));
  const responseById = new Map(responses.map((r) => [String(r.userId), r]));

  const participants = memberships.map((membership) => {
    const userId = String(membership.userId);
    const profile = profileById.get(userId);
    const response = responseById.get(userId);

    return {
      userId,
      name: profile?.fullName ?? "Member",
      email: profile?.email ?? null,
      phone: profile?.phone ?? null,
      avatarUrl: profile?.avatar?.url ?? null,
      memberSerial: membership.memberSerial ?? null,
      role: membership.role ?? "member",
      status: response ? "voted" : "pending",
      choice: response?.choice ?? null,
      respondedAt: response?.updatedAt ?? response?.createdAt ?? null,
    };
  });

  const votedCount = participants.filter((p) => p.status === "voted").length;
  const pendingCount = participants.length - votedCount;

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      vote,
      participants,
      totalMembers: participants.length,
      votedCount,
      pendingCount,
    },
  });
});

export const notifyGroupVoteMembers = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const group = req.group;
  const voteId = String(req.params.voteId || "").trim();
  if (!voteId) return next(new AppError("Vote id is required", 400));

  const vote = await GroupVoteModel.findOne({ _id: voteId, groupId: group._id }).lean();
  if (!vote) return next(new AppError("Vote not found", 404));

  const target = String(req.body?.target ?? "pending").trim().toLowerCase();
  if (!["pending", "all"].includes(target)) {
    return next(new AppError("target must be pending or all", 400));
  }

  const sendEmailFlag = Boolean(req.body?.sendEmail ?? false);
  const sendSmsFlag = Boolean(req.body?.sendSMS ?? false);
  const sendNotificationFlag = Boolean(req.body?.sendNotification ?? true);

  if (!sendEmailFlag && !sendSmsFlag && !sendNotificationFlag) {
    return next(new AppError("Select at least one delivery method", 400));
  }

  if (sendSmsFlag && (!process.env.TERMII_API_KEY || !process.env.TERMII_SENDER_ID)) {
    return next(new AppError("SMS is not configured on the server", 400));
  }

  const memberships = await GroupMembershipModel.find(
    { groupId: group._id, status: "active" },
    { userId: 1 },
  ).lean();

  if (memberships.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "No active members found",
      data: { totalRecipients: 0, channels: {}, failures: [] },
    });
  }

  const memberIds = uniqueStrings(memberships.map((m) => String(m.userId)));
  let targetUserIds = memberIds;

  if (target === "pending") {
    const respondedIds = await GroupVoteResponseModel.find(
      { voteId: vote._id },
      { userId: 1 },
    ).distinct("userId");
    const respondedSet = new Set(respondedIds.map((id) => String(id)));
    targetUserIds = memberIds.filter((id) => !respondedSet.has(String(id)));
  }

  if (targetUserIds.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "No members to notify",
      data: { totalRecipients: 0, channels: {}, failures: [] },
    });
  }

  const profiles = await ProfileModel.find(
    { _id: { $in: targetUserIds } },
    { fullName: 1, email: 1, phone: 1 },
  ).lean();

  const profileById = new Map(profiles.map((p) => [String(p._id), p]));
  const title = `${group.groupName} Vote Reminder`;
  const message = buildVoteReminderMessage({
    groupName: group.groupName,
    voteTitle: vote.title,
    endsAt: vote.endsAt,
  });

  const result = {
    totalRecipients: targetUserIds.length,
    target,
    channels: {
      email: { requested: sendEmailFlag, attempted: 0, sent: 0, failed: 0, skipped: 0 },
      sms: { requested: sendSmsFlag, attempted: 0, sent: 0, failed: 0, skipped: 0 },
      notification: { requested: sendNotificationFlag, attempted: 0, sent: 0, failed: 0, skipped: 0 },
    },
    failures: [],
  };

  if (sendNotificationFlag) {
    result.channels.notification.attempted = targetUserIds.length;
    try {
      const notifications = await createNotificationsBulk({
        userIds: targetUserIds,
        title,
        message,
        type: "group_vote_reminder",
        metadata: {
          groupId: group._id,
          voteId: vote._id,
          voteTitle: vote.title,
          target,
        },
      });
      result.channels.notification.sent = notifications.length;
      result.channels.notification.failed =
        targetUserIds.length - notifications.length;
      if (result.channels.notification.failed > 0) {
        result.failures.push({
          channel: "notification",
          to: "some recipients",
          error: "Some notifications could not be created",
        });
      }
    } catch (err) {
      result.channels.notification.failed = targetUserIds.length;
      result.failures.push({
        channel: "notification",
        to: "recipients",
        error: err ? String(err?.message ?? err) : "Notification creation failed",
      });
    }
  }

  if (sendEmailFlag) {
    const emailTargets = targetUserIds
      .map((id) => profileById.get(String(id)))
      .filter((profile) => Boolean(profile?.email))
      .map((profile) => ({
        to: profile.email,
        subject: title,
      }));

    result.channels.email.attempted = emailTargets.length;
    result.channels.email.skipped = targetUserIds.length - emailTargets.length;

    if (emailTargets.length > 0) {
      const html = `
        <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; line-height: 1.6; color: #111827;">
          <h2 style="margin: 0 0 12px; font-size: 18px;">${escapeHtml(title)}</h2>
          <p style="margin: 0; font-size: 14px;">${escapeHtml(message)}</p>
        </div>
      `.trim();

      const settles = await Promise.allSettled(
        emailTargets.map((target) =>
          sendEmail({
            to: target.to,
            subject: target.subject,
            html,
            text: message,
          }),
        ),
      );

      settles.forEach((s, idx) => {
        if (s.status === "fulfilled") {
          result.channels.email.sent += 1;
        } else {
          result.channels.email.failed += 1;
          result.failures.push({
            channel: "email",
            to: emailTargets[idx]?.to,
            error: s.reason ? String(s.reason?.message ?? s.reason) : "Email failed",
          });
        }
      });
    }
  }

  if (sendSmsFlag) {
    const smsTargets = [];
    targetUserIds.forEach((id) => {
      const profile = profileById.get(String(id));
      const phones = profile?.phone ? splitCsv(profile.phone) : [];
      if (phones.length === 0) {
        result.channels.sms.skipped += 1;
      } else {
        phones.forEach((phone) => smsTargets.push({ to: phone, message }));
      }
    });

    result.channels.sms.attempted = smsTargets.length;

    if (smsTargets.length > 0) {
      const settles = await Promise.allSettled(
        smsTargets.map((target) => sendSms({ to: target.to, message: target.message })),
      );

      settles.forEach((s, idx) => {
        if (s.status === "fulfilled") {
          result.channels.sms.sent += 1;
        } else {
          result.channels.sms.failed += 1;
          result.failures.push({
            channel: "sms",
            to: smsTargets[idx]?.to,
            error: s.reason ? String(s.reason?.message ?? s.reason) : "SMS failed",
          });
        }
      });
    }
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "Vote notifications dispatched",
    data: result,
  });
});
