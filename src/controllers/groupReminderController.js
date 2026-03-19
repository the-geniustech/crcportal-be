import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupReminderSettingsModel } from "../models/GroupReminderSettings.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ContributionModel } from "../models/Contribution.js";
import { NotificationModel } from "../models/Notification.js";

const DEFAULT_SETTINGS = {
  autoReminders: true,
  daysBeforeDue: 3,
  overdueReminders: true,
  meetingReminders: true,
};

function pickSettings(body = {}) {
  const updates = {};
  for (const key of Object.keys(DEFAULT_SETTINGS)) {
    if (Object.prototype.hasOwnProperty.call(body, key)) {
      updates[key] = body[key];
    }
  }
  return updates;
}

function formatMonthLabel(year, month) {
  return new Date(Date.UTC(year, month - 1, 1)).toLocaleDateString("en-US", {
    month: "long",
    year: "numeric",
  });
}

export const getGroupReminderSettings = catchAsync(async (req, res) => {
  const group = req.group;

  let settings = await GroupReminderSettingsModel.findOne({ groupId: group._id }).lean();
  if (!settings) {
    settings = await GroupReminderSettingsModel.create({
      groupId: group._id,
      ...DEFAULT_SETTINGS,
    }).then((doc) => doc.toObject());
  }

  return sendSuccess(res, { statusCode: 200, data: { settings } });
});

export const updateGroupReminderSettings = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const group = req.group;
  const updates = pickSettings(req.body || {});
  if (Object.keys(updates).length === 0) {
    return next(new AppError("No reminder settings updates provided", 400));
  }

  if (Object.prototype.hasOwnProperty.call(updates, "daysBeforeDue")) {
    const value = Number(updates.daysBeforeDue);
    if (!Number.isFinite(value) || value < 1 || value > 30) {
      return next(new AppError("daysBeforeDue must be a number between 1 and 30", 400));
    }
    updates.daysBeforeDue = value;
  }

  if (Object.prototype.hasOwnProperty.call(updates, "autoReminders")) {
    updates.autoReminders = Boolean(updates.autoReminders);
  }
  if (Object.prototype.hasOwnProperty.call(updates, "overdueReminders")) {
    updates.overdueReminders = Boolean(updates.overdueReminders);
  }
  if (Object.prototype.hasOwnProperty.call(updates, "meetingReminders")) {
    updates.meetingReminders = Boolean(updates.meetingReminders);
  }

  updates.updatedBy = req.user.profileId ?? null;

  const settings = await GroupReminderSettingsModel.findOneAndUpdate(
    { groupId: group._id },
    { $set: updates, $setOnInsert: { groupId: group._id } },
    { new: true, upsert: true, runValidators: true, setDefaultsOnInsert: true },
  ).lean();

  return sendSuccess(res, { statusCode: 200, data: { settings } });
});

export const sendGroupContributionReminders = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const group = req.group;
  const now = new Date();
  const year = Number.isFinite(Number(req.body?.year)) ? Number(req.body?.year) : now.getFullYear();
  const month = Number.isFinite(Number(req.body?.month))
    ? Number(req.body?.month)
    : now.getMonth() + 1;

  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return next(new AppError("Invalid year or month provided", 400));
  }

  const memberships = await GroupMembershipModel.find(
    { groupId: group._id, status: "active" },
    { userId: 1 },
  ).lean();

  if (memberships.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "No active members to remind",
      data: { sent: 0, totalMembers: 0, pendingMembers: 0 },
    });
  }

  const memberIds = memberships.map((m) => m.userId).filter(Boolean);

  const contributions = await ContributionModel.find(
    {
      groupId: group._id,
      userId: { $in: memberIds },
      year,
      month,
      contributionType: "regular",
    },
    { userId: 1, status: 1, amount: 1 },
  ).lean();

  const paidStatuses = new Set(["verified", "completed"]);
  const paidByUserId = new Set(
    contributions
      .filter((c) => paidStatuses.has(String(c.status)))
      .map((c) => String(c.userId)),
  );

  const pendingMembers = memberships.filter((m) => !paidByUserId.has(String(m.userId)));
  if (pendingMembers.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "All members are up to date",
      data: { sent: 0, totalMembers: memberships.length, pendingMembers: 0 },
    });
  }

  const amount = Number(group.monthlyContribution ?? 0);
  const label = formatMonthLabel(year, month);
  const title = `${group.groupName} Contribution Reminder`;
  const message = amount
    ? `Your contribution for ${group.groupName} (${label}) is due. Amount: NGN${amount.toLocaleString()}.`
    : `Your contribution for ${group.groupName} (${label}) is due.`;

  const notifications = pendingMembers.map((member) => ({
    userId: member.userId,
    title,
    message,
    type: "group_contribution_reminder",
    status: "unread",
    metadata: {
      groupId: group._id,
      groupName: group.groupName,
      year,
      month,
    },
  }));

  await NotificationModel.insertMany(notifications);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Contribution reminders sent",
    data: {
      sent: notifications.length,
      totalMembers: memberships.length,
      pendingMembers: pendingMembers.length,
    },
  });
});
