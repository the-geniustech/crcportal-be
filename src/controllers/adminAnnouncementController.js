import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { sendEmail } from "../services/mail/resendClient.js";
import { sendSms } from "../services/sms/termiiClient.js";
import { CommunicationLogModel } from "../models/CommunicationLog.js";
import { createNotificationsBulk } from "../services/notificationService.js";
import { hasUserRole, normalizeUserRoles, pickPrimaryRole } from "../utils/roles.js";

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

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (hasUserRole(req.user, "admin")) return null;

  if (!hasUserRole(req.user, "groupCoordinator")) {
    throw new AppError("Insufficient permissions", 403);
  }

  const coordinatorMemberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return coordinatorMemberships.map((m) => String(m.groupId));
}

function buildAnnouncementEmail({ title, message, senderName }) {
  const safeTitle = title?.trim() || "Announcement";
  const safeSender = senderName?.trim() || "CRC Connect";
  const safeMessage = message?.trim() || "";

  const text = `${safeTitle}\n\n${safeMessage}\n\nâ€” ${safeSender}`;
  const html = `
    <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; line-height: 1.6; color: #111827;">
      <h2 style="margin: 0 0 12px; font-size: 18px;">${escapeHtml(safeTitle)}</h2>
      <div style="white-space: pre-wrap; font-size: 14px;">${escapeHtml(safeMessage)}</div>
      <div style="margin-top: 20px; font-size: 12px; color: #6B7280;">â€” ${escapeHtml(safeSender)}</div>
    </div>
  `.trim();

  return { subject: safeTitle, text, html };
}

function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

export const createAdminAnnouncement = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const title = typeof req.body?.title === "string" ? req.body.title : "";
  const message = typeof req.body?.message === "string" ? req.body.message : "";
  const senderName = typeof req.body?.senderName === "string" ? req.body.senderName : "CRC Connect Admin";

  const target = String(req.body?.target ?? "all").trim().toLowerCase();
  const sendEmailFlag = Boolean(req.body?.sendEmail ?? true);
  const sendSmsFlag = Boolean(req.body?.sendSMS ?? false);
  const sendNotificationFlag = Boolean(req.body?.sendNotification ?? false);

  if (!message.trim()) return next(new AppError("Message is required", 400));
  if (!sendEmailFlag && !sendSmsFlag && !sendNotificationFlag) {
    return next(new AppError("Select at least one delivery method", 400));
  }

  const requestedGroupNumbersRaw = Array.isArray(req.body?.groupNumbers)
    ? req.body.groupNumbers
    : Array.isArray(req.body?.selectedGroups)
      ? req.body.selectedGroups
      : [];

  const requestedGroupNumbers = requestedGroupNumbersRaw
    .map((n) => Number(n))
    .filter((n) => Number.isFinite(n));

  if (target === "selected" && requestedGroupNumbers.length === 0) {
    return next(new AppError("Please select at least one group", 400));
  }

  const manageableGroupIds = await getManageableGroupIds(req);
  const filter = {};

  if (manageableGroupIds) {
    filter._id = { $in: manageableGroupIds };
  }
  if (target === "selected") {
    filter.groupNumber = { $in: uniqueStrings(requestedGroupNumbers).map((n) => Number(n)) };
  }

  const groups = await GroupModel.find(filter)
    .sort({ groupNumber: 1 })
    .lean();

  if (groups.length === 0) {
    return next(new AppError("No groups found for the selected scope", 404));
  }

  const coordinatorEmails = uniqueStrings(
    groups.flatMap((g) => splitCsv(g.coordinatorEmail)),
  );
  const coordinatorPhones = uniqueStrings(
    groups.flatMap((g) => splitCsv(g.coordinatorPhone)),
  );

  const result = {
    target,
    groupsMatched: groups.length,
    channels: {
      email: { requested: sendEmailFlag, attempted: 0, sent: 0, failed: 0, skipped: 0 },
      sms: { requested: sendSmsFlag, attempted: 0, sent: 0, failed: 0, skipped: 0 },
      notification: { requested: sendNotificationFlag, attempted: 0, sent: 0, failed: 0, skipped: 0 },
    },
    failures: [],
  };

  if (sendEmailFlag) {
    if (coordinatorEmails.length === 0) {
      result.channels.email.skipped = groups.length;
    } else {
      const emailBody = buildAnnouncementEmail({ title, message, senderName });
      result.channels.email.attempted = coordinatorEmails.length;

      const settles = await Promise.allSettled(
        coordinatorEmails.map((email) =>
          sendEmail({
            to: email,
            subject: emailBody.subject,
            html: emailBody.html,
            text: emailBody.text,
          }),
        ),
      );

      for (let i = 0; i < settles.length; i++) {
        const s = settles[i];
        if (s.status === "fulfilled") {
          result.channels.email.sent += 1;
        } else {
          result.channels.email.failed += 1;
          result.failures.push({
            channel: "email",
            to: coordinatorEmails[i],
            error: s.reason ? String(s.reason?.message ?? s.reason) : "Unknown error",
          });
        }
      }
    }
  }

  if (sendSmsFlag) {
    if (!process.env.TERMII_API_KEY || !process.env.TERMII_SENDER_ID) {
      return next(new AppError("SMS is not configured on the server", 400));
    }

    if (coordinatorPhones.length === 0) {
      result.channels.sms.skipped = groups.length;
    } else {
      result.channels.sms.attempted = coordinatorPhones.length;
      const settles = await Promise.allSettled(
        coordinatorPhones.map((phone) => sendSms({ to: phone, message })),
      );

      for (let i = 0; i < settles.length; i++) {
        const s = settles[i];
        if (s.status === "fulfilled") {
          result.channels.sms.sent += 1;
        } else {
          result.channels.sms.failed += 1;
          result.failures.push({
            channel: "sms",
            to: coordinatorPhones[i],
            error: s.reason ? String(s.reason?.message ?? s.reason) : "Unknown error",
          });
        }
      }
    }
  }

  if (sendNotificationFlag) {
    const groupIds = groups.map((g) => g._id);
    const coordinatorIds = uniqueStrings(
      groups
        .map((g) => g.coordinatorId)
        .filter(Boolean),
    );

    const membershipCoordinatorIds = await GroupMembershipModel.find({
      groupId: { $in: groupIds },
      role: "coordinator",
      status: "active",
    }).distinct("userId");

    const targetUserIds = uniqueStrings([
      ...coordinatorIds,
      ...membershipCoordinatorIds,
    ]);

    if (targetUserIds.length === 0) {
      result.channels.notification.skipped = groups.length;
    } else {
      result.channels.notification.attempted = targetUserIds.length;
      try {
        const notifications = await createNotificationsBulk({
          userIds: targetUserIds,
          title: title?.trim() || "Announcement",
          message,
          type: "announcement",
          metadata: {
            target,
            groupNumbers: requestedGroupNumbers,
          },
        });

        result.channels.notification.sent = notifications.length;
        result.channels.notification.failed =
          targetUserIds.length - notifications.length;

        if (result.channels.notification.failed > 0) {
          result.failures.push({
            channel: "notification",
            to: "coordinators",
            error: "Some notifications could not be created",
          });
        }
      } catch (err) {
        result.channels.notification.failed = targetUserIds.length;
        result.failures.push({
          channel: "notification",
          to: "coordinators",
          error: err ? String(err?.message ?? err) : "Notification creation failed",
        });
      }
    }
  }

  CommunicationLogModel.create({
    createdBy: req.user.profileId,
    creatorRole: pickPrimaryRole(normalizeUserRoles(req.user)),
    kind: "announcement",
    target,
    groupNumbers:
      target === "selected"
        ? requestedGroupNumbers
            .map((n) => Number(n))
            .filter((n) => Number.isFinite(n))
        : [],
    title: title?.trim() || null,
    message,
    channels: result.channels,
    failures: result.failures,
  }).catch(() => {});

  return sendSuccess(res, {
    statusCode: 201,
    data: { announcement: result },
    message: "Announcement dispatched",
  });
});


