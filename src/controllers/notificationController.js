import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { NotificationStatuses } from "../models/Notification.js";
import {
  listNotifications,
  markAllNotificationsRead,
  markNotificationRead,
} from "../services/notificationService.js";

function normalizeStatus(status) {
  if (!status) return null;
  const value = String(status).trim().toLowerCase();
  return NotificationStatuses.includes(value) ? value : null;
}

export const listMyNotifications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const status = normalizeStatus(req.query?.status);
  const page = req.query?.page;
  const limit = req.query?.limit;

  const { notifications, total, unread, page: safePage, limit: safeLimit } =
    await listNotifications({
      userId: req.user.profileId,
      status,
      page,
      limit,
    });

  return sendSuccess(res, {
    statusCode: 200,
    results: notifications.length,
    total,
    page: safePage,
    limit: safeLimit,
    data: { notifications, unread },
  });
});

export const listMyUnreadNotifications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const page = req.query?.page;
  const limit = req.query?.limit;

  const { notifications, total, unread, page: safePage, limit: safeLimit } =
    await listNotifications({
      userId: req.user.profileId,
      status: "unread",
      page,
      limit,
    });

  return sendSuccess(res, {
    statusCode: 200,
    results: notifications.length,
    total,
    page: safePage,
    limit: safeLimit,
    data: { notifications, unread },
  });
});

export const markMyNotificationRead = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const notificationId = req.params?.id;
  if (!notificationId) return next(new AppError("Missing notification id", 400));

  const notification = await markNotificationRead({
    userId: req.user.profileId,
    notificationId,
  });

  if (!notification) return next(new AppError("Notification not found", 404));

  return sendSuccess(res, { statusCode: 200, data: { notification } });
});

export const markAllMyNotificationsRead = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const result = await markAllNotificationsRead({ userId: req.user.profileId });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      ok: true,
      matchedCount: result.matchedCount ?? result.n ?? 0,
      modifiedCount: result.modifiedCount ?? result.nModified ?? 0,
    },
  });
});
