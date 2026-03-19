import { NotificationModel } from "../models/Notification.js";
import { emitToUser } from "../socket.js";

function normalizeMetadata(metadata) {
  if (!metadata) return {};
  if (typeof metadata === "object") return metadata;
  return { value: metadata };
}

export async function createNotification({
  userId,
  title,
  message,
  type,
  metadata,
}) {
  const notification = await NotificationModel.create({
    userId,
    title,
    message,
    type,
    status: "unread",
    metadata: normalizeMetadata(metadata),
  });

  emitToUser(userId, "notification:new", { notification });
  return notification;
}

export async function createNotificationsBulk({
  userIds,
  title,
  message,
  type,
  metadata,
}) {
  const uniqueUserIds = [...new Set(userIds.map((id) => String(id)).filter(Boolean))];
  if (uniqueUserIds.length === 0) return [];

  const payload = uniqueUserIds.map((userId) => ({
    userId,
    title,
    message,
    type,
    status: "unread",
    metadata: normalizeMetadata(metadata),
  }));

  const notifications = await NotificationModel.insertMany(payload, {
    ordered: false,
  });

  for (const notification of notifications) {
    emitToUser(notification.userId, "notification:new", { notification });
  }

  return notifications;
}

export async function listNotifications({
  userId,
  status,
  page = 1,
  limit = 50,
}) {
  const filter = { userId };
  if (status) filter.status = status;

  const safePage = Math.max(1, Number(page) || 1);
  const safeLimit = Math.min(200, Math.max(1, Number(limit) || 50));
  const skip = (safePage - 1) * safeLimit;

  const [notifications, total, unread] = await Promise.all([
    NotificationModel.find(filter).sort({ createdAt: -1 }).skip(skip).limit(safeLimit),
    NotificationModel.countDocuments(filter),
    NotificationModel.countDocuments({ userId, status: "unread" }),
  ]);

  return { notifications, total, unread, page: safePage, limit: safeLimit };
}

export async function markNotificationRead({ userId, notificationId }) {
  const notification = await NotificationModel.findOne({
    _id: notificationId,
    userId,
  });

  if (!notification) return null;

  if (notification.status !== "read") {
    notification.status = "read";
    await notification.save();
    emitToUser(userId, "notification:read", { id: notificationId });
    emitToUser(userId, "notification:updated", { notification });
  }

  return notification;
}

export async function markAllNotificationsRead({ userId }) {
  const result = await NotificationModel.updateMany(
    { userId, status: "unread" },
    { status: "read" },
  );

  emitToUser(userId, "notification:updated", {
    allRead: true,
  });

  return result;
}
