import { Schema, ObjectId, model } from "./_shared.js";

export const NotificationStatuses = ["read", "unread"];

export const NotificationSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    title: { type: String, required: true, trim: true },
    message: { type: String, required: true, trim: true },
    type: { type: String, required: true, trim: true, index: true },
    status: {
      type: String,
      enum: NotificationStatuses,
      default: "unread",
      index: true,
    },
    metadata: { type: Schema.Types.Mixed, default: {} },
  },
  { timestamps: true },
);

NotificationSchema.index({ userId: 1, status: 1, createdAt: -1 });
NotificationSchema.index({ userId: 1, createdAt: -1 });

export const NotificationModel = model("Notification", NotificationSchema);
