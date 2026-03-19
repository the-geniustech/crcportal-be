import { Schema, ObjectId, model } from "./_shared.js";

export const GuarantorNotificationSchema = new Schema(
  {
    guarantorId: {
      type: ObjectId,
      ref: "LoanGuarantor",
      required: true,
      index: true,
    },
    notificationType: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    message: { type: String, required: true, trim: true },
    sentVia: { type: [String], default: [] },
    readAt: { type: Date, default: null, index: true },
  },
  { timestamps: true },
);

GuarantorNotificationSchema.index({ guarantorId: 1, createdAt: -1 });
GuarantorNotificationSchema.index({ guarantorId: 1, readAt: 1, createdAt: -1 });

export const GuarantorNotificationModel = model(
  "GuarantorNotification",
  GuarantorNotificationSchema,
);
