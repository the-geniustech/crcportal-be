import { Schema, ObjectId, model } from "./_shared.js";

export const NotificationPreferenceSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, unique: true, index: true },

    emailNotifications: { type: Boolean, default: true },
    smsNotifications: { type: Boolean, default: true },
    pushNotifications: { type: Boolean, default: true },

    paymentReminders: { type: Boolean, default: true },
    groupUpdates: { type: Boolean, default: true },
    loanUpdates: { type: Boolean, default: true },
    meetingReminders: { type: Boolean, default: true },
    marketingEmails: { type: Boolean, default: false },
  },
  { timestamps: true },
);

export const NotificationPreferenceModel = model(
  "NotificationPreference",
  NotificationPreferenceSchema,
);

