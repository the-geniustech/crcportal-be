import { Schema, ObjectId, model } from "./_shared.js";

export const GroupReminderSettingsSchema = new Schema(
  {
    groupId: { type: ObjectId, ref: "Group", required: true, unique: true, index: true },
    autoReminders: { type: Boolean, default: true },
    daysBeforeDue: { type: Number, default: 3, min: 1, max: 30 },
    overdueReminders: { type: Boolean, default: true },
    meetingReminders: { type: Boolean, default: true },
    updatedBy: { type: ObjectId, ref: "Profile", default: null },
  },
  { timestamps: true },
);

export const GroupReminderSettingsModel = model(
  "GroupReminderSettings",
  GroupReminderSettingsSchema,
);
