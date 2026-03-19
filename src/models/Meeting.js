import { Schema, ObjectId, model } from "./_shared.js";

export const MeetingTypes = ["physical", "zoom", "google_meet"];
export const MeetingStatuses = ["scheduled", "completed", "cancelled"];

export const MeetingSchema = new Schema(
  {
    groupId: { type: ObjectId, ref: "Group", required: true, index: true },

    title: { type: String, required: true, trim: true },
    description: { type: String, default: "", trim: true },

    meetingType: { type: String, enum: MeetingTypes, required: true },
    location: { type: String, default: null, trim: true },

    meetingLink: { type: String, default: null, trim: true },
    meetingId: { type: String, default: null, trim: true },
    meetingPassword: { type: String, default: null, trim: true },

    scheduledDate: { type: Date, required: true, index: true },
    durationMinutes: { type: Number, required: true, min: 1 },

    status: {
      type: String,
      enum: MeetingStatuses,
      default: "scheduled",
      index: true,
    },
  },
  { timestamps: true },
);

MeetingSchema.index({ groupId: 1, scheduledDate: 1 });
MeetingSchema.index({ status: 1, scheduledDate: 1 });

export const MeetingModel = model("Meeting", MeetingSchema);
