import { Schema, ObjectId, model } from "./_shared.js";

export const MeetingRsvpStatuses = [
  "pending",
  "attending",
  "not_attending",
  "maybe",
];

export const MeetingRsvpSchema = new Schema(
  {
    meetingId: { type: ObjectId, ref: "Meeting", required: true, index: true },
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    status: {
      type: String,
      enum: MeetingRsvpStatuses,
      required: true,
      default: "pending",
      index: true,
    },
  },
  { timestamps: true },
);

MeetingRsvpSchema.index({ meetingId: 1, userId: 1 }, { unique: true });
MeetingRsvpSchema.index({ meetingId: 1, status: 1 });

export const MeetingRsvpModel = model("MeetingRsvp", MeetingRsvpSchema);

