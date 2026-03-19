import { Schema, ObjectId, model } from "./_shared.js";

export const AttendanceStatuses = ["present", "absent", "excused", "late"];

export const MeetingAttendanceSchema = new Schema(
  {
    meetingId: {
      type: ObjectId,
      ref: "Meeting",
      required: true,
      index: true,
    },
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },

    status: {
      type: String,
      enum: AttendanceStatuses,
      required: true,
      index: true,
    },
    checkInTime: { type: String, default: null, trim: true },
    notes: { type: String, default: null, trim: true },
  },
  { timestamps: true },
);

MeetingAttendanceSchema.index({ meetingId: 1, userId: 1 }, { unique: true });

export const MeetingAttendanceModel = model(
  "MeetingAttendance",
  MeetingAttendanceSchema,
);
