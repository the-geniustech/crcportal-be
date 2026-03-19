import { Schema, ObjectId, model } from "./_shared.js";

const MeetingActionItemSchema = new Schema(
  {
    task: { type: String, required: true, trim: true },
    assignee: { type: String, default: "", trim: true },
    dueDate: { type: String, default: "", trim: true },
  },
  { _id: false },
);

export const MeetingMinutesSchema = new Schema(
  {
    meetingId: {
      type: ObjectId,
      ref: "Meeting",
      required: true,
      unique: true,
      index: true,
    },
    content: { type: String, required: true, trim: true },
    attendeesCount: { type: Number, default: 0, min: 0 },
    decisionsMade: { type: [String], default: [] },
    actionItems: { type: [MeetingActionItemSchema], default: [] },
  },
  { timestamps: true },
);

export const MeetingMinutesModel = model("MeetingMinutes", MeetingMinutesSchema);
