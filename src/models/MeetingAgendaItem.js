import { Schema, ObjectId, model } from "./_shared.js";

export const MeetingAgendaItemSchema = new Schema(
  {
    meetingId: {
      type: ObjectId,
      ref: "Meeting",
      required: true,
      index: true,
    },
    title: { type: String, required: true, trim: true },
    description: { type: String, default: "", trim: true },
    durationMinutes: { type: Number, required: true, min: 1 },
    orderIndex: { type: Number, required: true, min: 0, index: true },
  },
  { timestamps: true },
);

MeetingAgendaItemSchema.index({ meetingId: 1, orderIndex: 1 }, { unique: true });

export const MeetingAgendaItemModel = model(
  "MeetingAgendaItem",
  MeetingAgendaItemSchema,
);
