import { Schema, ObjectId, model } from "./_shared.js";

const ChannelSchema = new Schema(
  {
    requested: { type: Boolean, default: false },
    attempted: { type: Number, default: 0, min: 0 },
    sent: { type: Number, default: 0, min: 0 },
    failed: { type: Number, default: 0, min: 0 },
    skipped: { type: Number, default: 0, min: 0 },
  },
  { _id: false },
);

const FailureSchema = new Schema(
  {
    channel: { type: String, required: true, trim: true },
    to: { type: String, required: true, trim: true },
    error: { type: String, required: true, trim: true },
  },
  { _id: false },
);

export const CommunicationLogSchema = new Schema(
  {
    createdBy: { type: ObjectId, ref: "Profile", required: true, index: true },
    creatorRole: { type: String, required: true, trim: true, index: true },

    kind: { type: String, default: "broadcast", trim: true, index: true }, // announcement | sms
    target: { type: String, required: true, trim: true, index: true }, // all | selected | defaulters | coordinators
    groupNumbers: { type: [Number], default: [], index: true },

    title: { type: String, default: null, trim: true },
    message: { type: String, required: true, trim: true },

    channels: {
      email: { type: ChannelSchema, default: () => ({}) },
      sms: { type: ChannelSchema, default: () => ({}) },
      notification: { type: ChannelSchema, default: () => ({}) },
    },

    failures: { type: [FailureSchema], default: [] },
  },
  { timestamps: true },
);

CommunicationLogSchema.index({ createdAt: -1 });

export const CommunicationLogModel = model("CommunicationLog", CommunicationLogSchema);

