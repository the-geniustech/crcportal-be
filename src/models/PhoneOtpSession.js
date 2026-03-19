import { Schema, model } from "./_shared.js";

export const PhoneOtpSessionSchema = new Schema(
  {
    pinId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    phone: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    fullName: {
      type: String,
      default: null,
      trim: true,
    },
    groupId: {
      type: Schema.Types.ObjectId,
      ref: "Group",
      default: null,
    },
    otpHash: {
      type: String,
      required: true,
      select: false,
    },
    expiresAt: {
      type: Date,
      required: true,
      expires: 0,
      index: true,
    },
    sentAt: {
      type: Date,
      default: Date.now,
    },
    attempts: {
      type: Number,
      default: 0,
    },
    consumedAt: {
      type: Date,
      default: null,
    },
    ip: {
      type: String,
      default: null,
    },
    userAgent: {
      type: String,
      default: null,
    },
  },
  { timestamps: true },
);

PhoneOtpSessionSchema.index({ phone: 1, consumedAt: 1, expiresAt: 1 });

export const PhoneOtpSessionModel = model("PhoneOtpSession", PhoneOtpSessionSchema);
