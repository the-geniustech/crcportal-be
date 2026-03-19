import { Schema, ObjectId, model } from "./_shared.js";

export const RefreshTokenSchema = new Schema(
  {
    userId: {
      type: ObjectId,
      ref: "User",
      required: true,
      index: true,
    },
    jti: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    tokenHash: {
      type: String,
      required: true,
      select: false,
    },
    expiresAt: {
      type: Date,
      required: true,
      index: true,
    },
    revokedAt: {
      type: Date,
      default: null,
      index: true,
    },
    replacedByJti: {
      type: String,
      default: null,
    },
    createdByIp: {
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

RefreshTokenSchema.index({ userId: 1, revokedAt: 1, expiresAt: 1 });

export const RefreshTokenModel = model("RefreshToken", RefreshTokenSchema);
