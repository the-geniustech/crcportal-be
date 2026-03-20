import { Schema, ObjectId, model } from "./_shared.js";

export const LoginHistorySchema = new Schema(
  {
    userId: { type: ObjectId, ref: "User", required: true, index: true },
    method: {
      type: String,
      enum: ["password", "phone_otp", "two_factor"],
      default: "password",
      index: true,
    },
    success: { type: Boolean, default: true, index: true },
    ip: { type: String, default: null },
    userAgent: { type: String, default: null },
  },
  { timestamps: true },
);

LoginHistorySchema.index({ userId: 1, createdAt: -1 });

export const LoginHistoryModel = model("LoginHistory", LoginHistorySchema);
