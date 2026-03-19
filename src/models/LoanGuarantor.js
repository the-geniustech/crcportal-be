import { Schema, ObjectId, model } from "./_shared.js";

export const LoanGuarantorStatuses = ["pending", "accepted", "rejected"];

export const LoanGuarantorSchema = new Schema(
  {
    loanApplicationId: {
      type: ObjectId,
      ref: "LoanApplication",
      required: true,
      index: true,
    },
    guarantorUserId: {
      type: ObjectId,
      ref: "Profile",
      required: true,
      index: true,
    },

    guarantorName: { type: String, required: true, trim: true },
    guarantorEmail: {
      type: String,
      default: null,
      trim: true,
      lowercase: true,
    },
    guarantorPhone: { type: String, default: null, trim: true },

    liabilityPercentage: { type: Number, required: true, min: 1, max: 100 },
    requestMessage: { type: String, default: null, trim: true },

    status: {
      type: String,
      enum: LoanGuarantorStatuses,
      default: "pending",
      index: true,
    },

    responseComment: { type: String, default: null, trim: true },
    respondedAt: { type: Date, default: null },
  },
  { timestamps: true },
);

LoanGuarantorSchema.index(
  { loanApplicationId: 1, guarantorUserId: 1 },
  { unique: true },
);
LoanGuarantorSchema.index({ guarantorUserId: 1, status: 1 });

export const LoanGuarantorModel = model("LoanGuarantor", LoanGuarantorSchema);
