import { Schema, ObjectId, model } from "./_shared.js";

const LoanEditChangeSchema = new Schema(
  {
    field: { type: String, required: true, trim: true },
    label: { type: String, required: true, trim: true },
    from: { type: Schema.Types.Mixed, default: null },
    to: { type: Schema.Types.Mixed, default: null },
  },
  { _id: false },
);

export const LoanApplicationEditRequestSchema = new Schema(
  {
    loanApplicationId: {
      type: ObjectId,
      ref: "LoanApplication",
      required: true,
      index: true,
    },
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    status: {
      type: String,
      enum: ["pending", "approved", "rejected", "cancelled"],
      default: "pending",
      index: true,
    },
    requestedAt: { type: Date, default: Date.now },
    reviewedAt: { type: Date, default: null },
    reviewedBy: { type: ObjectId, ref: "Profile", default: null },
    reviewNotes: { type: String, default: null, trim: true },
    changes: { type: [LoanEditChangeSchema], default: [] },
    payload: { type: Schema.Types.Mixed, default: {} },
  },
  { timestamps: true },
);

LoanApplicationEditRequestSchema.index({ loanApplicationId: 1, createdAt: -1 });
LoanApplicationEditRequestSchema.index({ userId: 1, createdAt: -1 });

export const LoanApplicationEditRequestModel = model(
  "LoanApplicationEditRequest",
  LoanApplicationEditRequestSchema,
);
