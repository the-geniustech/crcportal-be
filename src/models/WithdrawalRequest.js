import { Schema, ObjectId, model } from "./_shared.js";
import { ContributionTypeCanonical } from "../utils/contributionPolicy.js";

export const WithdrawalStatuses = [
  "pending",
  "approved",
  "processing",
  "completed",
  "rejected",
];

export const WithdrawalRequestSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },

    bankAccountId: {
      type: ObjectId,
      ref: "BankAccount",
      default: null,
      index: true,
    },

    groupId: { type: ObjectId, ref: "Group", default: null, index: true },
    groupName: { type: String, default: null, trim: true },

    contributionType: {
      type: String,
      enum: ContributionTypeCanonical,
      default: null,
      index: true,
    },

    amount: { type: Number, required: true, min: 0 },

    // Denormalized snapshot of account at request time (matches UI payloads)
    bankName: { type: String, required: true, trim: true },
    bankCode: { type: String, default: null, trim: true },
    accountNumber: { type: String, required: true, trim: true },
    accountName: { type: String, required: true, trim: true },

    reason: { type: String, default: null, trim: true },

    status: {
      type: String,
      enum: WithdrawalStatuses,
      default: "pending",
      index: true,
    },

    adminNotes: { type: String, default: null, trim: true },
    rejectionReason: { type: String, default: null, trim: true },

    approvedAt: { type: Date, default: null, index: true },
    completedAt: { type: Date, default: null, index: true },

    payoutReference: { type: String, default: null, trim: true },
    payoutGateway: { type: String, default: null, trim: true },
    payoutTransferCode: { type: String, default: null, trim: true },
    payoutStatus: { type: String, default: null, trim: true },
    payoutOtpResentAt: { type: Date, default: null },
  },
  { timestamps: true },
);

WithdrawalRequestSchema.index({ status: 1, createdAt: -1 });
WithdrawalRequestSchema.index({ userId: 1, createdAt: -1 });

export const WithdrawalRequestModel = model(
  "WithdrawalRequest",
  WithdrawalRequestSchema,
);
