import { Schema, ObjectId, model } from "./_shared.js";
import { ContributionTypeCanonical } from "../utils/contributionPolicy.js";

export const WithdrawalStatuses = [
  "pending",
  "approved",
  "processing",
  "completed",
  "rejected",
];

const ManualWithdrawalPayoutSchema = new Schema(
  {
    status: {
      type: String,
      enum: ["pending_otp", "completed"],
      default: null,
    },
    method: {
      type: String,
      enum: [
        "cash",
        "bank_transfer",
        "bank_settlement",
        "cheque",
        "pos",
        "other",
      ],
      default: null,
    },
    amount: { type: Number, default: null, min: 0 },
    externalReference: { type: String, default: null, trim: true },
    occurredAt: { type: Date, default: null },
    notes: { type: String, default: null, trim: true },
    previousStatus: { type: String, default: null, trim: true },
    initiatedByUserId: { type: ObjectId, ref: "User", default: null },
    initiatedBy: { type: ObjectId, ref: "Profile", default: null },
    authorizedBy: { type: ObjectId, ref: "Profile", default: null },
    initiatedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    otpChannel: {
      type: String,
      enum: ["phone", "email"],
      default: null,
    },
    otpRecipient: { type: String, default: null, trim: true },
    otpSentAt: { type: Date, default: null },
  },
  { _id: false },
);

const WithdrawalPayoutEventSchema = new Schema(
  {
    eventType: { type: String, required: true, trim: true },
    gateway: { type: String, default: null, trim: true },
    status: { type: String, default: null, trim: true },
    reference: { type: String, default: null, trim: true },
    transferCode: { type: String, default: null, trim: true },
    message: { type: String, default: null, trim: true },
    actorUserId: { type: ObjectId, ref: "User", default: null },
    actorProfileId: { type: ObjectId, ref: "Profile", default: null },
    occurredAt: { type: Date, default: Date.now },
    metadata: { type: Schema.Types.Mixed, default: null },
  },
  { _id: false },
);

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
    payoutEvents: { type: [WithdrawalPayoutEventSchema], default: [] },
    manualPayout: { type: ManualWithdrawalPayoutSchema, default: null },
    manualPayoutOtpHash: {
      type: String,
      default: null,
      select: false,
    },
    manualPayoutOtpExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
  },
  { timestamps: true },
);

WithdrawalRequestSchema.index({ status: 1, createdAt: -1 });
WithdrawalRequestSchema.index({ userId: 1, createdAt: -1 });

export const WithdrawalRequestModel = model(
  "WithdrawalRequest",
  WithdrawalRequestSchema,
);
