import { Schema, ObjectId, model } from "./_shared.js";
import { ContributionTypes } from "../utils/contributionPolicy.js";

export const ContributionStatuses = [
  "pending",
  "completed",
  "verified",
  "overdue",
];

export const ContributionSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    groupId: { type: ObjectId, ref: "Group", required: true, index: true },

    month: { type: Number, required: true, min: 1, max: 12, index: true },
    year: { type: Number, required: true, min: 2000, index: true },

    amount: { type: Number, required: true, min: 0 },
    contributionType: {
      type: String,
      enum: ContributionTypes,
      required: true,
      index: true,
    },
    status: {
      type: String,
      enum: ContributionStatuses,
      default: "pending",
      index: true,
    },

    paymentReference: { type: String, default: null, trim: true },
    paymentMethod: { type: String, default: null, trim: true },

    verifiedBy: { type: ObjectId, ref: "Profile", default: null },
    verifiedAt: { type: Date, default: null },

    notes: { type: String, default: null, trim: true },
  },
  { timestamps: true },
);

ContributionSchema.index(
  {
    userId: 1,
    groupId: 1,
    month: 1,
    year: 1,
    contributionType: 1,
  },
  { unique: true },
);
ContributionSchema.index({ groupId: 1, year: 1, month: 1 });

export const ContributionModel = model("Contribution", ContributionSchema);
