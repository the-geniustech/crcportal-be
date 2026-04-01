import { Schema, ObjectId, model } from "./_shared.js";

export const ContributionSettingSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    groupId: { type: ObjectId, ref: "Group", required: true, index: true },

    year: { type: Number, required: true, min: 2000, index: true },
    contributionType: { type: String, default: "revolving", trim: true, index: true },

    expectedMonthlyAmount: { type: Number, required: true, min: 0 },
    totalExpected: { type: Number, required: true, min: 0 },
    totalActual: { type: Number, default: 0, min: 0 },
    outstandingBalance: { type: Number, default: 0 },

    units: { type: Number, default: null, min: 0 },
  },
  { timestamps: true },
);

ContributionSettingSchema.index(
  { userId: 1, groupId: 1, year: 1, contributionType: 1 },
  { unique: true },
);

export const ContributionSettingModel = model(
  "ContributionSetting",
  ContributionSettingSchema,
);
