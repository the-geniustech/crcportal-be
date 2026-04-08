import { Schema, ObjectId, model } from "./_shared.js";

export const ContributionInterestSettingSchema = new Schema(
  {
    year: { type: Number, required: true, min: 2000, max: 2100, unique: true, index: true },
    monthlyRates: { type: Map, of: Number, default: {} },
    updatedBy: { type: ObjectId, ref: "Profile", default: null },
  },
  { timestamps: true },
);

export const ContributionInterestSettingModel = model(
  "ContributionInterestSetting",
  ContributionInterestSettingSchema,
);
