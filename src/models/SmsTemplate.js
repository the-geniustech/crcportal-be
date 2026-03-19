import { Schema, model } from "./_shared.js";

export const SmsTemplateSchema = new Schema(
  {
    key: { type: String, required: true, trim: true, lowercase: true, unique: true, index: true },
    name: { type: String, required: true, trim: true },
    body: { type: String, required: true, trim: true },
    isActive: { type: Boolean, default: true, index: true },
  },
  { timestamps: true },
);

export const SmsTemplateModel = model("SmsTemplate", SmsTemplateSchema);

