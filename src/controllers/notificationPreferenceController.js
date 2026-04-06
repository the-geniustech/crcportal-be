import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { NotificationPreferenceModel } from "../models/NotificationPreference.js";

const DEFAULT_PREFS = {
  emailNotifications: true,
  smsNotifications: true,
  pushNotifications: true,
  paymentReminders: true,
  groupUpdates: true,
  loanUpdates: true,
  meetingReminders: true,
  marketingEmails: false,
};

const DEFAULT_LOAN_PDF_PREFS = {
  loanPdfSendApplicant: true,
  loanPdfSendGuarantors: true,
  loanPdfExtraEmails: [],
};

function pickPrefs(body = {}) {
  const keys = Object.keys(DEFAULT_PREFS);
  const prefs = {};
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(body, key)) {
      prefs[key] = Boolean(body[key]);
    }
  }
  return prefs;
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || "").trim());
}

function parseEmailList(input) {
  if (!input) return [];
  if (Array.isArray(input)) {
    return input.flatMap((item) => parseEmailList(item));
  }
  if (typeof input === "string") {
    return input
      .split(/[,\n;]/g)
      .map((value) => value.trim())
      .filter(Boolean);
  }
  return [];
}

function pickLoanPdfPrefs(body = {}) {
  const prefs = {};

  if (Object.prototype.hasOwnProperty.call(body, "loanPdfSendApplicant")) {
    prefs.loanPdfSendApplicant = Boolean(body.loanPdfSendApplicant);
  }

  if (Object.prototype.hasOwnProperty.call(body, "loanPdfSendGuarantors")) {
    prefs.loanPdfSendGuarantors = Boolean(body.loanPdfSendGuarantors);
  }

  if (Object.prototype.hasOwnProperty.call(body, "loanPdfExtraEmails")) {
    const entries = parseEmailList(body.loanPdfExtraEmails);
    const normalized = Array.from(
      new Set(
        entries
          .map((value) => normalizeEmail(value))
          .filter((value) => value && isValidEmail(value)),
      ),
    );
    prefs.loanPdfExtraEmails = normalized.slice(0, 10);
  }

  return prefs;
}

export const getMyNotificationPreferences = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  let prefs = await NotificationPreferenceModel.findOne({ userId: req.user.profileId }).lean();
  if (!prefs) {
    prefs = await NotificationPreferenceModel.create({
      userId: req.user.profileId,
      ...DEFAULT_PREFS,
      ...DEFAULT_LOAN_PDF_PREFS,
    }).then((doc) => doc.toObject());
  }

  return sendSuccess(res, { statusCode: 200, data: { preferences: prefs } });
});

export const updateMyNotificationPreferences = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const updates = {
    ...pickPrefs(req.body || {}),
    ...pickLoanPdfPrefs(req.body || {}),
  };
  if (Object.keys(updates).length === 0) {
    return next(new AppError("No preference updates provided", 400));
  }

  const prefs = await NotificationPreferenceModel.findOneAndUpdate(
    { userId: req.user.profileId },
    { $set: updates, $setOnInsert: { userId: req.user.profileId } },
    { new: true, upsert: true, runValidators: true, setDefaultsOnInsert: true },
  ).lean();

  return sendSuccess(res, { statusCode: 200, data: { preferences: prefs } });
});
