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

export const getMyNotificationPreferences = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  let prefs = await NotificationPreferenceModel.findOne({ userId: req.user.profileId }).lean();
  if (!prefs) {
    prefs = await NotificationPreferenceModel.create({
      userId: req.user.profileId,
      ...DEFAULT_PREFS,
    }).then((doc) => doc.toObject());
  }

  return sendSuccess(res, { statusCode: 200, data: { preferences: prefs } });
});

export const updateMyNotificationPreferences = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const updates = pickPrefs(req.body || {});
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
