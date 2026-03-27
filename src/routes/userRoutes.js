import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  getMe,
  getMyContributionSettings,
  listMyGroups,
  listUsers,
  updateMe,
  updateMyContributionSettings,
  updateUserRole,
} from "../controllers/userController.js";
import {
  getMyNotificationPreferences,
  updateMyNotificationPreferences,
} from "../controllers/notificationPreferenceController.js";
import { generateMyFinancialReport } from "../controllers/financialReportController.js";
import { uploadSingle } from "../middlewares/upload.js";
import { cloudinaryUploadSingle } from "../middlewares/cloudinaryUpload.js";

const router = express.Router();

router.use(protect);

router.get("/me", getMe);
router.get("/me/groups", listMyGroups);
router.get("/me/notification-preferences", getMyNotificationPreferences);
router.put("/me/notification-preferences", updateMyNotificationPreferences);
router.get("/me/contribution-settings", getMyContributionSettings);
router.put("/me/contribution-settings", updateMyContributionSettings);
router.post("/me/reports", generateMyFinancialReport);
router.patch(
  "/me",
  uploadSingle("avatar"),
  cloudinaryUploadSingle({
    fileField: "avatar",
    bodyField: "avatar",
    folder: "avatars",
    resourceType: "image",
  }),
  updateMe,
);

router.get("/", restrictTo("admin"), listUsers);
router.patch("/:id/role", restrictTo("admin"), updateUserRole);

export default router;
