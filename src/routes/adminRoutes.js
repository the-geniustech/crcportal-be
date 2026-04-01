import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  approveMemberApplication,
  listAdminGroups,
  listContributionTracker,
  listMemberApprovals,
  markContributionPaid,
  rejectMemberApplication,
  sendContributionReminders,
} from "../controllers/adminController.js";
import { getAdminFinancialReports } from "../controllers/adminFinancialReportsController.js";
import {
  createAdminAttendanceMeeting,
  getAdminMeetingAttendanceRoster,
  listAdminAttendanceMeetings,
  upsertAdminMeetingAttendance,
} from "../controllers/adminAttendanceController.js";
import adminLoanRoutes from "./adminLoanRoutes.js";
import { createAdminAnnouncement } from "../controllers/adminAnnouncementController.js";
import { getAdminSmsStats, listAdminSmsTemplates, sendAdminBulkSms } from "../controllers/adminSmsController.js";
import {
  getAdminContributionTracking,
  getAdminSpecialContributionSummary,
} from "../controllers/adminContributionOverviewController.js";

const router = express.Router();

router.use(protect);
router.use(restrictTo("admin", "groupCoordinator"));

router.get("/member-approvals", restrictTo("groupCoordinator"), listMemberApprovals);
router.patch(
  "/member-approvals/:membershipId/approve",
  restrictTo("groupCoordinator"),
  approveMemberApplication,
);
router.patch(
  "/member-approvals/:membershipId/reject",
  restrictTo("groupCoordinator"),
  rejectMemberApplication,
);

router.get("/groups", listAdminGroups);

router.use("/loans", adminLoanRoutes);

router.get(
  "/contributions/tracker",
  restrictTo("groupCoordinator"),
  listContributionTracker,
);
router.post(
  "/contributions/remind",
  restrictTo("groupCoordinator"),
  sendContributionReminders,
);
router.post(
  "/contributions/mark-paid",
  restrictTo("groupCoordinator"),
  markContributionPaid,
);
router.get(
  "/contributions/tracking",
  restrictTo("groupCoordinator"),
  getAdminContributionTracking,
);
router.get(
  "/contributions/special-summary",
  restrictTo("groupCoordinator"),
  getAdminSpecialContributionSummary,
);

router.get("/financial-reports", getAdminFinancialReports);

router.get("/attendance/meetings", listAdminAttendanceMeetings);
router.post("/attendance/meetings", createAdminAttendanceMeeting);
router.get("/attendance/meetings/:meetingId/attendance", getAdminMeetingAttendanceRoster);
router.put("/attendance/meetings/:meetingId/attendance", upsertAdminMeetingAttendance);

router.post("/announcements", createAdminAnnouncement);

router.get("/sms/stats", getAdminSmsStats);
router.get("/sms/templates", listAdminSmsTemplates);
router.post("/sms/send", sendAdminBulkSms);

export default router;
