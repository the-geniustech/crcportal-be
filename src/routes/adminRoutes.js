import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  approveMemberApplication,
  exportContributionTracker,
  listAdminGroups,
  listContributionTracker,
  listContributionTrackerEntries,
  listMemberApprovals,
  markContributionPaid,
  rejectMemberApplication,
  sendContributionReminders,
  updateTrackedContribution,
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
import {
  getAdminSmsStats,
  listAdminSmsTemplates,
  sendAdminBulkSms,
} from "../controllers/adminSmsController.js";
import {
  getAdminContributionTracking,
  getAdminSpecialContributionSummary,
} from "../controllers/adminContributionOverviewController.js";
import {
  getContributionIncomeSummary,
  getContributionInterestSettings,
  getContributionInterestSharing,
  exportContributionIncomeSummary,
  exportContributionInterestSharing,
  updateContributionInterestSettings,
} from "../controllers/adminContributionInterestController.js";
import {
  createAdminMember,
  deleteAdminMember,
  exportAdminMembers,
  getAdminMemberDetails,
  listAdminMembers,
  updateAdminMember,
} from "../controllers/adminMemberController.js";
import {
  exportAdminAuditLogs,
  listAdminAuditLogs,
} from "../controllers/adminAuditLogController.js";

const router = express.Router();

router.use(protect);
router.use(restrictTo("admin", "groupCoordinator"));

router.get(
  "/member-approvals",
  restrictTo("admin", "groupCoordinator"),
  listMemberApprovals,
);
router.patch(
  "/member-approvals/:membershipId/approve",
  restrictTo("admin", "groupCoordinator"),
  approveMemberApplication,
);
router.patch(
  "/member-approvals/:membershipId/reject",
  restrictTo("admin", "groupCoordinator"),
  rejectMemberApplication,
);

router.get("/members", restrictTo("admin", "groupCoordinator"), listAdminMembers);
router.get(
  "/members/export",
  restrictTo("admin", "groupCoordinator"),
  exportAdminMembers,
);
router.post("/members", restrictTo("admin", "groupCoordinator"), createAdminMember);
router.get(
  "/members/:membershipId",
  restrictTo("admin", "groupCoordinator"),
  getAdminMemberDetails,
);
router.patch(
  "/members/:membershipId",
  restrictTo("admin", "groupCoordinator"),
  updateAdminMember,
);
router.delete(
  "/members/:membershipId",
  restrictTo("admin", "groupCoordinator"),
  deleteAdminMember,
);
router.get("/audit-logs/export", restrictTo("admin"), exportAdminAuditLogs);
router.get("/audit-logs", restrictTo("admin"), listAdminAuditLogs);

router.get("/groups", listAdminGroups);

router.use("/loans", adminLoanRoutes);

router.get(
  "/contributions/tracker",
  restrictTo("admin", "groupCoordinator"),
  listContributionTracker,
);
router.get(
  "/contributions/tracker/export",
  restrictTo("admin", "groupCoordinator"),
  exportContributionTracker,
);
router.get(
  "/contributions/tracker/entries",
  restrictTo("admin", "groupCoordinator"),
  listContributionTrackerEntries,
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
router.patch(
  "/contributions/:contributionId",
  restrictTo("admin", "groupCoordinator"),
  updateTrackedContribution,
);
router.get(
  "/contributions/tracking",
  restrictTo("admin", "groupCoordinator"),
  getAdminContributionTracking,
);
router.get(
  "/contributions/special-summary",
  restrictTo("admin", "groupCoordinator"),
  getAdminSpecialContributionSummary,
);

router.get("/financial-reports", getAdminFinancialReports);

router.get(
  "/contributions/interest-settings",
  restrictTo("admin"),
  getContributionInterestSettings,
);
router.put(
  "/contributions/interest-settings",
  restrictTo("admin"),
  updateContributionInterestSettings,
);
router.get(
  "/contributions/summary-income",
  restrictTo("admin"),
  getContributionIncomeSummary,
);
router.get(
  "/contributions/summary-income/export",
  restrictTo("admin"),
  exportContributionIncomeSummary,
);
router.get(
  "/contributions/interest-sharing",
  restrictTo("admin"),
  getContributionInterestSharing,
);
router.get(
  "/contributions/interest-sharing/export",
  restrictTo("admin"),
  exportContributionInterestSharing,
);

router.get("/attendance/meetings", listAdminAttendanceMeetings);
router.post("/attendance/meetings", createAdminAttendanceMeeting);
router.get(
  "/attendance/meetings/:meetingId/attendance",
  getAdminMeetingAttendanceRoster,
);
router.put(
  "/attendance/meetings/:meetingId/attendance",
  upsertAdminMeetingAttendance,
);

router.post("/announcements", createAdminAnnouncement);

router.get("/sms/stats", getAdminSmsStats);
router.get("/sms/templates", listAdminSmsTemplates);
router.post("/sms/send", sendAdminBulkSms);

export default router;
