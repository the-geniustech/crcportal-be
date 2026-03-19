import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  approveMemberApplication,
  listAdminGroups,
  listContributionTracker,
  listMemberApprovals,
  markContributionPaid,
  rejectMemberApplication,
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

router.get("/member-approvals", listMemberApprovals);
router.patch("/member-approvals/:membershipId/approve", approveMemberApplication);
router.patch("/member-approvals/:membershipId/reject", rejectMemberApplication);

router.get("/groups", listAdminGroups);

router.use("/loans", adminLoanRoutes);

router.get("/contributions/tracker", listContributionTracker);
router.post("/contributions/mark-paid", markContributionPaid);
router.get("/contributions/tracking", getAdminContributionTracking);
router.get("/contributions/special-summary", getAdminSpecialContributionSummary);

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
