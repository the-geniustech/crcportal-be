import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  addGroupMembers,
  archiveGroup,
  createGroup,
  getGroup,
  joinGroup,
  leaveGroup,
  listGroupMemberCandidates,
  listGroupMembers,
  listGroups,
  setCoordinator,
  updateGroup,
  updateGroupMember,
} from "../controllers/groupController.js";
import { listGroupLoans } from "../controllers/groupLoanController.js";
import {
  createGroupContribution,
  downloadGroupContributionReportPdf,
  listGroupContributions,
  updateContribution,
  verifyContribution,
} from "../controllers/contributionController.js";
import {
  createAgendaItem,
  createMeeting,
  deleteAgendaItem,
  deleteMeeting,
  getMeeting,
  getMinutes,
  listAgendaItems,
  listAttendance,
  listGroupMeetings,
  updateAgendaItem,
  updateMeeting,
  upsertAttendance,
  upsertMinutes,
} from "../controllers/meetingController.js";
import {
  createGroupVote,
  listGroupVotes,
} from "../controllers/groupVoteController.js";
import {
  getGroupReminderSettings,
  sendGroupContributionReminders,
  updateGroupReminderSettings,
} from "../controllers/groupReminderController.js";
import {
  loadGroup,
  loadMyGroupMembership,
  requireActiveMembership,
  requireGroupReadAccess,
  requireGroupRole,
} from "../middlewares/groupContext.js";

const router = express.Router();

router.use(protect);

router.get("/", listGroups);
router.post("/", restrictTo("admin", "groupCoordinator"), createGroup);

router.get("/:groupId", loadGroup, loadMyGroupMembership, getGroup);
router.patch(
  "/:groupId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  updateGroup,
);
router.delete("/:groupId", restrictTo("admin"), loadGroup, archiveGroup);

router.patch("/:groupId/coordinator", restrictTo("admin"), loadGroup, setCoordinator);

router.post("/:groupId/join", loadGroup, joinGroup);
router.post("/:groupId/leave", loadGroup, leaveGroup);

router.get(
  "/:groupId/members",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  listGroupMembers,
);
router.get(
  "/:groupId/members/candidates",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  listGroupMemberCandidates,
);
router.post(
  "/:groupId/members",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  addGroupMembers,
);
router.patch(
  "/:groupId/members/:memberId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  updateGroupMember,
);

router.get(
  "/:groupId/contributions",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  listGroupContributions,
);
router.get(
  "/:groupId/contributions/report",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  downloadGroupContributionReportPdf,
);
router.post(
  "/:groupId/contributions",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "treasurer", "admin"),
  createGroupContribution,
);
router.patch(
  "/:groupId/contributions/:contributionId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "treasurer", "admin"),
  updateContribution,
);
router.patch(
  "/:groupId/contributions/:contributionId/verify",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "treasurer", "admin"),
  verifyContribution,
);

router.get(
  "/:groupId/meetings",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  listGroupMeetings,
);

router.get(
  "/:groupId/votes",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  listGroupVotes,
);
router.post(
  "/:groupId/votes",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  createGroupVote,
);

router.get(
  "/:groupId/reminder-settings",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  getGroupReminderSettings,
);
router.patch(
  "/:groupId/reminder-settings",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  updateGroupReminderSettings,
);
router.post(
  "/:groupId/reminders/send",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "treasurer", "admin"),
  sendGroupContributionReminders,
);

router.get(
  "/:groupId/loans",
  loadGroup,
  loadMyGroupMembership,
  requireGroupReadAccess(),
  listGroupLoans,
);
router.post(
  "/:groupId/meetings",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "secretary", "admin"),
  createMeeting,
);
router.get(
  "/:groupId/meetings/:meetingId",
  loadGroup,
  loadMyGroupMembership,
  requireActiveMembership(),
  getMeeting,
);
router.patch(
  "/:groupId/meetings/:meetingId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "secretary", "admin"),
  updateMeeting,
);
router.delete(
  "/:groupId/meetings/:meetingId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "admin"),
  deleteMeeting,
);

router.get(
  "/:groupId/meetings/:meetingId/agenda",
  loadGroup,
  loadMyGroupMembership,
  requireActiveMembership(),
  listAgendaItems,
);
router.post(
  "/:groupId/meetings/:meetingId/agenda",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "secretary", "admin"),
  createAgendaItem,
);
router.patch(
  "/:groupId/meetings/:meetingId/agenda/:agendaItemId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "secretary", "admin"),
  updateAgendaItem,
);
router.delete(
  "/:groupId/meetings/:meetingId/agenda/:agendaItemId",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "secretary", "admin"),
  deleteAgendaItem,
);

router.get(
  "/:groupId/meetings/:meetingId/minutes",
  loadGroup,
  loadMyGroupMembership,
  requireActiveMembership(),
  getMinutes,
);
router.put(
  "/:groupId/meetings/:meetingId/minutes",
  loadGroup,
  loadMyGroupMembership,
  requireGroupRole("coordinator", "secretary", "admin"),
  upsertMinutes,
);

router.get(
  "/:groupId/meetings/:meetingId/attendance",
  loadGroup,
  loadMyGroupMembership,
  requireActiveMembership(),
  listAttendance,
);
router.put(
  "/:groupId/meetings/:meetingId/attendance",
  loadGroup,
  loadMyGroupMembership,
  requireActiveMembership(),
  upsertAttendance,
);

export default router;
