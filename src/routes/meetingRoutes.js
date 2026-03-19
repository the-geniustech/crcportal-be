import express from "express";

import { protect } from "../controllers/authController.js";
import {
  listMyCalendarMeetings,
  upsertMyMeetingRsvp,
} from "../controllers/meetingCalendarController.js";

const router = express.Router();

router.use(protect);

router.get("/me/calendar", listMyCalendarMeetings);
router.put("/:meetingId/rsvp", upsertMyMeetingRsvp);

export default router;

