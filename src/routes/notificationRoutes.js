import express from "express";

import { protect } from "../controllers/authController.js";
import {
  listMyNotifications,
  listMyUnreadNotifications,
  markAllMyNotificationsRead,
  markMyNotificationRead,
} from "../controllers/notificationController.js";

const router = express.Router();

router.use(protect);

router.get("/", listMyNotifications);
router.get("/unread", listMyUnreadNotifications);
router.patch("/read-all", markAllMyNotificationsRead);
router.patch("/:id/read", markMyNotificationRead);

export default router;
