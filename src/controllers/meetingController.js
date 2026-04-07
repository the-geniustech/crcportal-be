import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { MeetingModel, MeetingStatuses, MeetingTypes } from "../models/Meeting.js";
import { MeetingAgendaItemModel } from "../models/MeetingAgendaItem.js";
import { MeetingMinutesModel } from "../models/MeetingMinutes.js";
import { MeetingAttendanceModel, AttendanceStatuses } from "../models/MeetingAttendance.js";
import { hasUserRole } from "../utils/roles.js";

async function getMeetingInGroup({ meetingId, groupId }) {
  return MeetingModel.findOne({ _id: meetingId, groupId });
}

export const listGroupMeetings = catchAsync(async (req, res) => {
  const group = req.group;

  const filter = { groupId: group._id };
  if (req.query?.status) filter.status = String(req.query.status);

  const meetings = await MeetingModel.find(filter).sort({ scheduledDate: -1, createdAt: -1 });

  return sendSuccess(res, {
    statusCode: 200,
    results: meetings.length,
    data: { meetings },
  });
});

export const createMeeting = catchAsync(async (req, res, next) => {
  const group = req.group;

  const {
    title,
    description = "",
    meetingType,
    location = null,
    meetingLink = null,
    meetingId = null,
    meetingPassword = null,
    scheduledDate,
    durationMinutes,
    status = "scheduled",
  } = req.body || {};

  if (!title || !String(title).trim()) return next(new AppError("title is required", 400));
  if (!MeetingTypes.includes(meetingType)) {
    return next(new AppError(`Invalid meetingType. Allowed: ${MeetingTypes.join(", ")}`, 400));
  }
  if (!scheduledDate) return next(new AppError("scheduledDate is required", 400));
  if (!durationMinutes || typeof durationMinutes !== "number") {
    return next(new AppError("durationMinutes must be a number", 400));
  }
  if (status && !MeetingStatuses.includes(status)) {
    return next(new AppError(`Invalid status. Allowed: ${MeetingStatuses.join(", ")}`, 400));
  }

  const meeting = await MeetingModel.create({
    groupId: group._id,
    title: String(title).trim(),
    description,
    meetingType,
    location,
    meetingLink,
    meetingId,
    meetingPassword,
    scheduledDate: new Date(scheduledDate),
    durationMinutes,
    status,
  });

  return sendSuccess(res, {
    statusCode: 201,
    message: "Meeting scheduled",
    data: { meeting },
  });
});

export const getMeeting = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  return sendSuccess(res, { statusCode: 200, data: { meeting } });
});

export const updateMeeting = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const updates = {};
  const allowed = [
    "title",
    "description",
    "meetingType",
    "location",
    "meetingLink",
    "meetingId",
    "meetingPassword",
    "scheduledDate",
    "durationMinutes",
    "status",
  ];
  for (const key of allowed) {
    if (Object.prototype.hasOwnProperty.call(req.body || {}, key)) updates[key] = req.body[key];
  }

  if (typeof updates.meetingType !== "undefined" && !MeetingTypes.includes(updates.meetingType)) {
    return next(new AppError(`Invalid meetingType. Allowed: ${MeetingTypes.join(", ")}`, 400));
  }
  if (typeof updates.status !== "undefined" && !MeetingStatuses.includes(updates.status)) {
    return next(new AppError(`Invalid status. Allowed: ${MeetingStatuses.join(", ")}`, 400));
  }
  if (typeof updates.scheduledDate !== "undefined") {
    updates.scheduledDate = new Date(updates.scheduledDate);
  }

  if (Object.keys(updates).length === 0) {
    return next(new AppError("No updatable fields provided", 400));
  }

  const meeting = await MeetingModel.findOneAndUpdate(
    { _id: meetingId, groupId: group._id },
    updates,
    { new: true, runValidators: true },
  );

  if (!meeting) return next(new AppError("Meeting not found", 404));

  return sendSuccess(res, { statusCode: 200, data: { meeting } });
});

export const deleteMeeting = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await MeetingModel.findOneAndDelete({ _id: meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  await Promise.all([
    MeetingAgendaItemModel.deleteMany({ meetingId: meeting._id }),
    MeetingMinutesModel.deleteOne({ meetingId: meeting._id }),
    MeetingAttendanceModel.deleteMany({ meetingId: meeting._id }),
  ]);

  return sendSuccess(res, { statusCode: 200, message: "Meeting deleted" });
});

export const listAgendaItems = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const items = await MeetingAgendaItemModel.find({ meetingId: meeting._id }).sort({
    orderIndex: 1,
  });

  return sendSuccess(res, { statusCode: 200, results: items.length, data: { items } });
});

export const createAgendaItem = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const { title, description = "", durationMinutes, orderIndex } = req.body || {};

  if (!title || !String(title).trim()) return next(new AppError("title is required", 400));
  if (!durationMinutes || typeof durationMinutes !== "number") {
    return next(new AppError("durationMinutes must be a number", 400));
  }
  if (typeof orderIndex !== "number") return next(new AppError("orderIndex must be a number", 400));

  const item = await MeetingAgendaItemModel.create({
    meetingId: meeting._id,
    title: String(title).trim(),
    description,
    durationMinutes,
    orderIndex,
  });

  return sendSuccess(res, { statusCode: 201, data: { item } });
});

export const updateAgendaItem = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId, agendaItemId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const updates = {};
  for (const key of ["title", "description", "durationMinutes", "orderIndex"]) {
    if (Object.prototype.hasOwnProperty.call(req.body || {}, key)) updates[key] = req.body[key];
  }

  if (Object.keys(updates).length === 0) {
    return next(new AppError("No updatable fields provided", 400));
  }

  const item = await MeetingAgendaItemModel.findOneAndUpdate(
    { _id: agendaItemId, meetingId: meeting._id },
    updates,
    { new: true, runValidators: true },
  );

  if (!item) return next(new AppError("Agenda item not found", 404));

  return sendSuccess(res, { statusCode: 200, data: { item } });
});

export const deleteAgendaItem = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId, agendaItemId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const item = await MeetingAgendaItemModel.findOneAndDelete({
    _id: agendaItemId,
    meetingId: meeting._id,
  });
  if (!item) return next(new AppError("Agenda item not found", 404));

  return sendSuccess(res, { statusCode: 200, message: "Agenda item deleted" });
});

export const getMinutes = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const minutes = await MeetingMinutesModel.findOne({ meetingId: meeting._id });
  return sendSuccess(res, { statusCode: 200, data: { minutes } });
});

export const upsertMinutes = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const { content, attendeesCount = 0, decisionsMade = [], actionItems = [] } = req.body || {};
  if (!content || !String(content).trim()) return next(new AppError("content is required", 400));

  const minutes = await MeetingMinutesModel.findOneAndUpdate(
    { meetingId: meeting._id },
    { meetingId: meeting._id, content, attendeesCount, decisionsMade, actionItems },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );

  return sendSuccess(res, { statusCode: 200, data: { minutes } });
});

export const listAttendance = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const attendance = await MeetingAttendanceModel.find({ meetingId: meeting._id })
    .sort({ createdAt: -1 })
    .populate("userId");

  return sendSuccess(res, {
    statusCode: 200,
    results: attendance.length,
    data: { attendance },
  });
});

export const upsertAttendance = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const group = req.group;
  const { meetingId } = req.params;

  const meeting = await getMeetingInGroup({ meetingId, groupId: group._id });
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const { userId, status, checkInTime = null, notes = null } = req.body || {};
  const targetUserId = userId || req.user.profileId;

  if (
    userId &&
    String(userId) !== String(req.user.profileId) &&
    !hasUserRole(req.user, "admin") &&
    !["coordinator", "secretary", "admin"].includes(req.groupMembership?.role)
  ) {
    return next(new AppError("Insufficient group permissions to set attendance for others", 403));
  }

  if (!AttendanceStatuses.includes(status)) {
    return next(
      new AppError(`Invalid status. Allowed: ${AttendanceStatuses.join(", ")}`, 400),
    );
  }

  const record = await MeetingAttendanceModel.findOneAndUpdate(
    { meetingId: meeting._id, userId: targetUserId },
    { meetingId: meeting._id, userId: targetUserId, status, checkInTime, notes },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );

  return sendSuccess(res, { statusCode: 200, data: { attendance: record } });
});
