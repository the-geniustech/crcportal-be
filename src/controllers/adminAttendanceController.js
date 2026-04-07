import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { MeetingModel, MeetingStatuses, MeetingTypes } from "../models/Meeting.js";
import { MeetingAttendanceModel, AttendanceStatuses } from "../models/MeetingAttendance.js";
import { hasUserRole } from "../utils/roles.js";

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (hasUserRole(req.user, "admin")) return null;

  if (!hasUserRole(req.user, "groupCoordinator")) {
    throw new AppError("Insufficient permissions", 403);
  }

  const coordinatorMemberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return coordinatorMemberships.map((m) => String(m.groupId));
}

function monthStartUtc(year, month1to12) {
  return new Date(Date.UTC(year, month1to12 - 1, 1, 0, 0, 0, 0));
}

function monthEndUtc(year, month1to12) {
  return new Date(Date.UTC(year, month1to12, 0, 23, 59, 59, 999));
}

function shiftMonthUtc({ year, month }, deltaMonths) {
  const d = new Date(Date.UTC(year, month - 1 + deltaMonths, 1, 0, 0, 0, 0));
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1 };
}

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

export const listAdminAttendanceMeetings = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);
  const groupScopeIds = manageableGroupIds ?? [];
  const groupObjectIds =
    manageableGroupIds === null
      ? null
      : groupScopeIds
          .filter((id) => mongoose.Types.ObjectId.isValid(id))
          .map((id) => new mongoose.Types.ObjectId(id));

  const groupIdParam = req.query?.groupId ? String(req.query.groupId) : null;
  if (groupIdParam && !mongoose.Types.ObjectId.isValid(groupIdParam)) {
    return next(new AppError("Invalid groupId", 400));
  }
  if (groupIdParam && groupObjectIds) {
    const ok = groupScopeIds.includes(groupIdParam);
    if (!ok) return next(new AppError("You cannot manage this group", 403));
  }

  const q = typeof req.query?.q === "string" ? req.query.q.trim() : "";
  const statusParam = typeof req.query?.status === "string" ? req.query.status.trim() : "";
  if (statusParam && !MeetingStatuses.includes(statusParam)) {
    return next(new AppError(`Invalid status. Allowed: ${MeetingStatuses.join(", ")}`, 400));
  }

  const limit = clamp(parseInt(String(req.query?.limit ?? "200"), 10) || 200, 1, 500);

  const now = new Date();
  const nowYm = { year: now.getUTCFullYear(), month: now.getUTCMonth() + 1 };
  const fromYm = shiftMonthUtc(nowYm, -6);
  const toYm = shiftMonthUtc(nowYm, 6);
  const defaultFrom = monthStartUtc(fromYm.year, fromYm.month);
  const defaultTo = monthEndUtc(toYm.year, toYm.month);

  const from = req.query?.from ? new Date(String(req.query.from)) : defaultFrom;
  const to = req.query?.to ? new Date(String(req.query.to)) : defaultTo;
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
    return next(new AppError("Invalid from/to date", 400));
  }

  const filter = {
    scheduledDate: { $gte: from, $lte: to },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
    ...(groupIdParam ? { groupId: new mongoose.Types.ObjectId(groupIdParam) } : {}),
    ...(statusParam ? { status: statusParam } : {}),
  };

  if (q) {
    filter.$or = [{ title: { $regex: q, $options: "i" } }];
  }

  const meetings = await MeetingModel.find(filter)
    .sort({ scheduledDate: -1, createdAt: -1 })
    .limit(limit)
    .lean();

  const meetingIds = meetings.map((m) => m._id);
  const groupIds = [...new Set(meetings.map((m) => String(m.groupId)))];
  const groupIdsObj = groupIds
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  const [groups, activeMembersAgg, attendanceAgg] = await Promise.all([
    GroupModel.find({ _id: { $in: groupIdsObj } }, { groupName: 1 }).lean(),
    GroupMembershipModel.aggregate([
      { $match: { groupId: { $in: groupIdsObj }, status: "active" } },
      { $group: { _id: "$groupId", count: { $sum: 1 } } },
    ]),
    MeetingAttendanceModel.aggregate([
      { $match: { meetingId: { $in: meetingIds } } },
      { $group: { _id: { meetingId: "$meetingId", status: "$status" }, count: { $sum: 1 } } },
    ]),
  ]);

  const groupNameById = new Map(groups.map((g) => [String(g._id), g.groupName]));
  const activeMembersByGroupId = new Map(
    activeMembersAgg.map((r) => [String(r._id), Number(r.count || 0)]),
  );

  const statusCountsByMeetingId = new Map();
  for (const row of attendanceAgg) {
    const meetingId = String(row._id.meetingId);
    const status = String(row._id.status);
    const count = Number(row.count || 0);
    const cur = statusCountsByMeetingId.get(meetingId) ?? {};
    cur[status] = count;
    statusCountsByMeetingId.set(meetingId, cur);
  }

  const rows = meetings.map((m) => {
    const meetingId = String(m._id);
    const groupId = String(m.groupId);
    const groupName = groupNameById.get(groupId) ?? "Group";
    const totalMembers = activeMembersByGroupId.get(groupId) ?? 0;
    const counts = statusCountsByMeetingId.get(meetingId) ?? {};

    const present = Number(counts.present || 0);
    const late = Number(counts.late || 0);
    const excused = Number(counts.excused || 0);
    const absent = Math.max(0, totalMembers - present - late - excused);

    return {
      id: meetingId,
      title: m.title,
      groupId,
      groupName,
      scheduledDate: m.scheduledDate,
      durationMinutes: m.durationMinutes,
      status: m.status,
      meetingType: m.meetingType,
      location: m.location,
      meetingLink: m.meetingLink,
      totalMembers,
      present,
      absent,
      excused,
      late,
    };
  });

  return sendSuccess(res, { statusCode: 200, results: rows.length, data: { meetings: rows } });
});

export const createAdminAttendanceMeeting = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);

  const {
    groupId,
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

  const gid = String(groupId || "").trim();
  if (!gid || !mongoose.Types.ObjectId.isValid(gid)) return next(new AppError("Valid groupId is required", 400));
  if (manageableGroupIds && !manageableGroupIds.includes(gid)) {
    return next(new AppError("You cannot manage this group", 403));
  }

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

  const group = await GroupModel.findById(gid, { groupName: 1 }).lean();
  if (!group) return next(new AppError("Group not found", 404));

  const meeting = await MeetingModel.create({
    groupId: new mongoose.Types.ObjectId(gid),
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
    data: {
      meeting: {
        id: String(meeting._id),
        title: meeting.title,
        groupId: gid,
        groupName: group.groupName,
        scheduledDate: meeting.scheduledDate,
        durationMinutes: meeting.durationMinutes,
        status: meeting.status,
        meetingType: meeting.meetingType,
        location: meeting.location,
        meetingLink: meeting.meetingLink,
      },
    },
  });
});

export const getAdminMeetingAttendanceRoster = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);
  const { meetingId } = req.params;
  if (!mongoose.Types.ObjectId.isValid(meetingId)) return next(new AppError("Invalid meetingId", 400));

  const meeting = await MeetingModel.findById(meetingId).lean();
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const gid = String(meeting.groupId);
  if (manageableGroupIds && !manageableGroupIds.includes(gid)) {
    return next(new AppError("You cannot manage this group", 403));
  }

  const [group, memberships, attendance] = await Promise.all([
    GroupModel.findById(gid, { groupName: 1 }).lean(),
    GroupMembershipModel.find(
      { groupId: meeting.groupId, status: "active" },
      { userId: 1 },
    )
      .populate("userId", "fullName email phone")
      .sort({ createdAt: 1 })
      .lean(),
    MeetingAttendanceModel.find({ meetingId: meeting._id }).lean(),
  ]);

  const attendanceByUserId = new Map(attendance.map((a) => [String(a.userId), a]));

  const roster = memberships.map((m) => {
    const profile = m.userId && typeof m.userId === "object" ? m.userId : null;
    const userId = profile ? String(profile._id) : String(m.userId);
    const record = attendanceByUserId.get(userId);

    return {
      id: userId,
      memberId: userId,
      memberName: profile?.fullName ?? "Member",
      status: record?.status ?? "absent",
      checkInTime: record?.checkInTime ?? null,
      notes: record?.notes ?? null,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      meeting: {
        id: String(meeting._id),
        title: meeting.title,
        groupId: gid,
        groupName: group?.groupName ?? "Group",
        scheduledDate: meeting.scheduledDate,
        durationMinutes: meeting.durationMinutes,
        status: meeting.status,
        meetingType: meeting.meetingType,
        location: meeting.location,
        meetingLink: meeting.meetingLink,
      },
      roster,
    },
  });
});

export const upsertAdminMeetingAttendance = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);
  const { meetingId } = req.params;
  if (!mongoose.Types.ObjectId.isValid(meetingId)) return next(new AppError("Invalid meetingId", 400));

  const meeting = await MeetingModel.findById(meetingId, { groupId: 1 }).lean();
  if (!meeting) return next(new AppError("Meeting not found", 404));

  const gid = String(meeting.groupId);
  if (manageableGroupIds && !manageableGroupIds.includes(gid)) {
    return next(new AppError("You cannot manage this group", 403));
  }

  const userId = String(req.body?.userId || "").trim();
  const status = String(req.body?.status || "").trim();
  const checkInTime = req.body?.checkInTime ?? null;
  const notes = req.body?.notes ?? null;

  if (!userId || !mongoose.Types.ObjectId.isValid(userId)) {
    return next(new AppError("Valid userId is required", 400));
  }
  if (!AttendanceStatuses.includes(status)) {
    return next(new AppError(`Invalid status. Allowed: ${AttendanceStatuses.join(", ")}`, 400));
  }

  const isActiveMember = await GroupMembershipModel.exists({
    groupId: meeting.groupId,
    userId: new mongoose.Types.ObjectId(userId),
    status: "active",
  });
  if (!isActiveMember) return next(new AppError("User is not an active group member", 400));

  const record = await MeetingAttendanceModel.findOneAndUpdate(
    { meetingId: meeting._id, userId: new mongoose.Types.ObjectId(userId) },
    {
      meetingId: meeting._id,
      userId: new mongoose.Types.ObjectId(userId),
      status,
      checkInTime,
      notes,
    },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );

  return sendSuccess(res, { statusCode: 200, data: { attendance: record } });
});
