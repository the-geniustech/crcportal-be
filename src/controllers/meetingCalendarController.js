import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import pickColorFromId from "../utils/pickColorFromId.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { MeetingModel } from "../models/Meeting.js";
import { MeetingAgendaItemModel } from "../models/MeetingAgendaItem.js";
import { MeetingRsvpModel, MeetingRsvpStatuses } from "../models/MeetingRsvp.js";
import { hasUserRole } from "../utils/roles.js";

function parseBool(val) {
  if (typeof val === "boolean") return val;
  if (typeof val === "number") return val !== 0;
  const str = String(val ?? "").trim().toLowerCase();
  if (!str) return false;
  return ["1", "true", "yes", "y", "on"].includes(str);
}

function parseDateOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

export const listMyCalendarMeetings = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const from = parseDateOrNull(req.query?.from) || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const to = parseDateOrNull(req.query?.to) || new Date(Date.now() + 60 * 24 * 60 * 60 * 1000);

  if (from.getTime() > to.getTime()) {
    return next(new AppError("from must be <= to", 400));
  }

  const rangeMs = to.getTime() - from.getTime();
  const maxRangeMs = 366 * 24 * 60 * 60 * 1000;
  if (rangeMs > maxRangeMs) {
    return next(new AppError("Date range too large (max 366 days)", 400));
  }

  const includeAgenda = parseBool(req.query?.includeAgenda ?? true);
  const groupIdFilter = req.query?.groupId ? String(req.query.groupId) : null;

  let groupIds = [];

  if (hasUserRole(req.user, "admin")) {
    if (groupIdFilter) {
      groupIds = [groupIdFilter];
    } else {
      const allGroups = await GroupModel.find({}, { _id: 1 }).lean();
      groupIds = allGroups.map((g) => String(g._id));
    }
  } else {
    const memberships = await GroupMembershipModel.find(
      { userId: req.user.profileId, status: "active" },
      { groupId: 1 },
    ).lean();

    groupIds = memberships.map((m) => String(m.groupId));
    if (groupIdFilter) {
      if (!groupIds.includes(groupIdFilter)) {
        return next(new AppError("Not a member of this group", 403));
      }
      groupIds = [groupIdFilter];
    }
  }

  if (groupIds.length === 0) {
    return sendSuccess(res, { statusCode: 200, results: 0, data: { meetings: [] } });
  }

  const meetings = await MeetingModel.find(
    {
      groupId: { $in: groupIds },
      scheduledDate: { $gte: from, $lte: to },
    },
    {
      groupId: 1,
      title: 1,
      description: 1,
      meetingType: 1,
      location: 1,
      meetingLink: 1,
      scheduledDate: 1,
      durationMinutes: 1,
      status: 1,
    },
  )
    .sort({ scheduledDate: 1, createdAt: 1 })
    .lean();

  if (meetings.length === 0) {
    return sendSuccess(res, { statusCode: 200, results: 0, data: { meetings: [] } });
  }

  const meetingIds = meetings.map((m) => m._id);
  const uniqueGroupIds = [...new Set(meetings.map((m) => String(m.groupId)))];

  const [groups, myRsvps, attendeeCounts, memberCounts, agendaItems] =
    await Promise.all([
      GroupModel.find({ _id: { $in: uniqueGroupIds } }, { name: 1 }).lean(),
      MeetingRsvpModel.find(
        { meetingId: { $in: meetingIds }, userId: req.user.profileId },
        { meetingId: 1, status: 1 },
      ).lean(),
      MeetingRsvpModel.aggregate([
        { $match: { meetingId: { $in: meetingIds }, status: "attending" } },
        { $group: { _id: "$meetingId", count: { $sum: 1 } } },
      ]),
      GroupMembershipModel.aggregate([
        { $match: { groupId: { $in: uniqueGroupIds }, status: "active" } },
        { $group: { _id: "$groupId", count: { $sum: 1 } } },
      ]),
      includeAgenda
        ? MeetingAgendaItemModel.find(
            { meetingId: { $in: meetingIds } },
            { meetingId: 1, title: 1, orderIndex: 1 },
          )
            .sort({ meetingId: 1, orderIndex: 1, createdAt: 1 })
            .lean()
        : Promise.resolve([]),
    ]);

  const groupNameById = new Map(groups.map((g) => [String(g._id), g.name]));
  const rsvpByMeetingId = new Map(myRsvps.map((r) => [String(r.meetingId), r.status]));
  const attendeeCountByMeetingId = new Map(attendeeCounts.map((r) => [String(r._id), r.count]));
  const memberCountByGroupId = new Map(memberCounts.map((r) => [String(r._id), r.count]));

  const agendaByMeetingId = new Map();
  if (includeAgenda) {
    for (const item of agendaItems) {
      const key = String(item.meetingId);
      if (!agendaByMeetingId.has(key)) agendaByMeetingId.set(key, []);
      agendaByMeetingId.get(key).push(item.title);
    }
  }

  const payloadMeetings = meetings.map((m) => {
    const groupId = String(m.groupId);
    const meetingId = String(m._id);

    return {
      id: meetingId,
      title: m.title,
      description: m.description || "",
      groupId,
      groupName: groupNameById.get(groupId) || "Unknown Group",
      groupColor: pickColorFromId(groupId),
      meetingType: m.meetingType,
      location: m.location || undefined,
      meetingLink: m.meetingLink || undefined,
      scheduledDate: m.scheduledDate,
      durationMinutes: m.durationMinutes,
      rsvpStatus: rsvpByMeetingId.get(meetingId) || "pending",
      attendeesCount: attendeeCountByMeetingId.get(meetingId) || 0,
      totalMembers: memberCountByGroupId.get(groupId) || 0,
      agenda: includeAgenda ? agendaByMeetingId.get(meetingId) || [] : undefined,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: payloadMeetings.length,
    data: { meetings: payloadMeetings },
  });
});

export const upsertMyMeetingRsvp = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const { meetingId } = req.params;
  if (!meetingId) return next(new AppError("Missing meeting id", 400));

  const { status } = req.body || {};
  if (!MeetingRsvpStatuses.includes(status)) {
    return next(
      new AppError(`Invalid status. Allowed: ${MeetingRsvpStatuses.join(", ")}`, 400),
    );
  }

  const meeting = await MeetingModel.findById(meetingId, { groupId: 1 }).lean();
  if (!meeting) return next(new AppError("Meeting not found", 404));

  if (!hasUserRole(req.user, "admin")) {
    const membership = await GroupMembershipModel.findOne(
      { userId: req.user.profileId, groupId: meeting.groupId, status: "active" },
      { _id: 1 },
    ).lean();
    if (!membership) return next(new AppError("Not a member of this group", 403));
  }

  const rsvp = await MeetingRsvpModel.findOneAndUpdate(
    { meetingId, userId: req.user.profileId },
    { meetingId, userId: req.user.profileId, status },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );

  const attendeesCount = await MeetingRsvpModel.countDocuments({ meetingId, status: "attending" });

  return sendSuccess(res, {
    statusCode: 200,
    message: "RSVP updated",
    data: {
      rsvp,
      attendeesCount,
    },
  });
});
