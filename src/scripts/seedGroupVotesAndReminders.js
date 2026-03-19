import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { GroupModel } from "../models/Group.js";
import { GroupVoteModel } from "../models/GroupVote.js";
import { GroupReminderSettingsModel } from "../models/GroupReminderSettings.js";

const args = new Set(process.argv.slice(2));
const shouldReset = args.has("--reset");
const isDryRun = args.has("--dry-run");
const limitArg = Array.from(args).find((arg) => arg.startsWith("--limit="));
const limitValue = limitArg ? Number.parseInt(limitArg.split("=")[1], 10) : null;
const limit = Number.isFinite(limitValue) && limitValue > 0 ? limitValue : null;

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

await connectMongo({ mongoUri });

const groupQuery = GroupModel.find(
  {},
  { _id: 1, groupName: 1, memberCount: 1, monthlyContribution: 1, groupNumber: 1 },
).sort({ groupNumber: 1, createdAt: 1 });

if (limit) {
  groupQuery.limit(limit);
}

const groups = await groupQuery.lean();

if (!groups.length) {
  // eslint-disable-next-line no-console
  console.log("No groups found. Run seed:groups first.");
  await mongoose.disconnect();
  process.exit(0);
}

const now = new Date();
const futureDate = new Date(now);
futureDate.setDate(futureDate.getDate() + 14);
const pastDate = new Date(now);
pastDate.setDate(pastDate.getDate() - 10);

const clampVotes = (total, yes, no) => {
  const safeYes = Math.min(total, Math.max(0, yes));
  const safeNo = Math.min(total - safeYes, Math.max(0, no));
  return { yesVotes: safeYes, noVotes: safeNo };
};

const voteDocs = [];
const reminderOps = [];

groups.forEach((group, index) => {
  const totalVoters = Math.max(5, Number(group.memberCount) || 12);
  const activeVotes = clampVotes(
    totalVoters,
    Math.round(totalVoters * 0.62),
    Math.round(totalVoters * 0.18),
  );
  const closedVotes = clampVotes(
    totalVoters,
    Math.round(totalVoters * 0.55),
    Math.round(totalVoters * 0.3),
  );

  voteDocs.push(
    {
      groupId: group._id,
      title: "Approve contribution schedule",
      description: `Confirm ${group.groupName || "the group"} contribution schedule for the next cycle.`,
      status: "active",
      endsAt: futureDate,
      totalVoters,
      ...activeVotes,
    },
    {
      groupId: group._id,
      title: "Adopt new meeting time",
      description: `Vote on the updated meeting time for ${group.groupName || "the group"}.`,
      status: "closed",
      endsAt: pastDate,
      totalVoters,
      ...closedVotes,
    },
  );

  reminderOps.push({
    updateOne: {
      filter: { groupId: group._id },
      update: {
        $set: {
          groupId: group._id,
          autoReminders: true,
          daysBeforeDue: index % 2 === 0 ? 3 : 5,
          overdueReminders: true,
          meetingReminders: index % 3 !== 0,
        },
      },
      upsert: true,
    },
  });
});

if (isDryRun) {
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        groups: groups.length,
        votes: voteDocs,
        reminderSettings: reminderOps.map((op) => op.updateOne.update.$set),
      },
      null,
      2,
    ),
  );
  await mongoose.disconnect();
  process.exit(0);
}

if (shouldReset) {
  const groupIds = groups.map((group) => group._id);
  await GroupVoteModel.deleteMany({ groupId: { $in: groupIds } });
  await GroupReminderSettingsModel.deleteMany({ groupId: { $in: groupIds } });
}

const voteOps = voteDocs.map((vote) => ({
  updateOne: {
    filter: { groupId: vote.groupId, title: vote.title },
    update: { $set: vote },
    upsert: true,
  },
}));

const [voteResult, reminderResult] = await Promise.all([
  GroupVoteModel.bulkWrite(voteOps, { ordered: false }),
  GroupReminderSettingsModel.bulkWrite(reminderOps, { ordered: false }),
]);

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      reset: shouldReset,
      groupsSeeded: groups.length,
      votesUpserted: voteResult.upsertedCount ?? 0,
      votesModified: voteResult.modifiedCount ?? 0,
      remindersUpserted: reminderResult.upsertedCount ?? 0,
      remindersModified: reminderResult.modifiedCount ?? 0,
    },
    null,
    2,
  ),
);

await mongoose.disconnect();
