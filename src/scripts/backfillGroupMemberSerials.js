import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { formatGroupMemberSerial, ensureGroupMemberSequence } from "../utils/groupMemberSerial.js";

const parseArgs = (args) => {
  const output = {};
  for (let index = 0; index < args.length; index += 1) {
    const current = args[index];
    if (!current.startsWith("--")) continue;
    const key = current.replace(/^--/, "");
    const next = args[index + 1];
    if (!next || next.startsWith("--")) {
      output[key] = true;
    } else {
      output[key] = next;
      index += 1;
    }
  }
  return output;
};

const args = parseArgs(process.argv.slice(2));
const isDryRun = Boolean(args["dry-run"]);
const includePending = Boolean(args["include-pending"]);
const statusFilter = includePending
  ? { status: { $ne: "rejected" } }
  : { status: { $in: ["active", "inactive", "suspended"] } };

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const stats = {
  groups: 0,
  membershipsScanned: 0,
  membershipsUpdated: 0,
  dryRun: isDryRun,
};

await connectMongo({ mongoUri });

const groups = await GroupModel.find({}, { groupNumber: 1 }).lean();
const groupNumberById = new Map(
  groups.map((group) => [String(group._id), Number(group.groupNumber ?? 0)]),
);

const memberships = await GroupMembershipModel.find(
  { ...statusFilter },
  { memberNumber: 1, memberSerial: 1, joinedAt: 1, createdAt: 1, groupId: 1 },
)
  .sort({ joinedAt: 1, createdAt: 1, _id: 1 })
  .lean();

stats.groups = groups.length;
stats.membershipsScanned = memberships.length;

let nextNumber = 1;

for (const membership of memberships) {
  const joinedAt = membership.joinedAt || membership.createdAt || new Date();
  const memberNumber = nextNumber;
  nextNumber += 1;
  const groupNumber = groupNumberById.get(String(membership.groupId)) ?? 0;
  const serial = formatGroupMemberSerial({
    groupNumber,
    memberNumber,
  });

  const needsUpdate =
    membership.memberNumber !== memberNumber ||
    membership.memberSerial !== serial ||
    !membership.joinedAt;

  if (!needsUpdate) continue;
  stats.membershipsUpdated += 1;

  if (!isDryRun) {
    await GroupMembershipModel.updateOne(
      { _id: membership._id },
      {
        $set: {
          memberNumber,
          memberSerial: serial,
          joinedAt,
        },
      },
    );
  }
}

if (!isDryRun) {
  await ensureGroupMemberSequence();
}

// eslint-disable-next-line no-console
console.log(JSON.stringify({ ok: 1, stats }, null, 2));

await mongoose.disconnect();
