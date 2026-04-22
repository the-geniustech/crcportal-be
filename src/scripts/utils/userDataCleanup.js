import mongoose from "mongoose";
import { connectMongo } from "../../db.js";
import { UserModel } from "../../models/User.js";
import { ProfileModel } from "../../models/Profile.js";

export function parseArgs(args) {
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
}

export function asObjectId(value, fieldName) {
  if (!value || !mongoose.Types.ObjectId.isValid(value)) {
    throw new Error(`Invalid ${fieldName}`);
  }
  return new mongoose.Types.ObjectId(String(value));
}

export function toIdStrings(values) {
  return [...new Set((Array.isArray(values) ? values : [])
    .map((value) => String(value || "").trim())
    .filter(Boolean))];
}

export function buildMixedIdValues(values) {
  const strings = toIdStrings(values);
  const objectIds = strings
    .filter((value) => mongoose.Types.ObjectId.isValid(value))
    .map((value) => new mongoose.Types.ObjectId(value));
  return [...objectIds, ...strings];
}

export function withSession(queryOrAggregate, session) {
  return session ? queryOrAggregate.session(session) : queryOrAggregate;
}

export function mongoOptions(session) {
  return session ? { session } : {};
}

export async function resolveUserContext({ userId, profileId, session = null }) {
  const [user, profile] = await Promise.all([
    withSession(
      UserModel.findById(userId).select("+active profileId roles role").lean(),
      session,
    ),
    withSession(ProfileModel.findById(profileId).lean(), session),
  ]);

  if (!user) {
    throw new Error(`User ${userId} not found`);
  }
  if (!profile) {
    throw new Error(`Profile ${profileId} not found`);
  }
  if (!user.profileId || String(user.profileId) !== String(profile._id)) {
    throw new Error("User profileId does not match the provided profileId");
  }

  return { user, profile };
}

export function looksLikeTransactionSupportError(error) {
  const message = String(error?.message || "");
  return (
    /Transaction numbers are only allowed on a replica set member or mongos/i.test(
      message,
    ) ||
    /replica set/i.test(message)
  );
}

export function formatScriptError(error) {
  const message = error?.message ?? String(error);
  if (!looksLikeTransactionSupportError(error)) {
    return message;
  }
  return `${message}. Re-run with --no-transaction if you are intentionally using a standalone MongoDB instance.`;
}

export async function runWithOptionalTransaction({
  useTransaction = true,
  work,
}) {
  const mongoUri = process.env.MONGO_URI;
  if (!mongoUri) {
    throw new Error("Missing MONGO_URI");
  }

  await connectMongo({ mongoUri });

  const session = useTransaction ? await mongoose.startSession() : null;
  let result = null;

  try {
    if (!session) {
      result = await work(null);
    } else {
      await session.withTransaction(async () => {
        result = await work(session);
      });
    }
  } finally {
    if (session) {
      await session.endSession();
    }
    await mongoose.disconnect();
  }

  return result;
}
