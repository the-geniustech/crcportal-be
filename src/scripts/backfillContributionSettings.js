import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { ProfileModel } from "../models/Profile.js";

const PlannedContributionUnitTypes = ["revolving", "endwell", "festive"];

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

const normalizeNumber = (value) => {
  const num =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : NaN;
  return Number.isFinite(num) ? Number(num) : null;
};

const resolveHasPlan = (unitsRaw, storedYear, targetYear) => {
  if (storedYear !== targetYear) return false;
  if (typeof unitsRaw === "number" || typeof unitsRaw === "string") {
    const value = normalizeNumber(unitsRaw);
    return value !== null && value > 0;
  }
  if (!unitsRaw || typeof unitsRaw !== "object") return false;
  return PlannedContributionUnitTypes.some((key) => {
    const value = normalizeNumber(unitsRaw[key]);
    return value !== null && value > 0;
  });
};

const args = parseArgs(process.argv.slice(2));
const now = new Date();
const targetYear = Number(args.year ?? now.getFullYear());
const defaultUnits = normalizeNumber(args.defaultUnits ?? args.default ?? 5);
const isDryRun = Boolean(args["dry-run"]);

if (!Number.isFinite(targetYear)) {
  // eslint-disable-next-line no-console
  console.error("Invalid year provided.");
  process.exit(1);
}

if (!Number.isFinite(defaultUnits) || defaultUnits < 5 || defaultUnits % 5 !== 0) {
  // eslint-disable-next-line no-console
  console.error(
    "Default units must be a number, minimum 5, in multiples of 5.",
  );
  process.exit(1);
}

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const stats = {
  scanned: 0,
  updated: 0,
  skipped: 0,
  dryRun: isDryRun,
  targetYear,
  defaultUnits,
};

await connectMongo({ mongoUri });

const cursor = ProfileModel.find({}).cursor();

for await (const profile of cursor) {
  stats.scanned += 1;
  const settings = profile.contributionSettings || {};
  const storedYear = Number(settings.year);
  const hasPlan = resolveHasPlan(settings.units, storedYear, targetYear);
  if (hasPlan) {
    stats.skipped += 1;
    continue;
  }

  stats.updated += 1;
  if (!isDryRun) {
    profile.contributionSettings = {
      year: targetYear,
      units: {
        revolving: defaultUnits,
        endwell: defaultUnits,
        festive: defaultUnits,
      },
      updatedAt: now,
    };
    await profile.save({ validateBeforeSave: true });
  }
}

// eslint-disable-next-line no-console
console.log(JSON.stringify({ ok: 1, stats }, null, 2));

await mongoose.disconnect();
