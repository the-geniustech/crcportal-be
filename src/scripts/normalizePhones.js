import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { UserModel } from "../models/User.js";
import { ProfileModel } from "../models/Profile.js";
import { GroupModel } from "../models/Group.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { PhoneOtpSessionModel } from "../models/PhoneOtpSession.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { normalizeNigerianPhone } from "../utils/phone.js";

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
const clearInvalid = Boolean(args["clear-invalid"]);

const stats = {
  dryRun: isDryRun,
  clearInvalid,
  users: { scanned: 0, updated: 0, invalid: 0 },
  profiles: { scanned: 0, updated: 0, invalid: 0 },
  groups: { scanned: 0, updated: 0, invalid: 0 },
  loanApplications: { scanned: 0, updated: 0, invalid: 0 },
  loanGuarantors: { scanned: 0, updated: 0, invalid: 0 },
  phoneOtpSessions: { scanned: 0, updated: 0, invalid: 0 },
  loanEditRequests: { scanned: 0, updated: 0, invalid: 0 },
};

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const normalizeValue = (value) => {
  if (value === null || value === undefined) {
    return { value, changed: false, invalid: false };
  }
  const raw = String(value).trim();
  if (!raw) return { value, changed: false, invalid: false };
  const normalized = normalizeNigerianPhone(raw);
  if (!normalized) {
    return {
      value: clearInvalid ? null : value,
      changed: clearInvalid,
      invalid: true,
    };
  }
  return { value: normalized, changed: normalized !== raw, invalid: false };
};

const normalizePhoneList = (rawList) => {
  if (!Array.isArray(rawList)) return { list: rawList, changed: false };
  let changed = false;
  const next = rawList.map((item) => {
    if (!item || typeof item !== "object") return item;
    const result = normalizeValue(item.phone);
    if (result.invalid && !clearInvalid) return item;
    if (result.changed || (result.invalid && clearInvalid)) {
      changed = true;
      return { ...item, phone: result.value };
    }
    return item;
  });
  return { list: next, changed };
};

await connectMongo({ mongoUri });

const userCursor = UserModel.find({}).cursor();
for await (const user of userCursor) {
  stats.users.scanned += 1;
  const next = {};
  let invalid = false;

  const phone = normalizeValue(user.phone);
  if (phone.invalid) invalid = true;
  if (phone.changed) next.phone = phone.value;

  const pending = normalizeValue(user.pendingPhone);
  if (pending.invalid) invalid = true;
  if (pending.changed) next.pendingPhone = pending.value;

  if (invalid) stats.users.invalid += 1;
  if (Object.keys(next).length > 0) {
    stats.users.updated += 1;
    if (!isDryRun) {
      user.set(next);
      await user.save({ validateBeforeSave: false });
    }
  }
}

const profileCursor = ProfileModel.find({}).cursor();
for await (const profile of profileCursor) {
  stats.profiles.scanned += 1;
  const next = {};
  let invalid = false;

  const phone = normalizeValue(profile.phone);
  if (phone.invalid) invalid = true;
  if (phone.changed) next.phone = phone.value;

  const nextOfKin = normalizeValue(profile.nextOfKinPhone);
  if (nextOfKin.invalid) invalid = true;
  if (nextOfKin.changed) next.nextOfKinPhone = nextOfKin.value;

  if (invalid) stats.profiles.invalid += 1;
  if (Object.keys(next).length > 0) {
    stats.profiles.updated += 1;
    if (!isDryRun) {
      profile.set(next);
      await profile.save({ validateBeforeSave: false });
    }
  }
}

const groupCursor = GroupModel.find({}).cursor();
for await (const group of groupCursor) {
  stats.groups.scanned += 1;
  const next = {};
  let invalid = false;

  const phone = normalizeValue(group.coordinatorPhone);
  if (phone.invalid) invalid = true;
  if (phone.changed) next.coordinatorPhone = phone.value;

  if (invalid) stats.groups.invalid += 1;
  if (Object.keys(next).length > 0) {
    stats.groups.updated += 1;
    if (!isDryRun) {
      group.set(next);
      await group.save({ validateBeforeSave: false });
    }
  }
}

const loanCursor = LoanApplicationModel.find({}).cursor();
for await (const loan of loanCursor) {
  stats.loanApplications.scanned += 1;
  let invalid = false;
  const next = {};

  const normalizedGuarantors = normalizePhoneList(loan.guarantors);
  if (normalizedGuarantors.changed) {
    next.guarantors = normalizedGuarantors.list;
  }
  if (Array.isArray(loan.guarantors)) {
    for (const g of loan.guarantors) {
      const check = normalizeValue(g?.phone);
      if (check.invalid) invalid = true;
    }
  }

  if (invalid) stats.loanApplications.invalid += 1;
  if (Object.keys(next).length > 0) {
    stats.loanApplications.updated += 1;
    if (!isDryRun) {
      loan.set(next);
      await loan.save({ validateBeforeSave: false });
    }
  }
}

const guarantorCursor = LoanGuarantorModel.find({}).cursor();
for await (const guarantor of guarantorCursor) {
  stats.loanGuarantors.scanned += 1;
  const next = {};
  let invalid = false;

  const phone = normalizeValue(guarantor.guarantorPhone);
  if (phone.invalid) invalid = true;
  if (phone.changed) next.guarantorPhone = phone.value;

  if (invalid) stats.loanGuarantors.invalid += 1;
  if (Object.keys(next).length > 0) {
    stats.loanGuarantors.updated += 1;
    if (!isDryRun) {
      guarantor.set(next);
      await guarantor.save({ validateBeforeSave: false });
    }
  }
}

const otpCursor = PhoneOtpSessionModel.find({}).cursor();
for await (const session of otpCursor) {
  stats.phoneOtpSessions.scanned += 1;
  const next = {};
  let invalid = false;

  const phone = normalizeValue(session.phone);
  if (phone.invalid) invalid = true;
  if (phone.changed) next.phone = phone.value;

  if (invalid) stats.phoneOtpSessions.invalid += 1;
  if (Object.keys(next).length > 0) {
    stats.phoneOtpSessions.updated += 1;
    if (!isDryRun) {
      session.set(next);
      await session.save({ validateBeforeSave: false });
    }
  }
}

const editCursor = LoanApplicationEditRequestModel.find({}).cursor();
for await (const request of editCursor) {
  stats.loanEditRequests.scanned += 1;
  if (!request.payload || typeof request.payload !== "object") continue;

  const payload = request.payload;
  let changed = false;
  let invalid = false;

  if (Array.isArray(payload.guarantors)) {
    const normalizedGuarantors = normalizePhoneList(payload.guarantors);
    if (normalizedGuarantors.changed) {
      payload.guarantors = normalizedGuarantors.list;
      changed = true;
    }
    for (const g of payload.guarantors) {
      const check = normalizeValue(g?.phone);
      if (check.invalid) invalid = true;
    }
  }

  if (invalid) stats.loanEditRequests.invalid += 1;
  if (changed) {
    stats.loanEditRequests.updated += 1;
    if (!isDryRun) {
      request.payload = payload;
      await request.save({ validateBeforeSave: false });
    }
  }
}

// eslint-disable-next-line no-console
console.log(JSON.stringify({ ok: 1, stats }, null, 2));

await mongoose.disconnect();
