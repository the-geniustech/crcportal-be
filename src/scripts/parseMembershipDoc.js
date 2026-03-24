import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import AdmZip from "adm-zip";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_INPUT =
  process.env.SEED_DOCX_PATH ??
  "C:\\\\Users\\\\user\\\\Downloads\\\\crc membership list (1).docx";
const DEFAULT_OUTPUT = path.resolve(__dirname, "../seed-data/membership-doc");
const DEFAULT_PASSWORD = process.env.SEED_DEFAULT_PASSWORD ?? "ChangeMe123!";
const DEFAULT_STATUS = process.env.SEED_MEMBERSHIP_STATUS ?? "active";
const DEFAULT_MONTHLY_CONTRIBUTION = Number(
  process.env.SEED_GROUP_MONTHLY_CONTRIBUTION ?? "5000",
);
const DEFAULT_MAX_MEMBERS = Number(
  process.env.SEED_GROUP_MAX_MEMBERS ?? "120",
);
const DEFAULT_GROUP_LOCATION = process.env.SEED_GROUP_LOCATION ?? "Nigeria";
const DEFAULT_GROUP_CATEGORY = process.env.SEED_GROUP_CATEGORY ?? "Community";

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

const decodeXml = (value) =>
  value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");

const extractParagraphText = (xml) => {
  const textNodes = xml.match(/<w:t[^>]*>[\s\S]*?<\/w:t>/g) ?? [];
  if (textNodes.length === 0) return "";
  const combined = textNodes
    .map((node) => {
      const match = node.match(/<w:t[^>]*>([\s\S]*?)<\/w:t>/);
      return match ? decodeXml(match[1]) : "";
    })
    .join("");
  return combined.replace(/\s+/g, " ").trim();
};

const normalizeLine = (line) => line.replace(/\s+/g, " ").trim();

const normalizeNameKey = (value) =>
  value.toLowerCase().replace(/[^a-z0-9]+/g, "");

const normalizeEmail = (value) =>
  value ? String(value).trim().toLowerCase() : null;

const normalizePhone = (value) => {
  if (!value) return null;
  let digits = String(value).replace(/\D+/g, "");
  if (!digits) return null;
  if (digits.startsWith("234") && digits.length >= 12) {
    digits = `0${digits.slice(3)}`;
  }
  if (digits.length === 10) {
    digits = `0${digits}`;
  }
  return digits.length === 11 ? digits : digits;
};

const extractPhoneFromLine = (line) => {
  const matches = line.match(/(?:\+?234|0)?\d{9,13}/g) ?? [];
  for (const match of matches) {
    const normalized = normalizePhone(match);
    if (normalized && normalized.length === 11) return normalized;
  }
  const fallback = normalizePhone(line);
  return fallback && fallback.length === 11 ? fallback : null;
};

const extractEmail = (line) => {
  const match = line.match(
    /([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})/i,
  );
  return match ? normalizeEmail(match[1]) : null;
};

const cleanMemberName = (line) =>
  line.replace(/^\d+\s*[.)-]\s*/, "").trim();

const args = parseArgs(process.argv.slice(2));
const inputPath = args.input ?? DEFAULT_INPUT;
const outputDir = args.output ?? DEFAULT_OUTPUT;
const defaultPassword = args.password ?? DEFAULT_PASSWORD;
const membershipStatus = args.status ?? DEFAULT_STATUS;
const monthlyContribution = Number(args.monthlyContribution ?? DEFAULT_MONTHLY_CONTRIBUTION);
const maxMembers = Number(args.maxMembers ?? DEFAULT_MAX_MEMBERS);
const groupLocation = args.location ?? DEFAULT_GROUP_LOCATION;
const groupCategory = args.category ?? DEFAULT_GROUP_CATEGORY;
const isDryRun = Boolean(args["dry-run"]);

const zip = new AdmZip(inputPath);
const entry = zip.getEntry("word/document.xml");
if (!entry) {
  throw new Error("word/document.xml not found in the docx file.");
}
const documentXml = entry.getData().toString("utf8");

const paragraphs = documentXml.match(/<w:p[\s\S]*?<\/w:p>/g) ?? [];
const lines = paragraphs
  .map((p) => extractParagraphText(p))
  .map(normalizeLine)
  .filter(Boolean);

const groups = [];
let currentGroup = null;
let pendingCoordinatorName = false;

lines.forEach((rawLine) => {
  const line = normalizeLine(rawLine);
  if (!line) return;

  const groupMatch = line.match(/^group\s*(\d+)\s*(.*)$/i);
  if (groupMatch) {
    const groupNumber = Number(groupMatch[1]);
    const groupName = normalizeLine(groupMatch[2] || "") || `Group ${groupNumber}`;
    if (groupNumber >= 1 && groupNumber <= 28) {
      currentGroup = {
        groupNumber,
        groupName,
        coordinatorName: null,
        coordinatorPhone: null,
        coordinatorEmail: null,
        members: [],
      };
      groups.push(currentGroup);
    } else {
      currentGroup = null;
    }
    pendingCoordinatorName = false;
    return;
  }

  if (!currentGroup) return;

  if (pendingCoordinatorName) {
    currentGroup.coordinatorName = line;
    pendingCoordinatorName = false;
    return;
  }

  if (/^coordinator\b/i.test(line)) {
    const nameMatch = line.match(/^coordinator\s*:?\s*(.+)$/i);
    if (nameMatch && nameMatch[1]) {
      currentGroup.coordinatorName = normalizeLine(nameMatch[1]);
    } else {
      pendingCoordinatorName = true;
    }
    return;
  }

  if (/^phone\b/i.test(line)) {
    const phoneMatch = line.match(/^phone\s*(number|no\.?)?\s*:?\s*(.+)$/i);
    if (phoneMatch && phoneMatch[2]) {
      currentGroup.coordinatorPhone = extractPhoneFromLine(phoneMatch[2]);
    }
    return;
  }

  if (/email/i.test(line)) {
    const email = extractEmail(line);
    if (email) currentGroup.coordinatorEmail = email;
    return;
  }

  if (/^(festival|end well|special savings)$/i.test(line)) return;

  const memberName = cleanMemberName(line);
  if (memberName) currentGroup.members.push(memberName);
});

const warnings = [];
if (groups.length !== 28) {
  warnings.push(`Expected 28 groups, parsed ${groups.length}.`);
}

const usedEmails = new Set();
const usedPhones = new Set();
let memberCounter = 0;
let membershipCounter = 0;

const slugifyName = (name) =>
  name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.|\.$/g, "")
    .slice(0, 32);

const makeUniqueEmail = (fullName) => {
  const base = slugifyName(fullName) || "member";
  let suffix = 0;
  let candidate = `${base}@crc.local`;
  while (usedEmails.has(candidate)) {
    suffix += 1;
    candidate = `${base}.${suffix}@crc.local`;
  }
  usedEmails.add(candidate);
  return candidate;
};

const phonePrefixes = ["080", "081", "070", "090", "091"];
let phoneCounter = 0;
const makeUniquePhone = () => {
  let candidate = "";
  do {
    const prefix = phonePrefixes[phoneCounter % phonePrefixes.length];
    const body = String(10000000 + phoneCounter).slice(-8);
    candidate = `${prefix}${body}`;
    phoneCounter += 1;
  } while (usedPhones.has(candidate));
  usedPhones.add(candidate);
  return candidate;
};

const ensureUniqueContact = (email, phone, fallbackName) => {
  let finalEmail = normalizeEmail(email);
  let finalPhone = normalizePhone(phone);

  if (finalEmail && usedEmails.has(finalEmail)) {
    warnings.push(`Duplicate email detected: ${finalEmail}. Generated placeholder.`);
    finalEmail = null;
  }
  if (finalPhone && usedPhones.has(finalPhone)) {
    warnings.push(`Duplicate phone detected: ${finalPhone}. Generated placeholder.`);
    finalPhone = null;
  }

  if (!finalEmail) {
    finalEmail = makeUniqueEmail(fallbackName);
  } else {
    usedEmails.add(finalEmail);
  }

  if (!finalPhone) {
    finalPhone = makeUniquePhone();
  } else {
    usedPhones.add(finalPhone);
  }

  return { email: finalEmail, phone: finalPhone };
};

const groupsSeed = [];
const profilesSeed = [];
const usersSeed = [];
const membershipsSeed = [];

groups.forEach((group) => {
  const groupSeedKey = `group-${String(group.groupNumber).padStart(2, "0")}`;
  const memberSet = new Map();

  group.members.forEach((member) => {
    const key = normalizeNameKey(member);
    if (key) memberSet.set(key, member);
  });

  if (group.coordinatorName) {
    const coordinatorKey = normalizeNameKey(group.coordinatorName);
    if (!memberSet.has(coordinatorKey)) {
      memberSet.set(coordinatorKey, group.coordinatorName);
    }
  }

  const memberEntries = Array.from(memberSet.entries()).map(([key, name]) => ({
    key,
    name,
    isCoordinator:
      group.coordinatorName &&
      key === normalizeNameKey(group.coordinatorName),
  }));

  let coordinatorProfileSeedKey = null;
  let coordinatorEmail = group.coordinatorEmail;
  let coordinatorPhone = group.coordinatorPhone;

  memberEntries.forEach((member) => {
    memberCounter += 1;
    const seedKey = `member-${String(memberCounter).padStart(4, "0")}`;

    const desiredEmail = member.isCoordinator ? coordinatorEmail : null;
    const desiredPhone = member.isCoordinator ? coordinatorPhone : null;
    const contact = ensureUniqueContact(desiredEmail, desiredPhone, member.name);

    if (member.isCoordinator) {
      coordinatorEmail = contact.email;
      coordinatorPhone = contact.phone;
      coordinatorProfileSeedKey = seedKey;
    }

    profilesSeed.push({
      seedKey,
      fullName: member.name,
      email: contact.email,
      phone: contact.phone,
      membershipStatus,
    });

    usersSeed.push({
      seedKey,
      email: contact.email,
      phone: contact.phone,
      password: defaultPassword,
      role: member.isCoordinator ? "groupCoordinator" : "member",
      profileSeedKey: seedKey,
    });

    membershipCounter += 1;
    membershipsSeed.push({
      seedKey: `membership-${String(membershipCounter).padStart(4, "0")}`,
      groupSeedKey,
      profileSeedKey: seedKey,
      role: member.isCoordinator ? "coordinator" : "member",
      status: "active",
      joinedAt: new Date().toISOString(),
    });
  });

  groupsSeed.push({
    seedKey: groupSeedKey,
    groupNumber: group.groupNumber,
    groupName: group.groupName,
    description: `Membership group for ${group.groupName}.`,
    location: groupLocation,
    category: groupCategory,
    meetingFrequency: "monthly",
    meetingDay: "Saturday",
    imageUrl: null,
    isOpen: true,
    monthlyContribution: monthlyContribution,
    maxMembers: maxMembers,
    totalSavings: 0,
    memberCount: memberEntries.length,
    status: "active",
    coordinatorName: group.coordinatorName ?? null,
    coordinatorEmail: coordinatorEmail ?? null,
    coordinatorPhone: coordinatorPhone ?? null,
    coordinatorProfileSeedKey,
  });
});

const meta = {
  inputPath,
  groupsParsed: groups.length,
  groupsSeeded: groupsSeed.length,
  membersSeeded: profilesSeed.length,
  membershipsSeeded: membershipsSeed.length,
  warnings,
};

if (isDryRun) {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ meta, groupsSeed, profilesSeed, usersSeed, membershipsSeed }, null, 2));
  process.exit(0);
}

await fs.mkdir(outputDir, { recursive: true });

const groupsPath = path.join(outputDir, "groups.seed.json");
const profilesPath = path.join(outputDir, "profiles.seed.json");
const usersPath = path.join(outputDir, "users.seed.json");
const membershipsPath = path.join(outputDir, "group-memberships.seed.json");
const metaPath = path.join(outputDir, "membership.seed.meta.json");

await Promise.all([
  fs.writeFile(groupsPath, JSON.stringify(groupsSeed, null, 2)),
  fs.writeFile(profilesPath, JSON.stringify(profilesSeed, null, 2)),
  fs.writeFile(usersPath, JSON.stringify(usersSeed, null, 2)),
  fs.writeFile(membershipsPath, JSON.stringify(membershipsSeed, null, 2)),
  fs.writeFile(metaPath, JSON.stringify(meta, null, 2)),
]);

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      outputDir,
      groupsPath,
      profilesPath,
      usersPath,
      membershipsPath,
      metaPath,
      summary: meta,
    },
    null,
    2,
  ),
);
