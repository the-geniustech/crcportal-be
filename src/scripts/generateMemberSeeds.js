import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse/sync";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_INPUT =
  process.env.SEED_INPUT_PATH ??
  path.resolve(
    __dirname,
    "Champions Revolving Contributions New Membership Registration Form  (Responses) (9).csv",
  );
const DEFAULT_XLSX_FALLBACK =
  process.env.SEED_INPUT_XLSX ??
  path.resolve(
    __dirname,
    "Champions Revolving Contributions New Membership Registration Form  (Responses) (9).xlsx",
  );
const DEFAULT_PASSWORD = process.env.SEED_DEFAULT_PASSWORD ?? "ChangeMe123!";
const DEFAULT_STATUS = process.env.SEED_MEMBERSHIP_STATUS ?? "pending";

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

const normalizeText = (value) => {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text.length ? text : null;
};

const normalizeEmail = (value) => {
  const email = normalizeText(value);
  return email ? email.toLowerCase() : null;
};

const normalizePhone = (value) => {
  if (value === null || value === undefined) return null;
  let phone = value;
  if (typeof phone === "number") {
    phone = String(Math.trunc(phone));
  } else {
    phone = String(phone);
  }
  phone = phone.trim().replace(/\s+/g, "");
  return phone.length ? phone : null;
};

const args = parseArgs(process.argv.slice(2));
const inputPath = args.input ?? DEFAULT_INPUT;
const outputDir = args.output ?? path.resolve(__dirname, "../seed-data");
const defaultPassword = args.password ?? DEFAULT_PASSWORD;
const membershipStatus = args.status ?? DEFAULT_STATUS;
const isDryRun = Boolean(args["dry-run"]);

const fileExists = async (candidate) => {
  try {
    await fs.access(candidate);
    return true;
  } catch {
    return false;
  }
};

const resolveCsvInput = async () => {
  const ext = path.extname(inputPath).toLowerCase();
  if (ext === ".csv") return inputPath;
  const candidate = `${inputPath.replace(/\.[^/.]+$/, "")}.csv`;
  if (await fileExists(candidate)) return candidate;
  return null;
};

const resolvedInput = await resolveCsvInput();
if (!resolvedInput) {
  const fallbackXlsx =
    path.extname(inputPath).toLowerCase() === ".csv"
      ? inputPath.replace(/\.csv$/i, ".xlsx")
      : DEFAULT_XLSX_FALLBACK;
  const fallbackExists = await fileExists(fallbackXlsx);
  // eslint-disable-next-line no-console
  console.error(
    [
      `CSV input not found. Please export the Excel sheet as CSV and pass --input.`,
      `Provided: ${inputPath}`,
      fallbackExists
        ? `Excel detected at: ${fallbackXlsx}`
        : `Expected Excel path (not found): ${fallbackXlsx}`,
      `Tip: powershell -ExecutionPolicy Bypass -File C:\\Users\\user\\Desktop\\xlsx-to-csv.ps1 -Input "${fallbackXlsx}"`,
    ].join("\n"),
  );
  process.exit(1);
}

const rawCsv = await fs.readFile(resolvedInput, "utf8");
const rows = parse(rawCsv, {
  columns: true,
  skip_empty_lines: true,
  trim: true,
});
const sheetName = "CSV";

const profiles = [];
const users = [];
const meta = {
  inputPath: resolvedInput,
  sheetName,
  totalRows: rows.length,
  included: 0,
  skippedMissingContact: 0,
  skippedDuplicateEmail: 0,
  skippedDuplicatePhone: 0,
};

const seenEmails = new Set();
const seenPhones = new Set();

rows.forEach((row, index) => {
  const fullName = normalizeText(row["1.Full Name "]);
  const email = normalizeEmail(row["4. Email Address"]);
  const phonePrimary = normalizePhone(row["3. Mobile Phone number"]);
  const phoneSecondary = normalizePhone(row["2. WhatsApp Phone number"]);
  const phone = phonePrimary ?? phoneSecondary;

  if (!email && !phone) {
    meta.skippedMissingContact += 1;
    return;
  }

  if (email && seenEmails.has(email)) {
    meta.skippedDuplicateEmail += 1;
    return;
  }

  if (phone && seenPhones.has(phone)) {
    meta.skippedDuplicatePhone += 1;
    return;
  }

  const occupation = normalizeText(row["5. Occupation"]);
  const officeAddress = normalizeText(row["6. Office Address"]);
  const homeAddress = normalizeText(row["7. Home Address"]);
  const state = normalizeText(row["8. State of Residence"]);

  let address = null;
  if (homeAddress && officeAddress) {
    address = `Home: ${homeAddress} | Office: ${officeAddress}`;
  } else {
    address = homeAddress ?? officeAddress;
  }

  const seedKey = `member-${String(index + 1).padStart(4, "0")}`;

  profiles.push({
    seedKey,
    fullName,
    email,
    phone,
    address,
    state,
    occupation,
    membershipStatus,
  });

  users.push({
    seedKey,
    email,
    phone,
    password: defaultPassword,
    role: "member",
    profileSeedKey: seedKey,
  });

  if (email) seenEmails.add(email);
  if (phone) seenPhones.add(phone);
  meta.included += 1;
});

if (isDryRun) {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ meta, profiles, users }, null, 2));
  process.exit(0);
}

await fs.mkdir(outputDir, { recursive: true });

const profilesPath = path.join(outputDir, "profiles.seed.json");
const usersPath = path.join(outputDir, "users.seed.json");
const metaPath = path.join(outputDir, "members.seed.meta.json");

await Promise.all([
  fs.writeFile(profilesPath, JSON.stringify(profiles, null, 2)),
  fs.writeFile(usersPath, JSON.stringify(users, null, 2)),
  fs.writeFile(metaPath, JSON.stringify(meta, null, 2)),
]);

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      outputDir,
      profilesPath,
      usersPath,
      metaPath,
      summary: meta,
    },
    null,
    2,
  ),
);
