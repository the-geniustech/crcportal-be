import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const slugify = (value) =>
  String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);

const buildTimestamp = (date = new Date()) =>
  date.toISOString().replace(/[:.]/g, "-");

export function resolveDefaultContributionReportDir(importMetaUrl) {
  const currentFilePath = fileURLToPath(importMetaUrl);
  const currentDir = path.dirname(currentFilePath);
  return path.resolve(currentDir, "../../../reports/contribution-ingestion");
}

export async function writeStructuredReport({
  outputDir,
  prefix,
  payload,
  year,
  month,
  scope = "",
}) {
  const resolvedOutputDir = path.resolve(outputDir);
  await fs.mkdir(resolvedOutputDir, { recursive: true });

  const safePrefix = slugify(prefix || "report") || "report";
  const safeScope = slugify(scope);
  const timestamp = buildTimestamp();
  const parts = [
    safePrefix,
    year ? `y${year}` : null,
    month ? `m${String(month).padStart(2, "0")}` : null,
    safeScope || null,
    timestamp,
  ].filter(Boolean);

  const fileName = `${parts.join("__")}.json`;
  const filePath = path.join(resolvedOutputDir, fileName);
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return filePath;
}
