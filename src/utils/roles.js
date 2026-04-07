export const UserRoles = ["member", "groupCoordinator", "admin"];

const ROLE_ALIASES = new Map([
  ["coordinator", "groupCoordinator"],
  ["group_coordinator", "groupCoordinator"],
  ["groupcoordinator", "groupCoordinator"],
]);

export function normalizeRole(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const lower = raw.toLowerCase();
  const aliased = ROLE_ALIASES.get(lower);
  if (aliased && UserRoles.includes(aliased)) return aliased;
  if (lower === "admin" || lower === "member") return lower;
  if (UserRoles.includes(raw)) return raw;
  return null;
}

export function normalizeUserRoles(user) {
  const roles = new Set();
  if (user && Array.isArray(user.roles)) {
    for (const role of user.roles) {
      const normalized = normalizeRole(role);
      if (normalized) roles.add(normalized);
    }
  }
  if (user && user.role) {
    const normalized = normalizeRole(user.role);
    if (normalized) roles.add(normalized);
  }
  if (roles.size === 0) roles.add("member");
  return Array.from(roles);
}

export function hasUserRole(user, ...rolesToCheck) {
  if (!user) return false;
  const userRoles = normalizeUserRoles(user);
  const desired = rolesToCheck
    .flat()
    .map((role) => normalizeRole(role))
    .filter(Boolean);
  return desired.some((role) => userRoles.includes(role));
}

export function pickPrimaryRole(roles) {
  const normalized = Array.isArray(roles) ? roles : [];
  const priority = ["admin", "groupCoordinator", "member"];
  for (const role of priority) {
    if (normalized.includes(role)) return role;
  }
  return normalized[0] || "member";
}

export function coerceUserRoles(input) {
  if (!input) return [];
  if (Array.isArray(input)) {
    return input.map((role) => normalizeRole(role)).filter(Boolean);
  }
  const normalized = normalizeRole(input);
  return normalized ? [normalized] : [];
}
