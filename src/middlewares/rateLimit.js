import AppError from "../utils/AppError.js";

function defaultKey(req) {
  return `${req.ip || "unknown"}:${req.originalUrl}`;
}

export default function rateLimit({
  windowMs = 60_000,
  max = 10,
  keyGenerator = defaultKey,
  message = "Too many requests, please try again later.",
} = {}) {
  const hits = new Map();

  return (req, res, next) => {
    const now = Date.now();
    const key = keyGenerator(req);

    const entry = hits.get(key);
    if (!entry || entry.resetAt <= now) {
      hits.set(key, { count: 1, resetAt: now + windowMs });
      return next();
    }

    entry.count += 1;

    if (entry.count > max) {
      const retryAfterSeconds = Math.ceil((entry.resetAt - now) / 1000);
      res.setHeader("Retry-After", String(retryAfterSeconds));
      return next(new AppError(message, 429));
    }

    return next();
  };
}
