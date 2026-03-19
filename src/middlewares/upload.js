import multer from "multer";
import AppError from "../utils/AppError.js";

function imageFileFilter(req, file, cb) {
  if (file.mimetype && file.mimetype.startsWith("image/")) {
    return cb(null, true);
  }
  return cb(new AppError("Only image uploads are allowed", 400));
}

function buildMulter({
  limits = { fileSize: 5 * 1024 * 1024 },
  fileFilter = imageFileFilter,
} = {}) {
  return multer({
    storage: multer.memoryStorage(),
    limits,
    fileFilter,
  });
}

export function uploadSingle(fieldName, opts) {
  return buildMulter(opts).single(fieldName);
}

export function uploadMultiple(fieldName, maxCount = 10, opts) {
  return buildMulter(opts).array(fieldName, maxCount);
}

export function uploadFields(fields, opts) {
  return buildMulter(opts).fields(fields);
}
