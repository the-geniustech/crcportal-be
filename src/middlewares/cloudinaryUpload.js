import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import { getCloudinary } from "../services/cloudinary/cloudinaryClient.js";

function formatUploadResult(result) {
  return {
    url: result.secure_url,
    publicId: result.public_id,
    width: result.width ?? null,
    height: result.height ?? null,
    format: result.format ?? null,
    bytes: result.bytes ?? null,
    originalFilename: result.original_filename ?? null,
    resourceType: result.resource_type ?? null,
  };
}

function uploadBufferToCloudinary({ buffer, filename, folder, resourceType = "image" }) {
  const cloudinary = getCloudinary();

  return new Promise((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      {
        folder,
        resource_type: resourceType,
        filename_override: filename,
        use_filename: true,
        unique_filename: true,
      },
      (err, result) => {
        if (err) return reject(err);
        return resolve(result);
      },
    );

    stream.end(buffer);
  });
}

export function cloudinaryUploadSingle({
  fileField,
  bodyField,
  folder,
  resourceType = "image",
} = {}) {
  if (!fileField || !bodyField || !folder) {
    throw new Error("cloudinaryUploadSingle requires fileField, bodyField, folder");
  }

  return catchAsync(async (req, res, next) => {
    if (!req.file) return next();

    if (req.file.fieldname !== fileField) {
      return next(new AppError(`Unexpected file field: ${req.file.fieldname}`, 400));
    }

    const result = await uploadBufferToCloudinary({
      buffer: req.file.buffer,
      filename: req.file.originalname,
      folder,
      resourceType,
    });

    req.body[bodyField] = formatUploadResult(result);

    return next();
  });
}

export function cloudinaryUploadMultiple({
  fileField,
  bodyField,
  folder,
  resourceType = "image",
} = {}) {
  if (!fileField || !bodyField || !folder) {
    throw new Error("cloudinaryUploadMultiple requires fileField, bodyField, folder");
  }

  return catchAsync(async (req, res, next) => {
    const files = Array.isArray(req.files) ? req.files : [];
    if (files.length === 0) return next();

    const uploads = await Promise.all(
      files.map((file) =>
        uploadBufferToCloudinary({
          buffer: file.buffer,
          filename: file.originalname,
          folder,
          resourceType,
        }).then(formatUploadResult),
      ),
    );

    req.body[bodyField] = uploads;

    return next();
  });
}

export function cloudinaryUploadFields({ fields = [], resourceType = "image" } = {}) {
  if (!Array.isArray(fields) || fields.length === 0) {
    throw new Error("cloudinaryUploadFields requires a non-empty fields array");
  }

  return catchAsync(async (req, res, next) => {
    if (!req.files || typeof req.files !== "object") return next();

    for (const field of fields) {
      const { fileField, bodyField, folder } = field;
      if (!fileField || !bodyField || !folder) {
        return next(new AppError("Invalid cloudinary fields config", 500));
      }

      const fileList = Array.isArray(req.files[fileField]) ? req.files[fileField] : [];
      if (fileList.length === 0) continue;

      const uploads = await Promise.all(
        fileList.map((file) =>
          uploadBufferToCloudinary({
            buffer: file.buffer,
            filename: file.originalname,
            folder,
            resourceType,
          }).then(formatUploadResult),
        ),
      );

      req.body[bodyField] = uploads.length === 1 ? uploads[0] : uploads;
    }

    return next();
  });
}
