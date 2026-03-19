import AppError from "../utils/AppError.js";

function handleCastErrorDB(err) {
  return new AppError(`Invalid ${err.path}: ${err.value}.`, 400);
}

function handleDuplicateFieldsDB(err) {
  const value = err?.keyValue ? JSON.stringify(err.keyValue) : "(duplicate)";
  return new AppError(`Duplicate field value: ${value}. Please use another value!`, 400);
}

function handleValidationErrorDB(err) {
  const errors = Object.values(err.errors || {}).map((e) => e.message);
  const message = `Invalid input data. ${errors.join(" ")}`;
  return new AppError(message, 400);
}

function handleJWTError() {
  return new AppError("Invalid token. Please log in again.", 401);
}

function handleJWTExpiredError() {
  return new AppError("Your token has expired. Please log in again.", 401);
}

function sendErrorDev(err, req, res) {
  res.status(err.statusCode).json({
    status: err.status,
    message: err.message,
    error: err,
    stack: err.stack,
  });
}

function sendErrorProd(err, req, res) {
  if (err.isOperational) {
    return res.status(err.statusCode).json({
      status: err.status,
      message: err.message,
    });
  }

  // Programming or unknown error: don't leak details
  // eslint-disable-next-line no-console
  console.error("ERROR 💥", err);

  return res.status(500).json({
    status: "error",
    message: "Something went wrong!",
  });
}

export default function globalErrorHandler(err, req, res, next) {
  const normalized = err instanceof Error ? err : new Error(String(err));

  normalized.statusCode = normalized.statusCode || 500;
  normalized.status = normalized.status || "error";

  let error = normalized;

  if (error.name === "CastError") error = handleCastErrorDB(error);
  if (error.code === 11000) error = handleDuplicateFieldsDB(error);
  if (error.name === "ValidationError") error = handleValidationErrorDB(error);
  if (error.name === "JsonWebTokenError") error = handleJWTError();
  if (error.name === "TokenExpiredError") error = handleJWTExpiredError();

  if (process.env.NODE_ENV === "development") {
    return sendErrorDev(error, req, res);
  }

  return sendErrorProd(error, req, res);
}
