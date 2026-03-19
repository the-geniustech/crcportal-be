export default function sendSuccess(res, {
  statusCode = 200,
  message,
  data,
  ...rest
} = {}) {
  const payload = {
    status: "success",
    ...rest,
  };

  if (typeof message === "string") payload.message = message;
  if (typeof data !== "undefined") payload.data = data;

  return res.status(statusCode).json(payload);
}
