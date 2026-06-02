import dotenv from "dotenv";

dotenv.config();

import { createServer } from "node:http";

import app from "./app.js";
import { connectMongo } from "./db.js";
import { initSocket } from "./socket.js";

process.on("uncaughtException", (err) => {
  // eslint-disable-next-line no-console
  console.error("UNCAUGHT EXCEPTION 💥", err);
  process.exit(1);
});

const port = Number(process.env.PORT) || 4000;

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

await connectMongo({ mongoUri });

const server = createServer(app);
initSocket(server);

server.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`API listening on port ${port}`);
});

process.on("unhandledRejection", (err) => {
  // eslint-disable-next-line no-console
  console.error("UNHANDLED REJECTION 💥", err);
  server.close(() => process.exit(1));
});
