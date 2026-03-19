import mongoose from "mongoose";

export async function connectMongo({ mongoUri }) {
  if (!mongoUri) {
    throw new Error("Missing mongoUri");
  }

  if (mongoose.connection.readyState === 1) {
    return mongoose.connection;
  }

  await mongoose.connect(mongoUri);
  return mongoose.connection;
}
