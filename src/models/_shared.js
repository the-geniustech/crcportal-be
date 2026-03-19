import mongoose from "mongoose";

export const { Schema } = mongoose;
export const { ObjectId } = Schema.Types;

export function model(name, schema) {
  // Avoid OverwriteModelError in watch/hot-reload environments.
  return mongoose.models[name] ?? mongoose.model(name, schema);
}

