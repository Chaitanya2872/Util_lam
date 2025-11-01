import crypto from "crypto";
import dotenv from "dotenv";
dotenv.config();
// JWT configuration
export const ACCESS_TOKEN_SECRET =
  process.env.ACCESS_TOKEN_SECRET || crypto.randomBytes(64).toString("hex");
export const REFRESH_TOKEN_SECRET =
  process.env.REFRESH_TOKEN_SECRET || crypto.randomBytes(64).toString("hex");
export const ACCESS_TOKEN_EXPIRY = process.env.ACCESS_TOKEN_EXPIRY || "59m";
export const REFRESH_TOKEN_EXPIRY = process.env.REFRESH_TOKEN_EXPIRY || "7d";

// Server configuration
export const NODE_ENV = process.env.NODE_ENV || "development";
