// src/utils/jwtHelper.js
import jwt from "jsonwebtoken";
import { v4 as uuidv4 } from "uuid";
import {
  ACCESS_TOKEN_SECRET,
  ACCESS_TOKEN_EXPIRY,
  REFRESH_TOKEN_SECRET,
  REFRESH_TOKEN_EXPIRY,
} from "../config/serverConfig.js";

/**
 * Generate a new access token
 * @param {Object} user - User object containing mobile_number
 * @returns {String} JWT access token
 */
export function generateAccessToken(user) {
  return jwt.sign(
    { sub: user.mobile_number, jti: uuidv4() },
    ACCESS_TOKEN_SECRET,
    { expiresIn: ACCESS_TOKEN_EXPIRY }
  );
}

/**
 * Generate a new refresh token
 * @param {Object} user - User object containing mobile_number
 * @returns {String} JWT refresh token
 */
export function generateRefreshToken(user) {
  return jwt.sign(
    { sub: user.mobile_number, jti: uuidv4() },
    REFRESH_TOKEN_SECRET,
    { expiresIn: REFRESH_TOKEN_EXPIRY }
  );
}

/**
 * Generate both access and refresh tokens
 * @param {Object} user - User object containing mobile_number
 * @returns {Object} Object containing both tokens and their expiry times
 */
export function generateTokens(user) {
  const accessToken = generateAccessToken(user);
  const refreshToken = generateRefreshToken(user);

  // Calculate expiry timestamps
  const accessTokenExpiry = new Date();
  accessTokenExpiry.setSeconds(
    accessTokenExpiry.getSeconds() +
      parseInt(ACCESS_TOKEN_EXPIRY.match(/(\d+)/)[0])
  );

  const refreshTokenExpiry = new Date();
  refreshTokenExpiry.setSeconds(
    refreshTokenExpiry.getSeconds() +
      parseInt(REFRESH_TOKEN_EXPIRY.match(/(\d+)/)[0])
  );

  return {
    accessToken,
    refreshToken,
    accessTokenExpiry,
    refreshTokenExpiry,
  };
}

/**
 * Verify an access token
 * @param {String} token - JWT access token to verify
 * @returns {Promise} Resolves with decoded payload or rejects with error
 */
export function verifyAccessToken(token) {
  return new Promise((resolve, reject) => {
    jwt.verify(token, ACCESS_TOKEN_SECRET, (err, decoded) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(decoded);
    });
  });
}

/**
 * Verify a refresh token
 * @param {String} token - JWT refresh token to verify
 * @returns {Promise} Resolves with decoded payload or rejects with error
 */
export function verifyRefreshToken(token) {
  return new Promise((resolve, reject) => {
    jwt.verify(token, REFRESH_TOKEN_SECRET, (err, decoded) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(decoded);
    });
  });
}

/**
 * Refresh the access token using a valid refresh token
 * @param {String} refreshToken - Valid refresh token
 * @param {Object} user - User object (typically from database after verifying refresh token)
 * @returns {Promise} Resolves with new access token or rejects with error
 */
export async function refreshAccessToken(refreshToken, user) {
  try {
    // Verify the refresh token first
    const decoded = await verifyRefreshToken(refreshToken);

    // Check if the user in token matches the provided user
    if (decoded.sub !== user.mobile_number) {
      throw new Error("User mismatch in refresh token");
    }

    // Generate a new access token
    const newAccessToken = generateAccessToken(user);

    // Calculate new expiry timestamp
    const accessTokenExpiry = new Date();
    accessTokenExpiry.setSeconds(
      accessTokenExpiry.getSeconds() +
        parseInt(ACCESS_TOKEN_EXPIRY.match(/(\d+)/)[0])
    );

    return {
      accessToken: newAccessToken,
      accessTokenExpiry,
    };
  } catch (error) {
    throw error;
  }
}

/**
 * Decode a token without verification
 * Useful for extracting payload data when verification is not needed
 * @param {String} token - JWT token to decode
 * @returns {Object|null} Decoded token payload or null if invalid
 */
export function decodeToken(token) {
  try {
    return jwt.decode(token);
  } catch (error) {
    return null;
  }
}
