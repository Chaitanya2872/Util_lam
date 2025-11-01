// src/services/authService.js - Complete file with optimized OTP handling

import { User } from "../config/dbconfig.js";
import jwt from "jsonwebtoken";
import axios from "axios";
import dotenv from "dotenv";
import { v4 as uuidv4 } from "uuid";
import logger from '../utils/logger.js';
dotenv.config();

// JWT configuration
const ACCESS_TOKEN_SECRET =
  process.env.ACCESS_TOKEN_SECRET ||
  process.env.JWT_SECRET ||
  "your-secret-key";
const ACCESS_TOKEN_EXPIRY = process.env.ACCESS_TOKEN_EXPIRY || "15m";
const REFRESH_TOKEN_SECRET =
  process.env.REFRESH_TOKEN_SECRET || "your-refresh-secret-key";
const REFRESH_TOKEN_EXPIRY = process.env.REFRESH_TOKEN_EXPIRY || "7d";

// WhatsApp API credentials
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID || "";
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN || "";

/**
 * Generate access token
 * @param {Object} user - User object
 * @returns {String} JWT token
 */
export function generateAccessToken(user) {
  return jwt.sign(
    {
      mobile: user.mobile_number,
      user_id: user.id || user._id || user.mobile_number,
      jti: uuidv4(), // Add unique token ID
    },
    ACCESS_TOKEN_SECRET,
    {
      expiresIn: ACCESS_TOKEN_EXPIRY,
    }
  );
}

/**
 * Generate refresh token
 * @param {Object} user - User object
 * @returns {String} JWT refresh token
 */
export function generateRefreshToken(user) {
  return jwt.sign(
    {
      mobile: user.mobile_number,
      user_id: user.id || user._id || user.mobile_number,
      jti: uuidv4(), // Add unique token ID
    },
    REFRESH_TOKEN_SECRET,
    {
      expiresIn: REFRESH_TOKEN_EXPIRY,
    }
  );
}

/**
 * Generate both access and refresh tokens
 * @param {Object} user - User object
 * @returns {Object} Object containing both tokens and their expiry times
 */
export function generateTokens(user) {
  const accessToken = generateAccessToken(user);
  const refreshToken = generateRefreshToken(user);

  // Calculate expiry timestamps
  const accessTokenExpiry = new Date();
  const accessTokenDuration = ACCESS_TOKEN_EXPIRY.match(/(\d+)([smhd])/);
  const refreshTokenDuration = REFRESH_TOKEN_EXPIRY.match(/(\d+)([smhd])/);

  // Parse expiry duration
  if (accessTokenDuration) {
    const [, value, unit] = accessTokenDuration;
    const multiplier =
      unit === "s"
        ? 1
        : unit === "m"
        ? 60
        : unit === "h"
        ? 3600
        : unit === "d"
        ? 86400
        : 0;
    accessTokenExpiry.setSeconds(
      accessTokenExpiry.getSeconds() + parseInt(value) * multiplier
    );
  } else {
    // Default 15 minutes if format isn't recognized
    accessTokenExpiry.setMinutes(accessTokenExpiry.getMinutes() + 15);
  }

  const refreshTokenExpiry = new Date();
  if (refreshTokenDuration) {
    const [, value, unit] = refreshTokenDuration;
    const multiplier =
      unit === "s"
        ? 1
        : unit === "m"
        ? 60
        : unit === "h"
        ? 3600
        : unit === "d"
        ? 86400
        : 0;
    refreshTokenExpiry.setSeconds(
      refreshTokenExpiry.getSeconds() + parseInt(value) * multiplier
    );
  } else {
    // Default 7 days if format isn't recognized
    refreshTokenExpiry.setDate(refreshTokenExpiry.getDate() + 7);
  }

  return {
    accessToken,
    refreshToken,
    accessTokenExpiry,
    refreshTokenExpiry,
  };
}

/**
 * Verify a JWT access token
 * @param {String} token - Token to verify
 * @returns {Promise} Promise that resolves with decoded payload or rejects with error
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
 * Verify a JWT refresh token
 * @param {String} token - Refresh token to verify
 * @returns {Promise} Promise that resolves with decoded payload or rejects with error
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
 * Refresh an access token using a valid refresh token
 * @param {String} refreshToken - Valid refresh token
 * @returns {Promise} Promise resolving to new access token data
 */
export async function refreshAccessToken(refreshToken) {
  try {
    // Verify the refresh token
    const decoded = await verifyRefreshToken(refreshToken);

    // Get the user from database
    const user = await User.findOne({ mobile_number: decoded.mobile });

    if (!user) {
      throw new Error("User not found");
    }

    // Check if user is active
    if (!user.isActive) {
      throw new Error(
        "User session has expired or logged out. Please sign in again."
      );
    }

    // Generate new access token
    const accessToken = generateAccessToken(user);

    // Calculate expiry timestamp
    const accessTokenExpiry = new Date();
    const accessTokenDuration = ACCESS_TOKEN_EXPIRY.match(/(\d+)([smhd])/);
    if (accessTokenDuration) {
      const [, value, unit] = accessTokenDuration;
      const multiplier =
        unit === "s"
          ? 1
          : unit === "m"
          ? 60
          : unit === "h"
          ? 3600
          : unit === "d"
          ? 86400
          : 0;
      accessTokenExpiry.setSeconds(
        accessTokenExpiry.getSeconds() + parseInt(value) * multiplier
      );
    } else {
      accessTokenExpiry.setMinutes(accessTokenExpiry.getMinutes() + 15); // Default
    }

    return {
      accessToken,
      accessTokenExpiry,
    };
  } catch (error) {
    throw error;
  }
}

// Generate a 6-digit OTP
function generateOTP() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

// Send OTP via WhatsApp
async function sendWhatsAppOTP(phoneNumber, otp) {
  try {
    // Check if environment variables are defined
    if (!PHONE_NUMBER_ID || !ACCESS_TOKEN) {
      console.error("Missing environment variables:", {
        hasPhoneNumberId: !!PHONE_NUMBER_ID,
        hasAccessToken: !!ACCESS_TOKEN,
      });
      throw new Error("WhatsApp API credentials are missing");
    }

    console.log("Making API request to WhatsApp");

    // Add timeout to prevent infinite waiting
    const response = await axios({
      method: "POST",
      url: `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        "Content-Type": "application/json",
      },
      data: {
        messaging_product: "whatsapp",
        recipient_type: "individual",
        to: phoneNumber, // Use the parameter instead of hardcoded number
        type: "template",
        template: {
          name: "sending_otp",
          language: {
            code: "en_US",
          },
          components: [
            {
              type: "body",
              parameters: [
                {
                  type: "text",
                  text: otp,
                },
              ],
            },
            {
              type: "button",
              sub_type: "url",
              index: "0",
              parameters: [
                {
                  type: "text",
                  text: otp,
                },
              ],
            },
          ],
        },
      },
      timeout: 15000, // 15 seconds
    });

    return response.data;
  } catch (error) {
    console.log("Caught error in WhatsApp API call");

    // Check for timeout specifically
    if (error.code === "ECONNABORTED") {
      console.error("WhatsApp API request timed out after 15 seconds");
    } else {
      console.error("WhatsApp API error:");
      console.error("Error code:", error.code);
      console.error("Error message:", error.message);

      // Safely log response data if it exists
      if (error.response) {
        console.error("Status:", error.response.status);
        console.error(
          "Error data:",
          JSON.stringify(error.response.data, null, 2)
        );
      } else {
        console.error("No response received from API");
      }

      console.error("Request details:", {
        url: `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
        phoneNumber,
        templateName: "sending_otp",
      });
    }

    return {
      success: false,
      error: error.message || "Failed to send OTP via WhatsApp",
    };
  }
}

export async function initiateSignIn(mobileNumber, countryCode = "+91") {
  // Find the user
  const user = await User.findOne({ mobile_number: mobileNumber });

  // If user not found - throw error to be caught by controller
  if (!user) {
    logger.warn("⚠️ User not found", { mobileNumber });
    throw new Error("User not found");
  }

  try {
    // Generate OTP
    const otp = generateOTP();

    // Store OTP directly without triggering validation on the entire user document
    await User.updateOne(
      { mobile_number: mobileNumber },
      {
        $set: {
          "otp_record.otp": otp,
          "otp_record.created_at": new Date(),
          "otp_record.is_verified": false,
          isActive: false, // Keep isActive as false until OTP is verified
        },
      }
    );

    // Format phone number with country code if not already included
    const fullPhoneNumber = mobileNumber.startsWith("+")
      ? mobileNumber
      : `${countryCode}${mobileNumber}`;

    // Send OTP via WhatsApp
    await sendWhatsAppOTP(fullPhoneNumber, otp);

    // Log successful OTP send
    logger.info('✅ OTP sent for sign-in', { mobileNumber });

    return {
      success: true,
      message: "OTP sent to your WhatsApp number",
      mobile_number: mobileNumber,
    };
  } catch (error) {
    // Only catch WhatsApp/OTP related errors, not user validation errors
    console.error("WhatsApp OTP sending failed:", error);

    // Log the error details
    logger.error('❌ Failed to send OTP for sign-in', {
      mobileNumber,
      error: error.message
    });

    throw new Error("Failed to send OTP via WhatsApp");
  }
}

export async function verifyOTP(mobileNumber, otpToVerify) {
  try {
    // Find the user
    const user = await User.findOne({ mobile_number: mobileNumber });

    // If user not found
    if (!user) {
      logger.warn("⚠️ User not found", { mobileNumber });
      throw new Error("User not found");
    }

    // Check if user has any OTPs
    if (!user.otp_record) {
      throw new Error("No OTP found for this user");
    }

    // Check if OTP is already verified
    if (user.otp_record.is_verified) {
      throw new Error("OTP already used. Please request a new OTP.");
    }

    // Check if OTP is expired (using a more reasonable timeout - 5 minutes)
    const otpCreatedAt = new Date(user.otp_record.created_at);
    const now = new Date();

    // Calculate time difference in minutes
    const diffInMinutes = (now - otpCreatedAt) / (1000 * 60);

    // If OTP expired
    if (diffInMinutes > 5) {
      logger.warn('⚠️ OTP expired', { mobileNumber });
      // Mark OTP as verified but expired to prevent reuse
      await User.updateOne(
        { mobile_number: mobileNumber },
        { $set: { "otp_record.is_verified": true } }
      );
      throw new Error("OTP expired. Please request a new OTP.");
    }

    // Verify OTP
    // If OTP incorrect
    if (user.otp_record.otp !== otpToVerify) {
      logger.warn('⚠️ Incorrect OTP', { mobileNumber });
      throw new Error("Incorrect OTP");
    }

    // Mark OTP as verified and user as active using direct update
    // This avoids triggering validation on the entire user document
    await User.updateOne(
      { mobile_number: mobileNumber },
      {
        $set: {
          "otp_record.is_verified": true,
          isActive: true,
        },
      }
    );

    // Fetch the user again to get the latest data
    const updatedUser = await User.findOne({ mobile_number: mobileNumber });

    // Generate tokens (both access and refresh tokens)
    const { accessToken, refreshToken, accessTokenExpiry, refreshTokenExpiry } =
      generateTokens(updatedUser);

    // Return user and tokens
    logger.info('✅ OTP verified, user signed in', { mobileNumber });
    return {
      success: true,
      message: "Sign in successful",
      user: {
        user_name: updatedUser.user_name,
        mobile_number: updatedUser.mobile_number,
        user_id: updatedUser._id,
      },
      tokens: {
        accessToken,
        refreshToken,
        accessTokenExpiry,
        refreshTokenExpiry,
      },
    };
  } catch (error) {
    logger.error('❌ OTP verification failed', {
      mobileNumber,
      error: error.message
    });
    throw error;
  }
}

/**
 * Initiate sign up by sending OTP
 * @param {String} mobileNumber - User's mobile number
 * @param {String} userName - User's name
 * @param {String} countryCode - Country code for phone number
 * @returns {Promise<Object>} Response object
 */
export async function initiateSignUp(
  mobileNumber,
  userName,
  countryCode = "+91"
) {
  try {
    // Check if user already exists
    const existingUser = await User.findOne({ mobile_number: mobileNumber });

    if (existingUser) {
      return {
        success: false,
        message: "User with this mobile number already exists",
        code: "USER_EXISTS",
      };
    }

    // Generate OTP
    const otp = generateOTP();

    // Create temporary user object with OTP (not saved to database yet)
    const newUser = new User({
      user_name: userName,
      mobile_number: mobileNumber,
      otp_record: {
        otp,
        created_at: new Date(),
        is_verified: false,
      },
      isActive: false,
      spaces: [
        {
          space_name: "Home",
          address: "Default Address",
          devices: [],
        },
      ], // Create a default space to avoid empty spaces array
    });

    // Save the user, but catch validation errors
    try {
      await newUser.save();
    } catch (saveError) {
      console.error("User save error:", saveError);
      return {
        success: false,
        message: "Failed to create user account. Please try again.",
        error:
          process.env.NODE_ENV === "development"
            ? saveError.message
            : undefined,
      };
    }

    // Format phone number with country code if not already included
    const fullPhoneNumber = mobileNumber.startsWith("+")
      ? mobileNumber
      : `${countryCode}${mobileNumber}`;

    // Send OTP via WhatsApp
    await sendWhatsAppOTP(fullPhoneNumber, otp);

    return {
      success: true,
      message: "OTP sent to your WhatsApp number for signup verification",
      mobile_number: mobileNumber,
    };
  } catch (error) {
    console.error("Signup OTP sending failed:", error);
    return {
      success: false,
      message: "Failed to initiate signup. Please try again later.",
      error: process.env.NODE_ENV === "development" ? error.message : undefined,
    };
  }
}

/**
 * Verify signup OTP and complete registration
 * @param {String} mobileNumber - User's mobile number
 * @param {String} otpToVerify - OTP to verify
 * @returns {Promise<Object>} Response with user and token data
 */
export async function verifySignUpOTP(mobileNumber, otpToVerify) {
  try {
    // Find the user with pending verification
    const user = await User.findOne({ mobile_number: mobileNumber });

    // If user not found
    if (!user) {
      throw new Error("User not found. Please initiate signup again.");
    }

    // Check if user has any OTPs
    if (!user.otp_record) {
      throw new Error(
        "No OTP found for this user. Please initiate signup again."
      );
    }

    // Check if OTP is already verified
    if (user.otp_record.is_verified) {
      throw new Error("OTP already used. Please request a new OTP.");
    }

    // Check if OTP is expired (using a more reasonable timeout - 5 minutes)
    const otpCreatedAt = new Date(user.otp_record.created_at);
    const now = new Date();

    // Calculate time difference in minutes
    const diffInMinutes = (now - otpCreatedAt) / (1000 * 60);

    // If OTP expired
    if (diffInMinutes > 5) {
      // Mark OTP as verified but expired to prevent reuse
      await User.updateOne(
        { mobile_number: mobileNumber },
        { $set: { "otp_record.is_verified": true } }
      );
      throw new Error("OTP expired. Please request a new OTP.");
    }

    // Verify OTP
    if (user.otp_record.otp !== otpToVerify) {
      throw new Error("Incorrect OTP. Please try again.");
    }

    // Mark OTP as verified and user as active using direct update
    await User.updateOne(
      { mobile_number: mobileNumber },
      {
        $set: {
          "otp_record.is_verified": true,
          isActive: true,
        },
      }
    );

    // Fetch the user again to get the latest data
    const updatedUser = await User.findOne({ mobile_number: mobileNumber });

    // Generate tokens
    const { accessToken, refreshToken, accessTokenExpiry, refreshTokenExpiry } =
      generateTokens(updatedUser);

    // Return user and tokens
    return {
      success: true,
      message: "Signup successful",
      user: {
        user_name: updatedUser.user_name,
        mobile_number: updatedUser.mobile_number,
        user_id: updatedUser._id,
      },
      tokens: {
        accessToken,
        refreshToken,
        accessTokenExpiry,
        refreshTokenExpiry,
      },
    };
  } catch (error) {
    throw error;
  }
}

/**
 * Resend OTP for sign in
 * @param {String} mobileNumber - User's mobile number
 * @param {String} countryCode - Country code for phone number (default: +91)
 * @returns {Promise<Object>} Response object
 */
export async function resendSignInOTP(mobileNumber, countryCode = "+91") {
  try {
    // Find the user
    const user = await User.findOne({ mobile_number: mobileNumber });

    // If user not found
    if (!user) {
      logger.warn("⚠️ User not found", { mobileNumber });
      throw new Error("User not found");
    }

    // Generate new OTP
    const otp = generateOTP();

    // Store new OTP directly without triggering validation
    await User.updateOne(
      { mobile_number: mobileNumber },
      {
        $set: {
          "otp_record.otp": otp,
          "otp_record.created_at": new Date(),
          "otp_record.is_verified": false,
        },
      }
    );

    // Format phone number with country code if not already included
    const fullPhoneNumber = mobileNumber.startsWith("+")
      ? mobileNumber
      : `${countryCode}${mobileNumber}`;

    // Send OTP via WhatsApp
    await sendWhatsAppOTP(fullPhoneNumber, otp);

    return {
      success: true,
      message: "New OTP sent to your WhatsApp number",
      mobile_number: mobileNumber,
    };
  } catch (error) {
    console.error("Resend WhatsApp OTP failed:", error);
    return {
      success: false,
      message: "Failed to send OTP to your WhatsApp. Please try again later.",
      error: process.env.NODE_ENV === "development" ? error.message : undefined,
    };
  }
}

/**
 * Resend OTP for sign up
 * @param {String} mobileNumber - User's mobile number
 * @param {String} countryCode - Country code for phone number (default: +91)
 * @returns {Promise<Object>} Response object
 */
export async function resendSignUpOTP(mobileNumber, countryCode = "+91") {
  try {
    // Find the user with pending verification
    const user = await User.findOne({ mobile_number: mobileNumber });

    // If user not found
    if (!user) {
      throw new Error("User not found. Please initiate signup again.");
    }

    // Generate new OTP
    const otp = generateOTP();

    // Store new OTP directly without triggering validation
    await User.updateOne(
      { mobile_number: mobileNumber },
      {
        $set: {
          "otp_record.otp": otp,
          "otp_record.created_at": new Date(),
          "otp_record.is_verified": false,
        },
      }
    );

    // Format phone number with country code if not already included
    const fullPhoneNumber = mobileNumber.startsWith("+")
      ? mobileNumber
      : `${countryCode}${mobileNumber}`;

    // Send OTP via WhatsApp
    await sendWhatsAppOTP(fullPhoneNumber, otp);

    return {
      success: true,
      message: "New OTP sent to your WhatsApp number for signup verification",
      mobile_number: mobileNumber,
    };
  } catch (error) {
    console.error("Resend Signup OTP failed:", error);
    return {
      success: false,
      message: "Failed to resend OTP. Please try again later.",
      error: process.env.NODE_ENV === "development" ? error.message : undefined,
    };
  }
}

/**
 * Logout user by setting isActive to false
 * @param {String} mobileNumber - User's mobile number
 * @returns {Promise<Boolean>} Success status
 */
export async function logoutUser(mobileNumber) {
  try {
    // Direct update of isActive status without triggering document validation
    const result = await User.updateOne(
      { mobile_number: mobileNumber },
      { $set: { isActive: false } }
    );

    if (result.matchedCount === 0) {
      throw new Error("User not found");
    }

    return {
      success: true,
      message: "User logged out successfully",
    };
  } catch (error) {
    console.error("Logout failed:", error);
    return {
      success: false,
      message: "Failed to logout user",
      error: process.env.NODE_ENV === "development" ? error.message : undefined,
    };
  }
}
