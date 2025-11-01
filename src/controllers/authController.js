// src/controllers/authController.js
import {
  initiateSignUp,
  verifySignUpOTP,
  initiateSignIn,
  verifyOTP,
  refreshAccessToken,
  logoutUser,
  resendSignInOTP,
  resendSignUpOTP,
} from "../services/authService.js";
import { User } from "../config/dbconfig.js";

// Part 1: Initiate sign-in and send OTP via WhatsApp
export async function signinInitiate(req, res) {
  try {
    const { mobile_number, country_code } = req.body;

    if (!mobile_number) {
      return res
        .status(400)
        .json({ success: false, message: "Mobile number is required" });
    }

    const result = await initiateSignIn(mobile_number, country_code || "+91");
    return res.json(result);
  } catch (error) {
    if (error.message === "User not found") {
      return res.status(404).json({
        success: false,
        message: "User not found. Please sign up first.",
        code: "USER_NOT_FOUND",
      });
    }

    if (error.message === "Failed to send OTP via WhatsApp") {
      return res.status(500).json({
        success: false,
        message: "Failed to send OTP. Please try again later.",
        code: "OTP_SEND_FAILED",
      });
    }

    console.error("Sign-in initiation error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal server error" });
  }
}

// Part 2: Verify OTP and complete sign-in
export async function signinVerifyOTP(req, res) {
  try {
    const { mobile_number, otp } = req.body;

    if (!mobile_number) {
      return res
        .status(400)
        .json({ success: false, message: "Mobile number is required" });
    }

    if (!otp) {
      return res
        .status(400)
        .json({ success: false, message: "OTP is required" });
    }

    const result = await verifyOTP(mobile_number, otp);

    // Set cookies for both tokens if verification successful
    if (result.success && result.tokens) {
      const {
        accessToken,
        refreshToken,
        accessTokenExpiry,
        refreshTokenExpiry,
      } = result.tokens;

      // Set HTTP-only cookies
      res.cookie("accessToken", accessToken, {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "strict",
        expires: accessTokenExpiry,
      });

      res.cookie("refreshToken", refreshToken, {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "strict",
        expires: refreshTokenExpiry,
        // Restrict to refresh token endpoint
      });
    }

    return res.json(result);
  } catch (error) {
    if (error.message === "User not found") {
      return res.status(404).json({
        success: false,
        message: "User not found",
        code: "USER_NOT_FOUND",
      });
    }

    if (error.message === "No OTP found for this user") {
      return res.status(400).json({
        success: false,
        message: "Please request an OTP first",
        code: "NO_OTP_FOUND",
      });
    }

    if (error.message?.includes("OTP expired")) {
      return res.status(400).json({
        success: false,
        message: "OTP has expired. Please request a new one",
        code: "OTP_EXPIRED",
      });
    }

    if (error.message === "Incorrect OTP") {
      return res.status(400).json({
        success: false,
        message: "Incorrect OTP. Please try again",
        code: "INCORRECT_OTP",
      });
    }

    if (error.message?.includes("OTP already used")) {
      return res.status(400).json({
        success: false,
        message: "OTP already used. Please request a new one",
        code: "OTP_ALREADY_USED",
      });
    }

    console.error("OTP verification error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal server error" });
  }
}

/**
 * Resend OTP for sign in
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function signinResendOTP(req, res) {
  try {
    const { mobile_number, country_code } = req.body;

    if (!mobile_number) {
      return res
        .status(400)
        .json({ success: false, message: "Mobile number is required" });
    }

    const result = await resendSignInOTP(mobile_number, country_code || "+91");
    return res.json(result);
  } catch (error) {
    if (error.message === "User not found") {
      return res.status(404).json({
        success: false,
        message: "User not found. Please sign up first.",
        code: "USER_NOT_FOUND",
      });
    }

    console.error("Sign-in OTP resend error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal server error" });
  }
}

// Step 1: Initiate sign up and send OTP
export async function signupInitiate(req, res) {
  try {
    const { user_name, mobile_number, country_code } = req.body;

    if (!user_name || !mobile_number) {
      return res.status(400).json({
        success: false,
        message: "Username and mobile number are required",
      });
    }

    const result = await initiateSignUp(
      mobile_number,
      user_name,
      country_code || "+91"
    );

    if (result.code === "USER_EXISTS") {
      return res.status(409).json(result);
    }

    return res.status(200).json(result);
  } catch (error) {
    console.error("Signup initiation error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal server error" });
  }
}

// Step 2: Verify signup OTP and complete registration
export async function signupVerifyOTP(req, res) {
  try {
    const { mobile_number, otp } = req.body;

    if (!mobile_number) {
      return res
        .status(400)
        .json({ success: false, message: "Mobile number is required" });
    }

    if (!otp) {
      return res
        .status(400)
        .json({ success: false, message: "OTP is required" });
    }

    const result = await verifySignUpOTP(mobile_number, otp);

    // Set cookies for both tokens if verification successful
    if (result.success && result.tokens) {
      const {
        accessToken,
        refreshToken,
        accessTokenExpiry,
        refreshTokenExpiry,
      } = result.tokens;

      // Set HTTP-only cookies
      res.cookie("accessToken", accessToken, {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "strict",
        expires: accessTokenExpiry,
      });

      res.cookie("refreshToken", refreshToken, {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "strict",
        expires: refreshTokenExpiry, // Restrict to refresh token endpoint
      });
    }

    return res.status(201).json(result);
  } catch (error) {
    if (error.message?.includes("User not found")) {
      return res.status(404).json({
        success: false,
        message: "User not found. Please initiate signup again.",
        code: "USER_NOT_FOUND",
      });
    }

    if (error.message?.includes("No OTP found")) {
      return res.status(400).json({
        success: false,
        message: "Please request an OTP first",
        code: "NO_OTP_FOUND",
      });
    }

    if (error.message?.includes("OTP expired")) {
      return res.status(400).json({
        success: false,
        message: "OTP has expired. Please request a new one",
        code: "OTP_EXPIRED",
      });
    }

    if (error.message?.includes("Incorrect OTP")) {
      return res.status(400).json({
        success: false,
        message: "Incorrect OTP. Please try again",
        code: "INCORRECT_OTP",
      });
    }

    if (error.message?.includes("OTP already used")) {
      return res.status(400).json({
        success: false,
        message: "OTP already used. Please request a new one",
        code: "OTP_ALREADY_USED",
      });
    }

    console.error("Signup OTP verification error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal server error" });
  }
}

/**
 * Resend OTP for sign up
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function signupResendOTP(req, res) {
  try {
    const { mobile_number, country_code } = req.body;

    if (!mobile_number) {
      return res
        .status(400)
        .json({ success: false, message: "Mobile number is required" });
    }

    const result = await resendSignUpOTP(mobile_number, country_code || "+91");
    return res.json(result);
  } catch (error) {
    if (error.message?.includes("User not found")) {
      return res.status(404).json({
        success: false,
        message: "User not found. Please initiate signup again.",
        code: "USER_NOT_FOUND",
      });
    }

    console.error("Signup OTP resend error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal server error" });
  }
}

/**
 * Handle token refresh
 * To be used with refreshTokenMiddleware
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function refreshToken(req, res) {
  try {
    // Get refresh token from cookie or request body
    const refreshToken = 
      req.cookies?.refreshToken || 
      req.body.refreshToken || 
      req.headers['x-refresh-token'] ||  // Support custom header
      (req.headers.authorization && req.headers.authorization.startsWith('Bearer ') 
        ? req.headers.authorization.substring(7) 
        : null);

    if (!refreshToken) {
      return res.status(401).json({
        success: false,
        message: "Refresh token is required",
        code: "REFRESH_TOKEN_REQUIRED",
      });
    }

    // Refresh the access token
    const { accessToken, accessTokenExpiry } = await refreshAccessToken(
      refreshToken
    );

    // Set the new access token as a cookie
    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "strict",
      expires: accessTokenExpiry,
    });

    // Return the new access token
    return res.status(200).json({
      success: true,
      message: "Token refreshed successfully",
      accessToken,
      accessTokenExpiry,
    });
  } catch (error) {
    console.error("Token refresh error:", error);

    if (error.name === "TokenExpiredError") {
      return res.status(401).json({
        success: false,
        message: "Refresh token has expired, please login again",
        code: "REFRESH_TOKEN_EXPIRED",
      });
    }

    if (error.name === "JsonWebTokenError") {
      return res.status(401).json({
        success: false,
        message: "Invalid refresh token",
        code: "INVALID_REFRESH_TOKEN",
      });
    }

    if (error.message?.includes("User session has expired")) {
      return res.status(401).json({
        success: false,
        message:
          "Your session has expired or you have been logged out. Please sign in again.",
        code: "SESSION_EXPIRED",
      });
    }

    return res.status(500).json({
      success: false,
      message: "Failed to refresh token",
      code: "TOKEN_REFRESH_FAILED",
    });
  }
}

/**
 * Handle user logout
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function logout(req, res) {
  try {
    // Get user's mobile number from token or request body
    const mobileNumber = req.user?.mobile || req.body.mobile_number;

    if (!mobileNumber) {
      return res.status(400).json({
        success: false,
        message: "Mobile number is required for logout",
        code: "MOBILE_NUMBER_REQUIRED",
      });
    }

    // Log the user out by setting isActive to false
    const result = await logoutUser(mobileNumber);

    if (!result.success) {
      return res.status(400).json(result);
    }

    // Clear cookies
    res.clearCookie("accessToken");
    res.clearCookie("refreshToken");

    return res.status(200).json({
      success: true,
      message: "Logout successful",
    });
  } catch (error) {
    console.error("Logout error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred during logout",
    });
  }
}

/**
 * Get authenticated user details
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function getUser(req, res) {
  try {
    const mobileNumber = req.user?.mobile_number;

    if (!mobileNumber) {
      return res.status(401).json({
        success: false,
        message: 'Authentication required'
      });
    }

    // Fetch complete user data from database
    const user = await User.findOne({ mobile_number: mobileNumber })
      .select('-password -refreshToken -otp_record')
      .lean();

    if (!user) {
      return res.status(404).json({
        success: false,
        message: 'User not found'
      });
    }

    const userData = {
      id: user._id.toString(), // Now we have the actual MongoDB _id
      user_name: user.user_name || '',
      mobile_number: user.mobile_number,
      country_code: user.country_code || '+91',
      spaces_count: user.spaces?.length || 0,
      spaces: user.spaces?.map(space => ({
        space_id: space._id.toString(),
        space_name: space.space_name,
        devices: {
          total: space.devices?.length || 0,
          base: space.devices?.filter(d => d.device_type === 'base').length || 0,
          tank: space.devices?.filter(d => d.device_type === 'tank').length || 0
        }
      })) || [],
      created_at: user.createdAt || new Date(),
      last_login: user.lastLogin || user.createdAt || new Date(),
      is_active: !!user.isActive
    };

    return res.json({
      success: true,
      data: userData
    });

  } catch (error) {
    console.error('Error getting user:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to retrieve user details'
    });
  }
}
