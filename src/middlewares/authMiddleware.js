// src/middleware/authMiddleware.js
import {
  verifyAccessToken,
  verifyRefreshToken,
  refreshAccessToken,
} from "../services/authService.js";
import { User } from "../config/dbconfig.js";

/**
 * Middleware to authenticate users via JWT access token
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next function
 */
export function authenticateToken(req, res, next) {
  try {
    // Check for token in Authorization header or cookies
    const authHeader = req.headers["authorization"];
    const tokenFromCookie = req.cookies?.accessToken;

    // No token provided
    if (!authHeader && !tokenFromCookie) {
      return res.status(401).json({
        success: false,
        message: "Authentication required",
        code: "AUTH_REQUIRED",
      });
    }

    // Get token from Authorization header or cookie
    let token;
    if (authHeader) {
      const [bearer, headerToken] = authHeader.split(" ");
      if (bearer !== "Bearer" || !headerToken) {
        return res.status(401).json({
          success: false,
          message: "Invalid authorization format",
          code: "INVALID_AUTH_FORMAT",
        });
      }
      token = headerToken;
    } else {
      token = tokenFromCookie;
    }

    // Verify the token
    verifyAccessToken(token)
      .then(async (decoded) => {
        // Verify user is active in the database
        const user = await User.findOne({ mobile_number: decoded.mobile });

        if (!user || !user.isActive) {
          return res.status(401).json({
            success: false,
            message: "Session expired or logged out. Please login again.",
            code: "SESSION_EXPIRED",
          });
        }

        // Set user info in request object
        req.user = {
          mobile_number: decoded.mobile,
          user_id: decoded.user_id,
          user_name: user.user_name,
        };
        next();
      })
      .catch((err) => {
        if (err.name === "TokenExpiredError") {
          return res.status(401).json({
            success: false,
            message: "Token expired",
            code: "TOKEN_EXPIRED",
          });
        }
        return res.status(403).json({
          success: false,
          message: "Invalid token",
          code: "INVALID_TOKEN",
        });
      });
  } catch (error) {
    console.error("Authentication middleware error:", error);
    next(error);
  }
}

/**
 * Middleware to refresh an expired access token using a refresh token
 * This middleware should be used in a dedicated refresh endpoint
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next function
 */
export function refreshTokenMiddleware(req, res, next) {
  try {
    // Get the refresh token from request body or cookies
    const refreshToken = req.body.refreshToken || req.cookies?.refreshToken;

    if (!refreshToken) {
      return res.status(401).json({
        success: false,
        message: "Refresh token required",
        code: "REFRESH_TOKEN_REQUIRED",
      });
    }

    // Verify the refresh token
    verifyRefreshToken(refreshToken)
      .then(async (decoded) => {
        // Check if user is active
        const user = await User.findOne({ mobile_number: decoded.mobile });

        if (!user || !user.isActive) {
          return res.status(401).json({
            success: false,
            message: "Session expired or logged out. Please login again.",
            code: "SESSION_EXPIRED",
          });
        }

        // Set user info in request object
        req.user = {
          mobile_number: decoded.mobile,
          user_id: decoded.user_id,
          user_name: user.user_name,
        };

        // Attach the verified refresh token to the request for the controller
        req.refreshToken = refreshToken;

        next();
      })
      .catch((err) => {
        if (err.name === "TokenExpiredError") {
          return res.status(401).json({
            success: false,
            message: "Refresh token expired, please login again",
            code: "REFRESH_TOKEN_EXPIRED",
          });
        }
        return res.status(403).json({
          success: false,
          message: "Invalid refresh token",
          code: "INVALID_REFRESH_TOKEN",
        });
      });
  } catch (error) {
    console.error("Refresh token middleware error:", error);
    next(error);
  }
}

/**
 * Optional middleware that checks for an expired access token and attempts to use
 * a refresh token from cookies if available, to automatically refresh the session
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next function
 */
export function autoRefreshMiddleware(req, res, next) {
  try {
    // Check for token in Authorization header or cookies
    const authHeader = req.headers["authorization"];
    const accessToken = req.cookies?.accessToken;
    const refreshToken = req.cookies?.refreshToken;

    // No tokens provided, skip auto-refresh
    if ((!authHeader && !accessToken) || !refreshToken) {
      return next();
    }

    // Get token from Authorization header or cookie
    let token;
    if (authHeader) {
      const [bearer, headerToken] = authHeader.split(" ");
      if (bearer !== "Bearer" || !headerToken) {
        return next();
      }
      token = headerToken;
    } else {
      token = accessToken;
    }

    // Check the access token
    verifyAccessToken(token)
      .then(async (decoded) => {
        // Verify user is active
        const user = await User.findOne({ mobile_number: decoded.mobile });

        if (!user || !user.isActive) {
          return next(); // Let the route handler decide what to do
        }

        // Access token is valid and user is active, set user and continue
        req.user = {
          mobile_number: decoded.mobile,
          user_id: decoded.user_id,
          user_name: user.user_name,
        };
        next();
      })
      .catch(async (err) => {
        // If token is expired and we have a refresh token, try to refresh
        if (err.name === "TokenExpiredError" && refreshToken) {
          try {
            // Verify refresh token first
            const decoded = await verifyRefreshToken(refreshToken);

            // Check if user is active
            const user = await User.findOne({ mobile_number: decoded.mobile });

            if (!user || !user.isActive) {
              return next(); // Let the route handler decide what to do
            }

            // If valid and user is active, get a new access token from the service
            const { accessToken: newAccessToken, accessTokenExpiry } =
              await refreshAccessToken(refreshToken);

            // Set the new token in response headers and cookies
            res.setHeader("X-New-Access-Token", newAccessToken);
            res.cookie("accessToken", newAccessToken, {
              httpOnly: true,
              secure: process.env.NODE_ENV === "production",
              sameSite: "strict",
              expires: accessTokenExpiry,
            });

            // Set user in request and continue
            req.user = {
              mobile_number: decoded.mobile,
              user_id: decoded.user_id,
              user_name: user.user_name,
            };
            next();
          } catch (refreshError) {
            // If refresh fails, just continue to next middleware
            // This allows the route handler to handle auth as needed
            next();
          }
        } else {
          // Other token error, continue to next middleware
          next();
        }
      });
  } catch (error) {
    console.error("Auto-refresh middleware error:", error);
    next(error);
  }
}
