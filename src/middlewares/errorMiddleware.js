import { NODE_ENV } from "../config/serverConfig.js";

/**
 * Global error handling middleware
 * @param {Error} err - Error object
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next function
 */
export function errorHandler(err, req, res, next) {
  console.error(err.stack);

  // Different response format based on environment
  res.status(500).json({
    message: "Internal server error",
    error: NODE_ENV === "development" ? err.message : undefined,
  });
}

/**
 * Async handler wrapper to avoid try/catch blocks in routes
 * @param {Function} fn - Async function to wrap
 * @returns {Function} Express middleware function
 */
export function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}
