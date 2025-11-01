// src/controllers/userController.js

import {
  getAllUsers,
  getUserById,
  deleteUser,
  reactivateUser
} from "../services/userService.js";

/**
 * Get all users with optional filtering and pagination
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function getUsers(req, res) {
  try {
    const {
      page = 1,
      limit = 10,
      sortBy = 'created_at',
      sortOrder = 'desc',
      includeInactive = 'false',
      search,
      isActive
    } = req.query;

    // Build filters based on query parameters
    const filters = {};
    
    // Search functionality (search in user_name or mobile_number)
    if (search) {
      filters.$or = [
        { user_name: { $regex: search, $options: 'i' } },
        { mobile_number: { $regex: search, $options: 'i' } }
      ];
    }

    // Filter by active status
    if (isActive !== undefined) {
      filters.isActive = isActive === 'true';
    }

    // Options for pagination and sorting
    const options = {
      page: parseInt(page),
      limit: parseInt(limit),
      sortBy,
      sortOrder,
      includeInactive: includeInactive === 'true'
    };

    const result = await getAllUsers(filters, options);
    
    return res.status(200).json(result);
  } catch (error) {
    console.error("Get users controller error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to retrieve users",
      error: process.env.NODE_ENV === "development" ? error.message : undefined
    });
  }
}

/**
 * Get a single user by ID or mobile number
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function getUser(req, res) {
  try {
    const { identifier } = req.params;
    const { type = 'mobile' } = req.query;

    if (!identifier) {
      return res.status(400).json({
        success: false,
        message: "User identifier is required"
      });
    }

    const result = await getUserById(identifier, type);
    
    return res.status(200).json(result);
  } catch (error) {
    console.error("Get user controller error:", error);
    
    if (error.message === "User not found") {
      return res.status(404).json({
        success: false,
        message: "User not found",
        code: "USER_NOT_FOUND"
      });
    }

    if (error.message?.includes("Invalid identifier type")) {
      return res.status(400).json({
        success: false,
        message: "Invalid identifier type. Use 'mobile' or 'id'",
        code: "INVALID_IDENTIFIER_TYPE"
      });
    }

    return res.status(500).json({
      success: false,
      message: "Failed to retrieve user",
      error: process.env.NODE_ENV === "development" ? error.message : undefined
    });
  }
}

/**
 * Delete a user (soft delete by default, hard delete with query parameter)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function removeUser(req, res) {
  try {
    const { identifier } = req.params;
    const { type = 'mobile', hard = 'false' } = req.query;

    if (!identifier) {
      return res.status(400).json({
        success: false,
        message: "User identifier is required"
      });
    }

    const hardDelete = hard === 'true';
    const result = await deleteUser(identifier, type, hardDelete);
    
    return res.status(200).json(result);
  } catch (error) {
    console.error("Delete user controller error:", error);
    
    if (error.message === "User not found") {
      return res.status(404).json({
        success: false,
        message: "User not found",
        code: "USER_NOT_FOUND"
      });
    }

    if (error.message?.includes("Invalid identifier type")) {
      return res.status(400).json({
        success: false,
        message: "Invalid identifier type. Use 'mobile' or 'id'",
        code: "INVALID_IDENTIFIER_TYPE"
      });
    }

    if (error.message?.includes("Failed to delete") || error.message?.includes("Failed to deactivate")) {
      return res.status(500).json({
        success: false,
        message: "Failed to delete user. Please try again.",
        code: "DELETE_OPERATION_FAILED"
      });
    }

    return res.status(500).json({
      success: false,
      message: "An error occurred while deleting user",
      error: process.env.NODE_ENV === "development" ? error.message : undefined
    });
  }
}

/**
 * Reactivate a soft-deleted user
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export async function reactivateUserController(req, res) {
  try {
    const { identifier } = req.params;
    const { type = 'mobile' } = req.query;

    if (!identifier) {
      return res.status(400).json({
        success: false,
        message: "User identifier is required"
      });
    }

    const result = await reactivateUser(identifier, type);
    
    return res.status(200).json(result);
  } catch (error) {
    console.error("Reactivate user controller error:", error);
    
    if (error.message === "Deactivated user not found") {
      return res.status(404).json({
        success: false,
        message: "Deactivated user not found",
        code: "DEACTIVATED_USER_NOT_FOUND"
      });
    }

    if (error.message?.includes("Invalid identifier type")) {
      return res.status(400).json({
        success: false,
        message: "Invalid identifier type. Use 'mobile' or 'id'",
        code: "INVALID_IDENTIFIER_TYPE"
      });
    }

    if (error.message?.includes("Failed to reactivate")) {
      return res.status(500).json({
        success: false,
        message: "Failed to reactivate user. Please try again.",
        code: "REACTIVATE_OPERATION_FAILED"
      });
    }

    return res.status(500).json({
      success: false,
      message: "An error occurred while reactivating user",
      error: process.env.NODE_ENV === "development" ? error.message : undefined
    });
  }
}

