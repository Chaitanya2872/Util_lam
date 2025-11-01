// src/routes/userRoutes.js

import express from "express";
import {
  getUsers,
  getUser,
  removeUser,
  reactivateUserController,
} from "../controllers/userController.js";
import { authenticateToken } from "../middlewares/authMiddleware.js"; // Assuming you have auth middleware

const router = express.Router();

/**
 * @route GET /api/users
 * @desc Get all users with optional filtering and pagination
 * @access Protected (requires authentication)
 * @query {number} page - Page number (default: 1)
 * @query {number} limit - Items per page (default: 10)
 * @query {string} sortBy - Sort field (default: 'created_at')
 * @query {string} sortOrder - Sort order 'asc' or 'desc' (default: 'desc')
 * @query {string} includeInactive - Include inactive users 'true' or 'false' (default: 'false')
 * @query {string} search - Search in user_name or mobile_number
 * @query {string} isActive - Filter by active status 'true' or 'false'
 */
router.get("/users", authenticateToken, getUsers);

/**
 * @route GET /api/user/:identifier
 * @desc Get a single user by mobile number or ID
 * @access Protected (requires authentication)
 * @param {string} identifier - Mobile number or user ID
 * @query {string} type - Identifier type 'mobile' or 'id' (default: 'mobile')
 */
router.get("/user/:identifier", authenticateToken, getUser);

/**
 * @route DELETE /api/user/:identifier
 * @desc Delete a user (soft delete by default)
 * @access Protected (requires authentication)
 * @param {string} identifier - Mobile number or user ID
 * @query {string} type - Identifier type 'mobile' or 'id' (default: 'mobile')
 * @query {string} hard - Hard delete 'true' or 'false' (default: 'false')
 */
router.delete("/user/:identifier", authenticateToken, removeUser);

/**
 * @route PATCH /api/user/:identifier/reactivate
 * @desc Reactivate a soft-deleted user
 * @access Protected (requires authentication)
 * @param {string} identifier - Mobile number or user ID
 * @query {string} type - Identifier type 'mobile' or 'id' (default: 'mobile')
 */
router.patch("/user/:identifier/reactivate", authenticateToken, reactivateUserController);



export default router;