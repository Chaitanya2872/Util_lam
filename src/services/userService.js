// src/services/userService.js

import { User } from "../config/dbconfig.js";

/**
 * Get all users from database
 * @param {Object} filters - Optional filters for querying users
 * @param {Object} options - Optional pagination and sorting options
 * @returns {Promise<Object>} Response with users data
 */
export async function getAllUsers(filters = {}, options = {}) {
  try {
    const {
      page = 1,
      limit = 10,
      sortBy = 'created_at',
      sortOrder = 'desc',
      includeInactive = false
    } = options;

    // Build query based on filters
    const query = { ...filters };
    
    // By default, exclude inactive users unless specifically requested
    if (!includeInactive) {
      query.isActive = true;
    }

    // Calculate skip value for pagination
    const skip = (page - 1) * limit;

    // Build sort object
    const sort = {};
    sort[sortBy] = sortOrder === 'desc' ? -1 : 1;

    // Execute query with pagination and sorting
    const users = await User.find(query)
      .select('-otp_record.otp') // Exclude sensitive OTP data
      .sort(sort)
      .skip(skip)
      .limit(parseInt(limit))
      .lean(); // Use lean() for better performance

    // Get total count for pagination
    const totalUsers = await User.countDocuments(query);
    const totalPages = Math.ceil(totalUsers / limit);

    return {
      success: true,
      data: {
        users,
        pagination: {
          currentPage: parseInt(page),
          totalPages,
          totalUsers,
          hasNextPage: page < totalPages,
          hasPrevPage: page > 1,
          limit: parseInt(limit)
        }
      },
      message: `Retrieved ${users.length} users successfully`
    };
  } catch (error) {
    console.error("Get users error:", error);
    throw new Error("Failed to retrieve users from database");
  }
}

/**
 * Get a single user by mobile number or user ID
 * @param {String} identifier - Mobile number or user ID
 * @param {String} identifierType - 'mobile' or 'id'
 * @returns {Promise<Object>} Response with user data
 */
export async function getUserById(identifier, identifierType = 'mobile') {
  try {
    let query = {};
    
    if (identifierType === 'mobile') {
      query.mobile_number = identifier;
    } else if (identifierType === 'id') {
      query._id = identifier;
    } else {
      throw new Error("Invalid identifier type. Use 'mobile' or 'id'");
    }

    const user = await User.findOne(query)
      .select('-otp_record.otp') // Exclude sensitive OTP data
      .lean();

    if (!user) {
      throw new Error("User not found");
    }

    return {
      success: true,
      data: { user },
      message: "User retrieved successfully"
    };
  } catch (error) {
    console.error("Get user by ID error:", error);
    throw error;
  }
}

/**
 * Delete a user by mobile number or user ID
 * @param {String} identifier - Mobile number or user ID
 * @param {String} identifierType - 'mobile' or 'id'
 * @param {Boolean} hardDelete - Whether to permanently delete or soft delete
 * @returns {Promise<Object>} Response with deletion status
 */
export async function deleteUser(identifier, identifierType = 'mobile', hardDelete = false) {
  try {
    let query = {};
    
    if (identifierType === 'mobile') {
      query.mobile_number = identifier;
    } else if (identifierType === 'id') {
      query._id = identifier;
    } else {
      throw new Error("Invalid identifier type. Use 'mobile' or 'id'");
    }

    // Check if user exists
    const existingUser = await User.findOne(query);
    if (!existingUser) {
      throw new Error("User not found");
    }

    let result;
    
    if (hardDelete) {
      // Permanently delete the user
      result = await User.deleteOne(query);
      
      if (result.deletedCount === 0) {
        throw new Error("Failed to delete user");
      }
      
      return {
        success: true,
        message: "User permanently deleted successfully",
        data: {
          deletedUser: {
            user_name: existingUser.user_name,
            mobile_number: existingUser.mobile_number,
            user_id: existingUser._id
          },
          deletionType: 'permanent'
        }
      };
    } else {
      // Soft delete - set user as inactive and clear sensitive data
      result = await User.updateOne(
        query,
        {
          $set: {
            isActive: false,
            'otp_record.otp': null,
            'otp_record.is_verified': false,
            deleted_at: new Date()
          }
        }
      );
      
      if (result.matchedCount === 0) {
        throw new Error("Failed to deactivate user");
      }
      
      return {
        success: true,
        message: "User deactivated successfully",
        data: {
          deactivatedUser: {
            user_name: existingUser.user_name,
            mobile_number: existingUser.mobile_number,
            user_id: existingUser._id
          },
          deletionType: 'soft'
        }
      };
    }
  } catch (error) {
    console.error("Delete user error:", error);
    throw error;
  }
}

/**
 * Reactivate a soft-deleted user
 * @param {String} identifier - Mobile number or user ID
 * @param {String} identifierType - 'mobile' or 'id'
 * @returns {Promise<Object>} Response with reactivation status
 */
export async function reactivateUser(identifier, identifierType = 'mobile') {
  try {
    let query = {};
    
    if (identifierType === 'mobile') {
      query.mobile_number = identifier;
    } else if (identifierType === 'id') {
      query._id = identifier;
    } else {
      throw new Error("Invalid identifier type. Use 'mobile' or 'id'");
    }

    // Find the deactivated user
    const user = await User.findOne({ ...query, isActive: false });
    
    if (!user) {
      throw new Error("Deactivated user not found");
    }

    // Reactivate the user
    const result = await User.updateOne(
      query,
      {
        $set: {
          isActive: true
        },
        $unset: {
          deleted_at: 1
        }
      }
    );

    if (result.matchedCount === 0) {
      throw new Error("Failed to reactivate user");
    }

    return {
      success: true,
      message: "User reactivated successfully",
      data: {
        reactivatedUser: {
          user_name: user.user_name,
          mobile_number: user.mobile_number,
          user_id: user._id
        }
      }
    };
  } catch (error) {
    console.error("Reactivate user error:", error);
    throw error;
  }
}