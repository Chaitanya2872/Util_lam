// src/controllers/notificationController.js
import * as notificationService from "../services/notificationService.js";

/**
 * Get notifications for the authenticated user
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getUserNotifications = async (req, res) => {
  try {
    const { user_id } = req.user;
    const { limit, offset, unread } = req.query;

    // Parse query parameters
    const options = {
      limit: limit ? parseInt(limit) : 20,
      offset: offset ? parseInt(offset) : 0,
      unread: unread === "true",
    };

    const result = await notificationService.getUserNotifications(
      user_id,
      options
    );

    return res.status(200).json({
      success: true,
      data: result.notifications,
      pagination: result.pagination,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to retrieve notifications",
    });
  }
};

/**
 * Mark a notification as read
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const markNotificationAsRead = async (req, res) => {
  try {
    const { notificationId } = req.params;

    if (!notificationId) {
      return res.status(400).json({
        success: false,
        message: "Notification ID is required",
      });
    }

    const success = await notificationService.markNotificationAsRead(
      notificationId
    );

    if (!success) {
      return res.status(404).json({
        success: false,
        message: "Notification not found or already read",
      });
    }

    return res.status(200).json({
      success: true,
      message: "Notification marked as read",
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to update notification",
    });
  }
};

/**
 * Mark all notifications as read
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const markAllNotificationsAsRead = async (req, res) => {
  try {
    const { user_id } = req.user;

    const count = await notificationService.markAllNotificationsAsRead(user_id);

    return res.status(200).json({
      success: true,
      message: `${count} notifications marked as read`,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to update notifications",
    });
  }
};
