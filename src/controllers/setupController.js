// src/controllers/setupController.js
import * as setupService from "../services/setupService.js";
import { User } from "../config/dbconfig.js";

/**
 * Create a new setup configuration
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const createSetup = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;
    const setupData = req.body;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    // Validate required fields for single condition
    if (!setupData.condition) {
      return res.status(400).json({
        success: false,
        message: "Condition is required",
      });
    }

    if (!setupData.condition.device_id) {
      return res.status(400).json({
        success: false,
        message: "Condition device ID is required",
      });
    }

    if (!setupData.condition.device_type) {
      return res.status(400).json({
        success: false,
        message: "Condition device type is required",
      });
    }

    // For tank devices, validate level and operator
    if (setupData.condition.device_type === "tank") {
      if (setupData.condition.level === undefined) {
        return res.status(400).json({
          success: false,
          message: "Level is required for tank devices",
        });
      }

      if (setupData.condition.level < 0 || setupData.condition.level > 100) {
        return res.status(400).json({
          success: false,
          message: "Level must be between 0 and 100 for tank devices",
        });
      }

      if (!setupData.condition.operator) {
        return res.status(400).json({
          success: false,
          message: "Operator is required for tank devices",
        });
      }

      if (!["<", ">", "<=", ">=", "=="].includes(setupData.condition.operator)) {
        return res.status(400).json({
          success: false,
          message: "Invalid operator. Must be one of: <, >, <=, >=, ==",
        });
      }
    }


      if (setupData.condition.maximum !== undefined) {
        if (typeof setupData.condition.maximum !== 'number' || 
            setupData.condition.maximum < 0 || 
            setupData.condition.maximum > 100) {
          return res.status(400).json({
            success: false,
            message: "Maximum must be a number between 0 and 100",
          });
        }
      }

      // NEW: Validate minimum parameter (optional)
      if (setupData.condition.minimum !== undefined) {
        if (typeof setupData.condition.minimum !== 'number' || 
            setupData.condition.minimum < 0 || 
            setupData.condition.minimum > 100) {
          return res.status(400).json({
            success: false,
            message: "Minimum must be a number between 0 and 100",
          });
        }
      }

      // NEW: Validate that minimum < maximum if both are provided
      if (setupData.condition.minimum !== undefined && 
          setupData.condition.maximum !== undefined) {
        if (setupData.condition.minimum >= setupData.condition.maximum) {
          return res.status(400).json({
            success: false,
            message: "Minimum must be less than maximum",
          });
        }
      }

    // For base devices, validate status
    if (setupData.condition.device_type === "base") {
      if (!setupData.condition.status) {
        return res.status(400).json({
          success: false,
          message: "Status is required for base devices",
        });
      }

      if (!["on", "off"].includes(setupData.condition.status)) {
        return res.status(400).json({
          success: false,
          message: "Status must be 'on' or 'off' for base devices",
        });
      }
    }

    // Validate actions array
    if (
      !setupData.condition.actions ||
      !Array.isArray(setupData.condition.actions) ||
      setupData.condition.actions.length === 0
    ) {
      return res.status(400).json({
        success: false,
        message: "At least one action is required",
      });
    }

    // Validate each action
    for (let i = 0; i < setupData.condition.actions.length; i++) {
      const action = setupData.condition.actions[i];
      
      if (!action.device_id) {
        return res.status(400).json({
          success: false,
          message: `Action ${i + 1}: Device ID is required`,
        });
      }

      if (!action.set_status) {
        return res.status(400).json({
          success: false,
          message: `Action ${i + 1}: set_status is required`,
        });
      }

      if (!["on", "off"].includes(action.set_status)) {
        return res.status(400).json({
          success: false,
          message: `Action ${i + 1}: set_status must be 'on' or 'off'`,
        });
      }

      // Add this validation in your createSetup function, after device type validation

// For base devices, validate status and switch_no
if (setupData.condition.device_type === "base") {
  if (!setupData.condition.status) {
    return res.status(400).json({
      success: false,
      message: "Status is required for base devices",
    });
  }

  if (!["on", "off"].includes(setupData.condition.status)) {
    return res.status(400).json({
      success: false,
      message: "Status must be 'on' or 'off' for base devices",
    });
  }

  // ✅ NEW: Validate switch_no for base devices
  if (setupData.condition.switch_no && !["BM1", "BM2"].includes(setupData.condition.switch_no)) {
    return res.status(400).json({
      success: false,
      message: "switch_no must be 'BM1' or 'BM2' for base devices",
    });
  }
}

// For tank devices, validate required fields and switch_no
if (setupData.condition.device_type === "tank") {
  if (setupData.condition.level === undefined) {
    return res.status(400).json({
      success: false,
      message: "Level is required for tank devices",
    });
  }

  if (setupData.condition.level < 0 || setupData.condition.level > 100) {
    return res.status(400).json({
      success: false,
      message: "Level must be between 0 and 100 for tank devices",
    });
  }

  if (!setupData.condition.operator) {
    return res.status(400).json({
      success: false,
      message: "Operator is required for tank devices",
    });
  }

  if (!["<", ">", "<=", ">=", "=="].includes(setupData.condition.operator)) {
    return res.status(400).json({
      success: false,
      message: "Invalid operator. Must be one of: <, >, <=, >=, ==",
    });
  }

  // ✅ NEW: Validate switch_no for tank devices (for the action device)
  if (setupData.condition.switch_no && !["BM1", "BM2"].includes(setupData.condition.switch_no)) {
    return res.status(400).json({
      success: false,
      message: "switch_no must be 'BM1' or 'BM2'",
    });
  }
}

      // Optional delay validation
      if (action.delay !== undefined) {
        if (typeof action.delay !== 'number' || action.delay < 0) {
          return res.status(400).json({
            success: false,
            message: `Action ${i + 1}: delay must be a positive number`,
          });
        }
      }
    }

    const setup = await setupService.createSetup(
      mobile_number,
      spaceId,
      setupData
    );

    return res.status(201).json({
      success: true,
      data: setup,
      message: "Setup created successfully",
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message.includes("not found in this space")
    ) {
      statusCode = 404;
    }
    if (
      error.message.includes("must be") ||
      error.message.includes("required") ||
      error.message.includes("between 0 and 100") ||
      error.message.includes("type mismatch") ||
      error.message.includes("Status field") ||
      error.message.includes("Level field") ||
      error.message.includes("Valid operator")
    ) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get all setups for a space
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getSetups = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    const setups = await setupService.getSetups(mobile_number, spaceId);

    return res.status(200).json({
      success: true,
      data: setups,
      message: `Found ${setups.length} setup(s)`,
    });
  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found"
    ) {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};
/**
 * Get a setup by ID
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getSetupById = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, setupId } = req.params;

    if (!spaceId || !setupId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Setup ID are required",
      });
    }

    const setup = await setupService.getSetupById(
      mobile_number,
      spaceId,
      setupId
    );

    return res.status(200).json({
      success: true,
      data: setup,
    });
  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Setup not found"
    ) {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Update a setup
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const updateSetup = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, setupId } = req.params;
    const setupData = req.body;

    if (!spaceId || !setupId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Setup ID are required",
      });
    }

    // Validate condition if being updated
    if (setupData.condition) {
      if (!setupData.condition.device_id) {
        return res.status(400).json({
          success: false,
          message: "Condition device ID is required",
        });
      }

      if (!setupData.condition.device_type) {
        return res.status(400).json({
          success: false,
          message: "Condition device type is required",
        });
      }

      // For tank devices
      if (setupData.condition.device_type === "tank") {
        if (setupData.condition.level === undefined) {
          return res.status(400).json({
            success: false,
            message: "Level is required for tank devices",
          });
        }

        if (setupData.condition.level < 0 || setupData.condition.level > 100) {
          return res.status(400).json({
            success: false,
            message: "Level must be between 0 and 100 for tank devices",
          });
        }

        if (!setupData.condition.operator) {
          return res.status(400).json({
            success: false,
            message: "Operator is required for tank devices",
          });
        }

        if (!["<", ">", "<=", ">=", "=="].includes(setupData.condition.operator)) {
          return res.status(400).json({
            success: false,
            message: "Invalid operator. Must be one of: <, >, <=, >=, ==",
          });
        }
      }

      if (setupData.condition.maximum !== undefined) {
          if (typeof setupData.condition.maximum !== 'number' || 
              setupData.condition.maximum < 0 || 
              setupData.condition.maximum > 100) {
            return res.status(400).json({
              success: false,
              message: "Maximum must be a number between 0 and 100",
            });
          }
        }

        // NEW: Validate minimum parameter in updates
        if (setupData.condition.minimum !== undefined) {
          if (typeof setupData.condition.minimum !== 'number' || 
              setupData.condition.minimum < 0 || 
              setupData.condition.minimum > 100) {
            return res.status(400).json({
              success: false,
              message: "Minimum must be a number between 0 and 100",
            });
            }
        }

        // NEW: Cross-validation in updates
        if (setupData.condition.minimum !== undefined && 
            setupData.condition.maximum !== undefined) {
          if (setupData.condition.minimum >= setupData.condition.maximum) {
            return res.status(400).json({
              success: false,
              message: "Minimum must be less than maximum",
            });
          }
        }


      // For base devices
      if (setupData.condition.device_type === "base") {
        if (!setupData.condition.status) {
          return res.status(400).json({
            success: false,
            message: "Status is required for base devices",
          });
        }

        if (!["on", "off"].includes(setupData.condition.status)) {
          return res.status(400).json({
            success: false,
            message: "Status must be 'on' or 'off' for base devices",
          });
        }
      }

      // Validate actions if provided
      if (setupData.condition.actions) {
        if (!Array.isArray(setupData.condition.actions) || setupData.condition.actions.length === 0) {
          return res.status(400).json({
            success: false,
            message: "At least one action is required",
          });
        }

        for (let i = 0; i < setupData.condition.actions.length; i++) {
          const action = setupData.condition.actions[i];
          
          if (!action.device_id) {
            return res.status(400).json({
              success: false,
              message: `Action ${i + 1}: Device ID is required`,
            });
          }

          if (!action.set_status) {
            return res.status(400).json({
              success: false,
              message: `Action ${i + 1}: set_status is required`,
            });
          }

          if (!["on", "off"].includes(action.set_status)) {
            return res.status(400).json({
              success: false,
              message: `Action ${i + 1}: set_status must be 'on' or 'off'`,
            });
          }
        }
      }
    }

    const updatedSetup = await setupService.updateSetup(
      mobile_number,
      spaceId,
      setupId,
      setupData
    );

    return res.status(200).json({
      success: true,
      data: updatedSetup,
      message: "Setup updated successfully",
    });
  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Setup not found" ||
      error.message.includes("not found in this space")
    ) {
      statusCode = 404;
    }
    if (
      error.message.includes("must be") ||
      error.message.includes("required") ||
      error.message.includes("between 0 and 100") ||
      error.message.includes("type mismatch") ||
      error.message.includes("Status field") ||
      error.message.includes("Level field") ||
      error.message.includes("Valid operator")
    ) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Update setup active status
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const updateSetupStatus = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, setupId } = req.params;
    const { active } = req.body;

    if (!spaceId || !setupId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Setup ID are required",
      });
    }

    if (active === undefined) {
      return res.status(400).json({
        success: false,
        message: "Active status is required",
      });
    }

    if (typeof active !== 'boolean') {
      return res.status(400).json({
        success: false,
        message: "Active status must be a boolean value",
      });
    }

    const updatedSetup = await setupService.updateSetupStatus(
      mobile_number,
      spaceId,
      setupId,
      active
    );

    return res.status(200).json({
      success: true,
      data: updatedSetup,
      message: `Setup ${active ? "activated" : "deactivated"} successfully`,
    });
  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Setup not found"
    ) {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Delete a setup
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const deleteSetup = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, setupId } = req.params;

    if (!spaceId || !setupId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Setup ID are required",
      });
    }

    const result = await setupService.deleteSetup(
      mobile_number,
      spaceId,
      setupId
    );

    return res.status(200).json({
      success: true,
      message: result.message,
    });
  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Setup not found"
    ) {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get available devices for actions in a space
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getAvailableDevicesForActions = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    // Get user and space
    const user = await User.findOne({ mobile_number });
    if (!user) {
      return res.status(404).json({
        success: false,
        message: "User not found",
      });
    }

    const space = user.spaces.find((space) => space._id.toString() === spaceId);
    if (!space) {
      return res.status(404).json({
        success: false,
        message: "Space not found",
      });
    }

    // Filter only base devices for actions
    const actionDevices = space.devices.filter(device => device.device_type === "base");

    return res.status(200).json({
      success: true,
      data: actionDevices,
      message: `Found ${actionDevices.length} device(s) available for actions`,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/**
 * Get available devices for conditions in a space
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getAvailableDevicesForConditions = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    // Get user and space
    const user = await User.findOne({ mobile_number });
    if (!user) {
      return res.status(404).json({
        success: false,
        message: "User not found",
      });
    }

    const space = user.spaces.find((space) => space._id.toString() === spaceId);
    if (!space) {
      return res.status(404).json({
        success: false,
        message: "Space not found",
      });
    }

    // All devices can be used for conditions
    return res.status(200).json({
      success: true,
      data: space.devices,
      message: `Found ${space.devices.length} device(s) available for conditions`,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};