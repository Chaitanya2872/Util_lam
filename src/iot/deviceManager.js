// src/iot/deviceManager.js
import { User } from "../config/dbconfig.js";
import { publish } from "../utils/mqttHelper.js";
import { getTopic } from "../config/awsIotConfig.js";
import { createNotification } from "../services/notificationService.js";
import logger from "../utils/logger.js";

/**
 * Main function to handle device status updates and trigger automation
 * @param {String} mobileNumber - User's mobile number
 * @param {String} spaceId - Space ID
 * @param {String} deviceId - Device ID that was updated
 * @param {Object} deviceData - Updated device data
 */
export async function handleDeviceStatusUpdate(
  mobileNumber,
  spaceId,
  deviceId,
  deviceData
) {
  try {
    logger.info(`Handling device status update for device: ${deviceId}`);
    
    // First, update the device status in the database
    await updateDeviceStatus(mobileNumber, spaceId, deviceId, deviceData);
    
    // Then check if this update triggers any setup conditions
    await checkSetupConditions(mobileNumber, spaceId, deviceId, deviceData);
    
  } catch (error) {
    logger.error(`Error handling device status update: ${error.message}`);
  }
}

/**
 * Update device status in the user's space
 * @param {String} mobileNumber - User's mobile number
 * @param {String} spaceId - Space ID
 * @param {String} deviceId - Device ID to update
 * @param {Object} deviceData - New device data
 */
async function updateDeviceStatus(mobileNumber, spaceId, deviceId, deviceData) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      logger.error(`User not found: ${mobileNumber}`);
      return false;
    }

    const space = user.spaces.find((s) => s._id.toString() === spaceId);
    if (!space) {
      logger.error(`Space not found: ${spaceId}`);
      return false;
    }

    const device = space.devices.find((d) => d.device_id === deviceId);
    if (!device) {
      logger.error(`Device not found: ${deviceId}`);
      return false;
    }

    // Update device properties based on device type
    if (deviceData.device_type === "tank") {
      device.level = deviceData.level || device.level;
      device.volume = deviceData.volume || device.volume;
      device.temperature = deviceData.temperature || device.temperature;
    } else if (deviceData.device_type === "base") {
      device.status = deviceData.status || device.status;
    }

    device.last_updated = new Date();
    
    await user.save();
    logger.info(`Device ${deviceId} status updated successfully`);
    return true;
    
  } catch (error) {
    logger.error(`Error updating device status: ${error.message}`);
    return false;
  }
}

/**
 * Check if any setup conditions are met and execute actions
 * @param {String} mobileNumber - User's mobile number
 * @param {String} spaceId - Space ID
 * @param {String} deviceId - Device ID that was updated
 * @param {Object} deviceData - Updated device data
 */
export async function checkSetupConditions(
  mobileNumber,
  spaceId,
  deviceId,
  deviceData
) {
  try {
    // Find the user and space
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      logger.error(`User not found: ${mobileNumber}`);
      return;
    }

    const space = user.spaces.find((s) => s._id.toString() === spaceId);
    if (!space) {
      logger.error(`Space not found: ${spaceId}`);
      return;
    }

    // No setups to check
    if (!space.setups || space.setups.length === 0) {
      logger.info(`No setups found in space: ${spaceId}`);
      return;
    }

    logger.info(
      `Checking ${space.setups.length} setups for triggered conditions`
    );

    // Check each setup to see if this device update triggers it
    for (const setup of space.setups) {
      // Skip inactive setups
      if (!setup.active) {
        logger.info(`Skipping inactive setup: ${setup.name}`);
        continue;
      }

      // Check if this device is the condition device for this setup
      if (setup.condition.device_id === deviceId) {
        logger.info(`Setup ${setup.name} uses device ${deviceId} as condition`);

        // Check if condition is met based on device type
        const conditionMet = await evaluateCondition(setup.condition, deviceData, space);

        // If condition is met, execute actions
        if (conditionMet) {
          logger.info(
            `Condition met for setup: ${setup.name}. Executing actions.`
          );
          await executeActions(mobileNumber, spaceId, setup.condition.actions, setup.name);
          
          // Update setup last triggered time
          setup.last_triggered = new Date();
          await user.save();
        } else {
          logger.info(`Condition not met for setup: ${setup.name}`);
        }
      }
    }
  } catch (error) {
    logger.error(`Error checking setup conditions: ${error.message}`);
  }
}

/**
 * Evaluate if a condition is met
 * @param {Object} condition - The condition object
 * @param {Object} deviceData - Current device data
 * @param {Object} space - Space object containing all devices
 * @returns {Boolean} - Whether condition is met
 */
async function evaluateCondition(condition, deviceData, space) {
  try {
    // Get the actual device from space for most current data
    const conditionDevice = space.devices.find(d => d.device_id === condition.device_id);
    if (!conditionDevice) {
      logger.error(`Condition device not found: ${condition.device_id}`);
      return false;
    }

    let conditionMet = false;

    if (conditionDevice.device_type === "tank") {
      // For tank device, check water level against threshold
      const currentLevel = deviceData.level !== undefined ? deviceData.level : conditionDevice.level;
      const thresholdLevel = condition.level;
      const operator = condition.operator || "<"; // Default to < if not specified

      switch (operator) {
        case "<":
          conditionMet = currentLevel < thresholdLevel;
          break;
        case "<=":
          conditionMet = currentLevel <= thresholdLevel;
          break;
        case ">":
          conditionMet = currentLevel > thresholdLevel;
          break;
        case ">=":
          conditionMet = currentLevel >= thresholdLevel;
          break;
        case "==":
        case "===":
          conditionMet = currentLevel == thresholdLevel;
          break;
        default:
          conditionMet = false;
      }

      logger.info(
        `Tank level condition: ${currentLevel}% ${operator} ${thresholdLevel}% = ${conditionMet}`
      );
    } else if (conditionDevice.device_type === "base") {
      // For base device, check if status matches
      const currentStatus = deviceData.status !== undefined ? deviceData.status : conditionDevice.status;
      const triggerStatus = condition.status;

      conditionMet = currentStatus === triggerStatus;
      logger.info(
        `Base status condition: ${currentStatus} === ${triggerStatus} = ${conditionMet}`
      );
    }

    return conditionMet;
  } catch (error) {
    logger.error(`Error evaluating condition: ${error.message}`);
    return false;
  }
}

/**
 * Execute the actions defined in a setup
 * @param {String} mobileNumber - User's mobile number
 * @param {String} spaceId - Space ID
 * @param {Array} actions - Array of actions to execute
 * @param {String} setupName - Name of the setup for logging
 */
async function executeActions(mobileNumber, spaceId, actions, setupName = "Unknown") {
  try {
    // Find the user and space
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      logger.error(`User not found: ${mobileNumber}`);
      return;
    }

    const space = user.spaces.find((s) => s._id.toString() === spaceId);
    if (!space) {
      logger.error(`Space not found: ${spaceId}`);
      return;
    }

    logger.info(`Executing ${actions.length} actions for setup: ${setupName}`);

    // Execute each action
    for (const action of actions) {
      await executeIndividualAction(action, space, user, setupName);
    }

    // Save user document with updated device statuses
    await user.save();
    logger.info(`All actions executed for setup: ${setupName}`);
    
  } catch (error) {
    logger.error(`Error executing actions: ${error.message}`);
  }
}

/**
 * Execute a single action
 * @param {Object} action - Action to execute
 * @param {Object} space - Space object
 * @param {Object} user - User object
 * @param {String} setupName - Setup name for logging
 */
async function executeIndividualAction(action, space, user, setupName) {
  try {
    // Find the device to control
    const actionDevice = space.devices.find(
      (d) => d.device_id === action.device_id
    );

    if (!actionDevice) {
      logger.error(`Action device not found: ${action.device_id}`);
      return;
    }

    // Only base devices can be controlled
    if (actionDevice.device_type !== "base") {
      logger.error(`Cannot control non-base device: ${action.device_id}`);
      return;
    }

    // Skip if device is already in the target state
    if (actionDevice.status === action.set_status) {
      logger.info(
        `Device ${action.device_id} already in state ${action.set_status}`
      );
      return;
    }

    logger.info(
      `Changing device ${actionDevice.device_name} from ${actionDevice.status} to ${action.set_status}`
    );

    // Update device status locally
    const previousStatus = actionDevice.status;
    actionDevice.status = action.set_status;
    actionDevice.last_updated = new Date();

    // Send MQTT command if device has thing_name
    if (actionDevice.thing_name) {
      const controlTopic = getTopic(
        "control",
        actionDevice.thing_name,
        "control"
      );
      const controlMessage = {
        deviceid: action.device_id,
        switch_no: action.switch_no || "BM1", // Use specified switch or default to first switch
        status: action.set_status,
        triggered_by: "automation",
        setup_name: setupName,
        timestamp: new Date().toISOString()
      };

      logger.info(
        `Sending control command to ${
          actionDevice.thing_name
        }: ${JSON.stringify(controlMessage)}`
      );

      // Publish MQTT message
      try {
        await publish(controlTopic, controlMessage);
        logger.info(`MQTT control message sent successfully`);
      } catch (mqttError) {
        logger.error(
          `Error publishing MQTT control message: ${mqttError.message}`
        );
        // Revert status change if MQTT fails
        actionDevice.status = previousStatus;
        return;
      }
    }

    // Create notification for the action
    await createNotification({
      type: "SETUP_ACTION",
      title: "Automated Action",
      message: `Device ${actionDevice.device_name} turned ${action.set_status} by automation setup "${setupName}"`,
      user_id: user._id,
      data: {
        device_id: action.device_id,
        device_name: actionDevice.device_name,
        previous_status: previousStatus,
        new_status: action.set_status,
        space_name: space.space_name,
        space_id: space._id,
        setup_name: setupName,
        automated: true,
        timestamp: new Date().toISOString()
      },
    });

    logger.info(
      `Executed action: ${actionDevice.device_name} → ${action.set_status}`
    );
    
  } catch (error) {
    logger.error(`Error executing individual action: ${error.message}`);
  }
}

/**
 * Get current device status
 * @param {String} mobileNumber - User's mobile number
 * @param {String} spaceId - Space ID
 * @param {String} deviceId - Device ID
 * @returns {Object|null} - Device data or null if not found
 */
export async function getDeviceStatus(mobileNumber, spaceId, deviceId) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) return null;

    const space = user.spaces.find((s) => s._id.toString() === spaceId);
    if (!space) return null;

    const device = space.devices.find((d) => d.device_id === deviceId);
    return device || null;
    
  } catch (error) {
    logger.error(`Error getting device status: ${error.message}`);
    return null;
  }
}

/**
 * Manually trigger setup evaluation (for testing or manual triggers)
 * @param {String} mobileNumber - User's mobile number
 * @param {String} spaceId - Space ID
 * @param {String} setupId - Setup ID to evaluate
 */
export async function triggerSetupEvaluation(mobileNumber, spaceId, setupId) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      logger.error(`User not found: ${mobileNumber}`);
      return false;
    }

    const space = user.spaces.find((s) => s._id.toString() === spaceId);
    if (!space) {
      logger.error(`Space not found: ${spaceId}`);
      return false;
    }

    const setup = space.setups.find((s) => s._id.toString() === setupId);
    if (!setup) {
      logger.error(`Setup not found: ${setupId}`);
      return false;
    }

    if (!setup.active) {
      logger.error(`Setup is inactive: ${setup.name}`);
      return false;
    }

    // Get the condition device current status
    const conditionDevice = space.devices.find(
      (d) => d.device_id === setup.condition.device_id
    );

    if (!conditionDevice) {
      logger.error(`Condition device not found: ${setup.condition.device_id}`);
      return false;
    }

    // Create device data object for evaluation
    const deviceData = {
      device_type: conditionDevice.device_type,
      status: conditionDevice.status,
      level: conditionDevice.level,
      volume: conditionDevice.volume,
      temperature: conditionDevice.temperature
    };

    // Evaluate condition
    const conditionMet = await evaluateCondition(setup.condition, deviceData, space);

    if (conditionMet) {
      logger.info(`Manual trigger: Condition met for setup ${setup.name}`);
      await executeActions(mobileNumber, spaceId, setup.condition.actions, setup.name);
      
      setup.last_triggered = new Date();
      await user.save();
      return true;
    } else {
      logger.info(`Manual trigger: Condition not met for setup ${setup.name}`);
      return false;
    }
    
  } catch (error) {
    logger.error(`Error in manual setup evaluation: ${error.message}`);
    return false;
  }
}