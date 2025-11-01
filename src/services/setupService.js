// src/services/setupService.js - Modified for New MQTT Format
import { User } from "../config/dbconfig.js";
import mongoose from "mongoose";
import logger from "../utils/logger.js";
import AWS from "aws-sdk";

// Initialize AWS IoT Data client for direct control calls
const iotData = new AWS.IotData({
  endpoint: process.env.IOT_ENDPOINT,
  region: process.env.AWS_REGION || "ap-south-1",
});

/**
 * Helper function to get device thingId from MongoDB
 */
async function getThingIdByDeviceId(deviceId) {
  try {
    const db = mongoose.connection.db;
    
    // Try sensor_metadata collection first
    const sensorColl = db.collection("sensor_metadata");
    const row = await sensorColl.findOne({ deviceid: deviceId });
    if (row && (row.thingid || row.thingId || row.thing_id)) {
      return row.thingid || row.thingId || row.thing_id;
    }

    // Fallback: search users -> spaces -> devices
    const usersColl = db.collection("users");
    const userDoc = await usersColl.findOne(
      { "spaces.devices.device_id": deviceId },
      { projection: { spaces: 1 } }
    );

    if (userDoc && userDoc.spaces) {
      for (const space of userDoc.spaces) {
        if (!space.devices) continue;
        for (const device of space.devices) {
          if (device.device_id === deviceId) {
            return device.thing_name || device.thingid || device.thingId || device.thing_id || null;
          }
        }
      }
    }

    logger.warn(`No thingid found for deviceid: ${deviceId}`);
    return null;
  } catch (error) {
    logger.error(`Error getting thingId: ${error.message}`);
    return null;
  }
}

/**
 * Convert action object to action code format
 * Example: { device_id: "MC1", switch_no: "BM1", set_status: "on" } -> "MC1S1ON"
 */
function convertActionToCode(action, devices) {
  try {
    const actionDevice = devices.find(d => d.device_id === action.device_id);
    if (!actionDevice) {
      logger.error(`Action device ${action.device_id} not found`);
      return null;
    }

    const status = (action.set_status || "").toString().toUpperCase();
    const switchNumber = action.switch_no === "BM2" ? "2" : "1";

    // If the action device is a base module use BM0 as the module prefix
    if (actionDevice.device_type === "base") {
      // Example: BM0S1ON
      return `BM0S${switchNumber}${status}`;
    }

    // For MC/VC or other module types keep previous format (e.g., MC1S1ON, VC2S2OFF)
    let deviceTypePrefix = "BASE";
    if ((actionDevice.device_type || action.device_id).toString().includes("VC") || (actionDevice.device_type || "").toString().toUpperCase().includes("VC") || action.device_id.includes("VALVE")) {
      deviceTypePrefix = "VC";
    } else if ((actionDevice.device_type || action.device_id).toString().includes("MC") || (actionDevice.device_type || "").toString().toUpperCase().includes("MC") || action.device_id.includes("MOTOR")) {
      deviceTypePrefix = "MC";
    } else {
      // Fallback: try to extract alpha prefix from device_id
      const alphaMatch = (action.device_id || "").match(/^[A-Za-z]+/);
      deviceTypePrefix = alphaMatch ? alphaMatch[0].toUpperCase() : "DEV";
    }

    const deviceNumberMatch = action.device_id ? action.device_id.match(/\d+/) : null;
    const deviceNumber = deviceNumberMatch ? deviceNumberMatch[0] : "1";

    return `${deviceTypePrefix}${deviceNumber}S${switchNumber}${status}`;
  } catch (error) {
    logger.error(`Error converting action to code: ${error.message}`);
    return null;
  }
}

/**
 * Auto-control base devices after setting is published
 */
async function autoControlBaseDevices(condition, space) {
  try {
    if (condition.device_type !== "base") {
      return;
    }

    logger.info(`Auto-controlling base device: ${condition.device_id}`);
    
    const thingid = await getThingIdByDeviceId(condition.device_id);
    if (!thingid) {
      logger.error(`No thingid found for condition device: ${condition.device_id}`);
      return;
    }

    const controlTopic = `mqtt/device/${thingid}/control`;

    // Prepare control payload for the condition device
    const controlPayload = {
      deviceid: condition.device_id,
      switch_no: condition.switch_no,
      status: condition.status,
      timestamp: new Date().toISOString(),
      source: "auto_setup_control"
    };

    // Publish control command
    await iotData.publish({
      topic: controlTopic,
      payload: JSON.stringify(controlPayload),
      qos: 0
    }).promise();

    logger.info(`✅ Auto-control published for device ${condition.device_id} to topic: ${controlTopic}`);

    // Auto-control action devices if they are base modules
    if (condition.actions && condition.actions.length > 0) {
      for (const action of condition.actions) {
        try {
          const actionThingid = await getThingIdByDeviceId(action.device_id);
          if (!actionThingid) {
            logger.error(`No thingid found for action device: ${action.device_id}`);
            continue;
          }

          const actionControlTopic = `mqtt/device/${actionThingid}/control`;

          const actionControlPayload = {
            deviceid: action.device_id,
            switch_no: action.switch_no,
            status: action.set_status,
            timestamp: new Date().toISOString(),
            source: "auto_setup_action_control",
            delay: action.delay || 0
          };

          // Add delay if specified
          if (action.delay && action.delay > 0) {
            setTimeout(async () => {
              await iotData.publish({
                topic: actionControlTopic,
                payload: JSON.stringify(actionControlPayload),
                qos: 0
              }).promise();
              logger.info(`✅ Delayed auto-control published for action device ${action.device_id}`);
            }, action.delay * 1000);
          } else {
            await iotData.publish({
              topic: actionControlTopic,
              payload: JSON.stringify(actionControlPayload),
              qos: 0
            }).promise();
            logger.info(`✅ Auto-control published for action device ${action.device_id}`);
          }

        } catch (actionError) {
          logger.error(`Error auto-controlling action device ${action.device_id}: ${actionError.message}`);
        }
      }
    }
  } catch (error) {
    logger.error(`Error in autoControlBaseDevices: ${error.message}`);
  }
}

/**
 * Publish setting to MQTT
 * @param {string} deviceId - The device ID to get thing ID for
 * @param {object} mqttPayload - The complete MQTT payload to publish
 */
async function publishSetting(deviceId, mqttPayload) {
  try {
    // Get the thing ID for the device
    const thingId = await getThingIdByDeviceId(deviceId);
    if (!thingId) {
      logger.error(`No thingId found for device: ${deviceId}`);
      throw new Error(`No thingId found for device: ${deviceId}`);
    }

    const topic = `mqtt/device/${thingId}/setting`;
    
    await iotData.publish({
      topic,
      payload: JSON.stringify(mqttPayload),
      qos: 1
    }).promise();

    logger.info(`✅ Published setting to ${topic}:`, mqttPayload);
  } catch (error) {
    logger.error(`Error publishing setting: ${error.message}`);
    throw error;
  }
}

/**
 * Create a new setup configuration for a space
 */
export async function createSetup(mobileNumber, spaceId, setupData) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    const space = user.spaces[spaceIndex];

    // Validate the condition device
    const conditionDeviceId = setupData.condition.device_id;
    const conditionDevice = space.devices.find(
      (device) => device.device_id === conditionDeviceId
    );
    if (!conditionDevice) {
      throw new Error(
        `Condition device with ID '${conditionDeviceId}' not found in this space`
      );
    }
    if (conditionDevice.device_type !== setupData.condition.device_type) {
      throw new Error(
        `Device type mismatch. Device '${conditionDeviceId}' is of type '${conditionDevice.device_type}', not '${setupData.condition.device_type}'`
      );
    }

    // Validate condition specifics
    if (conditionDevice.device_type === "base") {
      if (!setupData.condition.status || !["on", "off"].includes(setupData.condition.status)) {
        throw new Error("Status field is required and must be 'on' or 'off' for base devices");
      }
      
      if (!setupData.condition.switch_no || !["BM1", "BM2"].includes(setupData.condition.switch_no)) {
        throw new Error("switch_no field is required and must be 'BM1' or 'BM2' for base devices");
      }
    }

    if (conditionDevice.device_type === "tank") {
      // Validate Trigger and Stop values
      if (typeof setupData.condition.trigger !== 'number' || 
          setupData.condition.trigger < 0 || 
          setupData.condition.trigger > 100) {
        throw new Error("Trigger must be a number between 0 and 100 for tank devices");
      }

      if (typeof setupData.condition.stop !== 'number' || 
          setupData.condition.stop < 0 || 
          setupData.condition.stop > 100) {
        throw new Error("Stop must be a number between 0 and 100 for tank devices");
      }

      if (setupData.condition.trigger >= setupData.condition.stop) {
        throw new Error("Trigger must be less than Stop value");
      }

      // Validate slot
      if (!setupData.condition.slot || !["Primary1", "Primary2", "Secondary"].includes(setupData.condition.slot)) {
        throw new Error("Slot field is required and must be 'Primary1', 'Primary2', or 'Secondary' for tank devices");
      }
    }

    // Validate each action device
    for (const action of setupData.condition.actions) {
      const actionDeviceId = action.device_id;
      const actionDevice = space.devices.find(
        (device) => device.device_id === actionDeviceId
      );
      if (!actionDevice) {
        throw new Error(`Action device with ID '${actionDeviceId}' not found in this space`);
      }
      if (actionDevice.device_type !== "base") {
        throw new Error(`Device '${actionDeviceId}' must be of type 'base' to be used in actions`);
      }
      if (!action.set_status || !["on", "off"].includes(action.set_status)) {
        throw new Error("set_status field is required and must be 'on' or 'off' for actions");
      }
      if (!action.switch_no || !["BM1", "BM2"].includes(action.switch_no)) {
        throw new Error("switch_no field is required and must be 'BM1' or 'BM2' for each action");
      }
    }

    // Create condition object
    const conditionData = {
      device_id: setupData.condition.device_id,
      device_type: setupData.condition.device_type,
      actions: setupData.condition.actions
    };

    // Add fields based on device type
    if (setupData.condition.device_type === "base") {
      conditionData.status = setupData.condition.status;
      conditionData.switch_no = setupData.condition.switch_no;
    } else if (setupData.condition.device_type === "tank") {
      conditionData.level = setupData.condition.level;
      conditionData.trigger = setupData.condition.trigger;
      conditionData.stop = setupData.condition.stop;
      conditionData.slot = setupData.condition.slot;
      conditionData.operator = setupData.condition.operator || "<";
    }

    // Create the new setup
    const newSetup = {
      _id: new mongoose.Types.ObjectId(),
      name: setupData.name || `Setup ${(space.setups?.length || 0) + 1}`,
      description: setupData.description || "",
      condition: conditionData,
      active: setupData.active !== undefined ? setupData.active : true,
      created_at: new Date(),
      updated_at: new Date(),
    };

    // Initialize setups array if needed
    if (!user.spaces[spaceIndex].setups) {
      user.spaces[spaceIndex].setups = [];
    }

    user.spaces[spaceIndex].setups.push(newSetup);

    // Save to database BEFORE MQTT operations
    await user.save();
    logger.info(`Setup saved to database for space ${spaceId}`);

    // Fetch the saved setup to get MongoDB-generated IDs and enrich with action codes
    const savedUser = await User.findOne({ mobile_number: mobileNumber });
    const savedSpace = savedUser.spaces.find(s => s._id.toString() === spaceId);
    const savedSetup = savedSpace.setups.find(s => s._id.toString() === newSetup._id.toString());

    // Convert to plain object for manipulation
    const enrichedSetup = savedSetup.toObject();

    // Enrich actions with action codes, device_type, and device_number
    if (enrichedSetup.condition.actions) {
      enrichedSetup.condition.actions = enrichedSetup.condition.actions.map(action => {
        // Generate action code
        const actionCode = convertActionToCode(action, space.devices);
        if (actionCode) {
          action.action_code = actionCode;
        }

        // Add device_type
        if (action.device_id.includes("VC") || action.device_id.includes("VALVE")) {
          action.device_type = "VC";
        } else if (action.device_id.includes("MC") || action.device_id.includes("MOTOR")) {
          action.device_type = "MC";
        } else {
          action.device_type = "base";
        }

        // Add device_number
        const deviceNumberMatch = action.device_id.match(/\d+/);
        if (deviceNumberMatch) {
          action.device_number = deviceNumberMatch[0];
        }

        return action;
      });
    }

    // Handle MQTT operations
    try {
      if (setupData.condition.device_type === "tank") {
        // Find the base device that owns this sensor
        // Match by slave_name or use first base device as fallback
        const baseDeviceForSensor = (space.devices || []).find(d => 
          d.device_type === "base" && 
          d.slave_name && 
          conditionDevice.slave_name && 
          d.slave_name === conditionDevice.slave_name
        );
        
        // If no matching base device found, use the first base device in the space
        const baseDevice = baseDeviceForSensor || (space.devices || []).find(d => d.device_type === "base");
        
        if (!baseDevice) {
          throw new Error("No base device found in this space to publish sensor settings");
        }

        // Convert actions to action codes (A1, A2, A3)
        const actionCodes = {};
        enrichedSetup.condition.actions.forEach((action, index) => {
          if (action.action_code) {
            actionCodes[`A${index + 1}`] = action.action_code;
          }
        });

        // Build MQTT payload with BASE device ID
        const mqttPayload = {
          deviceid: baseDevice.device_id,  // BASE DEVICE ID (e.g., "IOTIQBM_A0525001")
          sensor_no: conditionDevice.slave_name || setupData.condition.sensor_no || "TM1",
          slot: setupData.condition.slot,
          Trigger: setupData.condition.trigger.toString(),
          Stop: setupData.condition.stop.toString(),
          ...actionCodes // A1, A2, A3, etc.
        };

        // Publish using BASE device ID to get its thing ID
        await publishSetting(baseDevice.device_id, mqttPayload);
        logger.info(`✅ Setting published for tank sensor ${conditionDevice.device_id} via base device ${baseDevice.device_id}`, { mqttPayload });
        
      } else if (setupData.condition.device_type === "base") {
        const mqttPayload = {
          deviceid: setupData.condition.device_id,
          switch_no: setupData.condition.switch_no,
          status: setupData.condition.status,
          setup_id: newSetup._id.toString(),
          setup_name: newSetup.name,
          actions: setupData.condition.actions.map(action => ({
            device_id: action.device_id,
            switch_no: action.switch_no,
            set_status: action.set_status,
            delay: action.delay || 0
          }))
        };

        // Publish setting
        await publishSetting(setupData.condition.device_id, mqttPayload);
        logger.info(`✅ Setting published for base device ${setupData.condition.device_id}`);

        // Auto-control base devices
        await autoControlBaseDevices(setupData.condition, space);
      }
    } catch (mqttError) {
      logger.error(`MQTT publishing failed, but setup was saved to DB: ${mqttError.message}`);
    }

    // Construct the response object
    const response = {
      success: true,
      data: {
        setup_id: newSetup._id.toString(),
        name: setupData.name,
        description: setupData.description,
        active: setupData.active,
        condition: {
          device_id: setupData.condition.device_id,
          device_type: setupData.condition.device_type,
          level: setupData.condition.level,
          trigger: setupData.condition.trigger,
          stop: setupData.condition.stop,
          slot: setupData.condition.slot,
          operator: setupData.condition.operator,
        }
      }
    };

    // Add action codes as individual keys (A1, A2, A3, etc.)
    setupData.condition.actions.forEach((action, index) => {
      const actionCode = convertActionToCode(action, space.devices);
      response.data.condition[`A${index + 1}`] = actionCode; // e.g., A1: "BM0S1OFF"
    });

    // Return the response
    return response;
  } catch (error) {
    logger.error(`Error creating setup: ${error.message}`);
    return { success: false, message: error.message };
  }
}

/**
 * Update a setup configuration
 */
export async function updateSetup(mobileNumber, spaceId, setupId, setupData) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    const setupIndex = user.spaces[spaceIndex].setups?.findIndex(
      (setup) => setup._id.toString() === setupId
    );

    if (setupIndex === -1 || setupIndex === undefined) {
      throw new Error("Setup not found");
    }

    const space = user.spaces[spaceIndex];

    // If condition is being updated, validate new condition
    if (setupData.condition) {
      const conditionDeviceId = setupData.condition.device_id;
      const conditionDevice = space.devices.find(
        (device) => device.device_id === conditionDeviceId
      );

      if (!conditionDevice) {
        throw new Error(`Condition device with ID '${conditionDeviceId}' not found in this space`);
      }

      if (conditionDevice.device_type !== setupData.condition.device_type) {
        throw new Error(`Device type mismatch`);
      }

      // Validate conditions based on device type
      if (conditionDevice.device_type === "base") {
        if (!setupData.condition.status || !["on", "off"].includes(setupData.condition.status)) {
          throw new Error("Status field is required and must be 'on' or 'off' for base devices");
        }
        
        if (!setupData.condition.switch_no || !["BM1", "BM2"].includes(setupData.condition.switch_no)) {
          throw new Error("switch_no field is required and must be 'BM1' or 'BM2' for base devices");
        }
      } else if (conditionDevice.device_type === "tank") {
        if (typeof setupData.condition.trigger !== 'number' || 
            setupData.condition.trigger < 0 || 
            setupData.condition.trigger > 100) {
          throw new Error("Trigger must be a number between 0 and 100 for tank devices");
        }

        if (typeof setupData.condition.stop !== 'number' || 
            setupData.condition.stop < 0 || 
            setupData.condition.stop > 100) {
          throw new Error("Stop must be a number between 0 and 100 for tank devices");
        }

        if (setupData.condition.trigger >= setupData.condition.stop) {
          throw new Error("Trigger must be less than Stop value");
        }

        if (!setupData.condition.slot || !["Primary1", "Primary2", "Secondary"].includes(setupData.condition.slot)) {
          throw new Error("Slot field is required and must be 'Primary1', 'Primary2', or 'Secondary'");
        }
      }

      // Validate actions
      if (setupData.condition.actions) {
        for (const action of setupData.condition.actions) {
          const actionDevice = space.devices.find(
            (device) => device.device_id === action.device_id
          );

          if (!actionDevice) {
            throw new Error(`Action device with ID '${action.device_id}' not found in this space`);
          }

          if (actionDevice.device_type !== "base") {
            throw new Error(`Device '${action.device_id}' must be of type 'base' to be used in actions`);
          }

          if (!action.set_status || !["on", "off"].includes(action.set_status)) {
            throw new Error("set_status field is required and must be 'on' or 'off' for actions");
          }
          
          if (!action.switch_no || !["BM1", "BM2"].includes(action.switch_no)) {
            throw new Error("switch_no field is required and must be 'BM1' or 'BM2' for each action");
          }
        }
      }
    }

    // Update fields
    if (setupData.name) {
      user.spaces[spaceIndex].setups[setupIndex].name = setupData.name;
    }

    if (setupData.description !== undefined) {
      user.spaces[spaceIndex].setups[setupIndex].description = setupData.description;
    }

    if (setupData.active !== undefined) {
      user.spaces[spaceIndex].setups[setupIndex].active = setupData.active;
    }

    if (setupData.condition) {
      user.spaces[spaceIndex].setups[setupIndex].condition = setupData.condition;
    }

    user.spaces[spaceIndex].setups[setupIndex].updated_at = new Date();

    // Save to database BEFORE MQTT operations
    await user.save();
    logger.info(`Setup updated and saved to database for space ${spaceId}`);

    // Fetch the updated setup to get enriched data
    const updatedUser = await User.findOne({ mobile_number: mobileNumber });
    const updatedSpace = updatedUser.spaces.find(s => s._id.toString() === spaceId);
    const updatedSetup = updatedSpace.setups.find(s => s._id.toString() === setupId);

    // Convert to plain object for manipulation
    const enrichedSetup = updatedSetup.toObject();

    // Enrich actions with action codes
    if (enrichedSetup.condition.actions) {
      enrichedSetup.condition.actions = enrichedSetup.condition.actions.map(action => {
        const actionCode = convertActionToCode(action, space.devices);
        if (actionCode) {
          action.action_code = actionCode;
        }

        if (action.device_id.includes("VC") || action.device_id.includes("VALVE")) {
          action.device_type = "VC";
        } else if (action.device_id.includes("MC") || action.device_id.includes("MOTOR")) {
          action.device_type = "MC";
        } else {
          action.device_type = "base";
        }

        const deviceNumberMatch = action.device_id.match(/\d+/);
        if (deviceNumberMatch) {
          action.device_number = deviceNumberMatch[0];
        }

        return action;
      });
    }

    // Handle MQTT operations
    try {
      if (setupData.condition) {
        const conditionDevice = space.devices.find(
          (device) => device.device_id === setupData.condition.device_id
        );

        if (setupData.condition.device_type === "tank") {
          // Find the base device that owns this sensor
          const baseDeviceForSensor = (space.devices || []).find(d => 
            d.device_type === "base" && 
            d.slave_name && 
            conditionDevice.slave_name && 
            d.slave_name === conditionDevice.slave_name
          );
          
          const baseDevice = baseDeviceForSensor || (space.devices || []).find(d => d.device_type === "base");
          
          if (!baseDevice) {
            throw new Error("No base device found in this space to publish sensor settings");
          }

          // Convert actions to action codes
          const actionCodes = {};
          setupData.condition.actions.forEach((action, index) => {
            const actionCode = convertActionToCode(action, space.devices);
            if (actionCode) {
              actionCodes[`A${index + 1}`] = actionCode;
            }
          });

          // Build MQTT payload with BASE device ID
          const mqttPayload = {
            deviceid: baseDevice.device_id,
            sensor_no: conditionDevice.slave_name || setupData.condition.sensor_no || "TM1",
            slot: setupData.condition.slot,
            Trigger: setupData.condition.trigger.toString(),
            Stop: setupData.condition.stop.toString(),
            ...actionCodes
          };

          await publishSetting(baseDevice.device_id, mqttPayload);
          logger.info(`✅ Setting updated for tank device ${conditionDevice.device_id} via base device ${baseDevice.device_id}`, { mqttPayload });
          
        } else if (setupData.condition.device_type === "base") {
          const finalSetup = user.spaces[spaceIndex].setups[setupIndex];
          const mqttPayload = {
            deviceid: setupData.condition.device_id,
            switch_no: setupData.condition.switch_no,
            status: setupData.condition.status,
            setup_id: finalSetup._id.toString(),
            setup_name: finalSetup.name,
            actions: setupData.condition.actions.map(action => ({
              device_id: action.device_id,
              switch_no: action.switch_no,
              set_status: action.set_status,
              delay: action.delay || 0
            }))
          };

          await publishSetting(setupData.condition.device_id, mqttPayload);
          logger.info(`✅ Setting updated for base device ${setupData.condition.device_id}`);

          await autoControlBaseDevices(setupData.condition, space);
        }
      }
    } catch (mqttError) {
      logger.error(`MQTT publishing failed during update: ${mqttError.message}`);
    }

    logger.info(`Setup updated successfully for space ${spaceId}`);
    return updatedSetup;
  } catch (error) {
    logger.error(`Error updating setup: ${error.message}`);
    throw error;
  }
}

/**
 * Update setup active status
 */
export async function updateSetupStatus(mobileNumber, spaceId, setupId, active) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    const setupIndex = user.spaces[spaceIndex].setups?.findIndex(
      (setup) => setup._id.toString() === setupId
    );
    if (setupIndex === -1 || setupIndex === undefined) {
      throw new Error("Setup not found");
    }

    user.spaces[spaceIndex].setups[setupIndex].active = active;
    user.spaces[spaceIndex].setups[setupIndex].updated_at = new Date();

    await user.save();

    logger.info(`Setup ${setupId} status updated to ${active}`);
    return user.spaces[spaceIndex].setups[setupIndex];
  } catch (error) {
    logger.error(`Error updating setup status: ${error.message}`);
    throw error;
  }
}

/**
 * Get a single setup by ID
 */
export async function getSetupById(mobileNumber, spaceId, setupId) {
  try {
    logger.info(`Getting setup by ID: ${setupId} for space: ${spaceId}`);
    
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const space = user.spaces.find((space) => space._id.toString() === spaceId);
    if (!space) {
      throw new Error("Space not found");
    }

    const setup = space.setups?.find((setup) => setup._id.toString() === setupId);
    if (!setup) {
      throw new Error("Setup not found");
    }

    const conditionDevice = space.devices.find(
      (device) => device.device_id === setup.condition.device_id
    );

    const cleanActions = setup.condition.actions.map((action) => {
      const actionDevice = space.devices.find(
        (device) => device.device_id === action.device_id
      );

      // Enrich action with device_type, device_number and action_code
      const deviceType = action.device_id.includes("VC") || action.device_id.includes("VALVE") ? "VC" :
                         (action.device_id.includes("MC") || action.device_id.includes("MOTOR")) ? "MC" : "base";
      const deviceNumberMatch = action.device_id.match(/\d+/);
      const deviceNumber = deviceNumberMatch ? deviceNumberMatch[0] : undefined;
      const actionCode = convertActionToCode(action, space.devices);

      return {
        _id: action._id,
        device_id: action.device_id,
        device_name: actionDevice?.device_name || "Unknown Device",
        device_type: deviceType,
        device_number: deviceNumber,
        switch_no: action.switch_no || "BM1",
        set_status: action.set_status,
        delay: action.delay || 0,
        action_code: actionCode || undefined
      };
    });
    
    const conditionResponse = {
      device_id: setup.condition.device_id,
      device_name: conditionDevice?.device_name || "Unknown Device",
      device_type: setup.condition.device_type,
      actions: cleanActions,
    };

    if (setup.condition.device_type === "base") {
      conditionResponse.status = setup.condition.status;
      conditionResponse.switch_no = setup.condition.switch_no || "BM1";
    } else if (setup.condition.device_type === "tank") {
      conditionResponse.level = setup.condition.level;
      conditionResponse.trigger = setup.condition.trigger;
      conditionResponse.stop = setup.condition.stop;
      conditionResponse.slot = setup.condition.slot;
      conditionResponse.operator = setup.condition.operator;
    }

    const cleanSetup = {
      id: setup._id.toString(),
      name: setup.name,
      description: setup.description || "",
      condition: conditionResponse,
      active: setup.active,
      created_at: setup.created_at,
      updated_at: setup.updated_at,
    };

    logger.info(`Successfully retrieved setup ${setupId}`);
    return cleanSetup;
  } catch (error) {
    logger.error(`Error getting setup by ID: ${error.message}`);
    throw error;
  }
}

/**
 * Get all setups for a space
 */
export async function getSetups(mobileNumber, spaceId) {
  try {
    logger.info(`Getting setups for mobile: ${mobileNumber}, spaceId: ${spaceId}`);
    
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      logger.error(`User not found for mobile: ${mobileNumber}`);
      throw new Error("User not found");
    }

    const space = user.spaces.find((space) => space._id.toString() === spaceId);
    if (!space) {
      logger.error(`Space not found for spaceId: ${spaceId}`);
      throw new Error("Space not found");
    }

    const setups = space.setups || [];
    const devices = space.devices || [];

    if (setups.length === 0) {
      logger.info(`No setups found for space ${spaceId}`);
      return [];
    }

    const cleanSetups = setups.map((setup, index) => {
      try {
        if (!setup.condition) {
          logger.warn(`Setup ${setup._id} has no condition data`);
          return null;
        }

        const conditionDevice = devices.find(
          (device) => device && device.device_id === setup.condition.device_id
        );

        const cleanActions = (setup.condition.actions || []).map((action) => {
          if (!action) return null;

          const actionDevice = devices.find(
            (device) => device && device.device_id === action.device_id
          );

          // Enrich action with device_type, device_number and action_code
          const deviceType = action.device_id && (action.device_id.includes("VC") || action.device_id.includes("VALVE")) ? "VC" :
                             (action.device_id && (action.device_id.includes("MC") || action.device_id.includes("MOTOR"))) ? "MC" : "base";
          const deviceNumberMatch = action.device_id ? action.device_id.match(/\d+/) : null;
          const deviceNumber = deviceNumberMatch ? deviceNumberMatch[0] : undefined;
          const actionCode = convertActionToCode(action, devices);

          return {
            device_id: action.device_id,
            device_name: actionDevice?.device_name || "Unknown Device",
            device_type: deviceType,
            device_number: deviceNumber,
            switch_no: action.switch_no || "BM1",
            set_status: action.set_status,
            delay: action.delay || 0,
            action_code: actionCode || undefined
          };
        }).filter(action => action !== null);

        const conditionResponse = {
          device_id: setup.condition.device_id,
          device_name: conditionDevice?.device_name || "Unknown Device",
          device_type: setup.condition.device_type,
          actions: cleanActions,
        };

        if (setup.condition.device_type === "base") {
          conditionResponse.status = setup.condition.status;
          conditionResponse.switch_no = setup.condition.switch_no || "BM1";
        } else if (setup.condition.device_type === "tank") {
          conditionResponse.level = setup.condition.level;
          conditionResponse.trigger = setup.condition.trigger;
          conditionResponse.stop = setup.condition.stop;
          conditionResponse.slot = setup.condition.slot;
          conditionResponse.operator = setup.condition.operator;
        }

        return {
          id: setup._id?.toString() || `temp_${index}`,
          name: setup.name || `Setup ${index + 1}`,
          description: setup.description || "",
          condition: conditionResponse,
          active: setup.active !== undefined ? setup.active : true,
          created_at: setup.created_at || new Date(),
          updated_at: setup.updated_at || setup.created_at || new Date(),
        };
      } catch (setupError) {
        logger.error(`Error processing setup ${setup._id}: ${setupError.message}`);
        return null;
      }
    }).filter(setup => setup !== null);
    
    logger.info(`Successfully processed ${cleanSetups.length} setups for space ${spaceId}`);
    return cleanSetups;
  } catch (error) {
    logger.error(`Error getting setups: ${error.message}`);
    throw error;
  }
}

/**
 * Delete a setup configuration
 */
export async function deleteSetup(mobileNumber, spaceId, setupId) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    const setupIndex = user.spaces[spaceIndex].setups?.findIndex(
      (setup) => setup._id.toString() === setupId
    );

    if (setupIndex === -1 || setupIndex === undefined) {
      throw new Error("Setup not found");
    }

    user.spaces[spaceIndex].setups.splice(setupIndex, 1);
    await user.save();

    logger.info(`Setup deleted: ${setupId}`);
    return { success: true, message: "Setup deleted successfully" };
  } catch (error) {
    logger.error(`Error deleting setup: ${error.message}`);
    throw error;
  }
}