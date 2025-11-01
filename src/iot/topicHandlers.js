// src/iot/topicHandlers.js - FIXED VERSION
import logger from "../utils/logger.js";
import { User } from "../config/dbconfig.js";
import { createNotification } from "../services/notificationService.js";
import { publish } from "../utils/mqttHelper.js";
import { getTopic } from "../config/awsIotConfig.js";
import { checkSetupConditions } from "./deviceManager.js";
import { saveDeviceResponse, saveMqttDataToMongo } from "../services/migratedDataService.js";


// Handle device update messages (water level changes, status changes)
export async function handleUpdateMessage(topic, message) {
  try {
    // Extract deviceId from message
    const deviceId = message.deviceid;

    if (!deviceId) {
      logger.error("Update message missing deviceid:", message);
      return;
    }

    logger.info(`Received update for device ${deviceId}`);

    // ✅ Save to tank_readings collection
    try {
      await saveMqttDataToMongo({
        deviceid: deviceId,
        sensor_no: message.sensor_no,
        switch_no: message.switch_no,
        level: message.level || message.value,
        value: message.value,
        status: message.status,
        message_type: 'update',
        timestamp: message.timestamp || new Date(),
        thingid: message.thingId,
        raw_data: message
      });
      logger.info(`✅ Update saved to tank_readings for device ${deviceId}`);
    } catch (saveError) {
      logger.error(`❌ Error saving to tank_readings: ${saveError.message}`);
    }

    // Find the device in the database
    const user = await User.findOne({ "spaces.devices.device_id": deviceId });

    if (!user) {
      logger.error(`No user found with device ${deviceId}`);
      return;
    }

    // Find the specific space and device
    let deviceUpdated = false;
    let updatedDevice = null;
    let spaceName = "";

    for (const space of user.spaces) {
      const deviceIndex = space.devices.findIndex(
        (d) => d.device_id === deviceId
      );

      if (deviceIndex !== -1) {
        spaceName = space.space_name;
        const device = space.devices[deviceIndex];

        // Update device based on type
        if (device.device_type === "tank" && message.level !== undefined) {
          // Update tank level
          const previousLevel = device.level;
          device.level = message.level;
          device.last_updated = new Date();

          logger.info(
            `Updated tank device ${deviceId} level from ${previousLevel}% to ${message.level}%`
          );

          updatedDevice = {
            device_id: deviceId,
            device_name: device.device_name,
            device_type: "tank",
            level: message.level,
            previous_level: previousLevel,
            space_name: spaceName,
            space_id: space._id,
          };

          deviceUpdated = true;
        } else if (
          device.device_type === "base" &&
          message.status !== undefined
        ) {
          // Update base device status
          const previousStatus = device.status;
          device.status = message.status;
          device.last_updated = new Date();

          logger.info(
            `Updated base device ${deviceId} status from ${previousStatus} to ${message.status}`
          );

          updatedDevice = {
            device_id: deviceId,
            device_name: device.device_name,
            device_type: "base",
            status: message.status,
            previous_status: previousStatus,
            space_name: spaceName,
            space_id: space._id,
          };

          deviceUpdated = true;
        }

        // If device was updated, save changes and create notification
        if (deviceUpdated) {
          await user.save();

          // Create notification for significant changes
          await createNotification({
            type:
              device.device_type === "tank"
                ? "TANK_LEVEL_CHANGE"
                : "BASE_STATUS_CHANGE",
            title:
              device.device_type === "tank"
                ? "Tank Level Changed"
                : "Base Device Status Changed",
            message:
              device.device_type === "tank"
                ? `Tank ${device.device_name} level changed to ${message.level}%`
                : `Base device ${device.device_name} is now ${message.status}`,
            user_id: user._id,
            data: updatedDevice,
          });

          // Check if any setups should be triggered by this update
          await checkSetupConditions(
            user.mobile_number,
            space._id.toString(),
            deviceId,
            device
          );

          break;
        }
      }
    }

    if (!deviceUpdated) {
      logger.warn(`Device ${deviceId} found but no updates were made`);
    }
  } catch (error) {
    logger.error("Error handling update message:", error);
  }
}

// Handle device alive messages
export async function handleAliveMessage(topic, message) {
  try {
    // Extract deviceId from message
    const deviceId = message.deviceid;

    if (!deviceId) {
      logger.error("Alive message missing deviceid:", message);
      return;
    }

    logger.info(`Received alive message from device ${deviceId}`);

    // ✅ Save to tank_readings collection
    try {
      await saveMqttDataToMongo({
        deviceid: deviceId,
        message_type: 'alive_reply',
        timestamp: message.timestamp || new Date(),
        thingid: message.thingId,
        raw_data: message
      });
      logger.info(`✅ Alive message saved to tank_readings for device ${deviceId}`);
    } catch (saveError) {
      logger.error(`❌ Error saving alive message: ${saveError.message}`);
    }

    // Find the device in the database
    const user = await User.findOne({ "spaces.devices.device_id": deviceId });

    if (!user) {
      logger.error(`No user found with device ${deviceId}`);
      return;
    }

    // Update device online status
    let deviceUpdated = false;

    for (const space of user.spaces) {
      const deviceIndex = space.devices.findIndex(
        (d) => d.device_id === deviceId
      );

      if (deviceIndex !== -1) {
        const device = space.devices[deviceIndex];

        // Update online status
        if (!device.online_status) {
          device.online_status = true;
          device.last_updated = new Date();

          // Extract firmware version if available
          if (message.firmware || message.firmware_version) {
            device.firmware_version = message.firmware || message.firmware_version;
          }

          deviceUpdated = true;

          logger.info(`Device ${deviceId} is now online`);

          // Create notification for device coming online
          await createNotification({
            type: "DEVICE_ONLINE",
            title: "Device Online",
            message: `${device.device_name} is now online`,
            user_id: user._id,
            data: {
              device_id: deviceId,
              device_name: device.device_name,
              device_type: device.device_type,
              space_name: space.space_name,
              space_id: space._id,
            },
          });
        }

        // Save if device was updated
        if (deviceUpdated) {
          await user.save();
        }

        break;
      }
    }
  } catch (error) {
    logger.error("Error handling alive message:", error);
  }
}

// 🔥 FIXED: Handle slave response messages and REQUEST STATUS UPDATE
export async function handleSlaveResponseMessage(topic, message) {
  try {
    const baseDeviceId = message.deviceid;
    const slaveId = message.slaveid;
    const sensorNo = message.sensor_no;

    if (!baseDeviceId || !slaveId || !sensorNo) {
      logger.error("Slave response missing required fields:", message);
      return;
    }

    logger.info(`🔵 Received slave response for base ${baseDeviceId} and tank ${slaveId}`);

    // Find the user with this base device
    const user = await User.findOne({
      "spaces.devices.device_id": baseDeviceId,
    });

    if (!user) {
      logger.error(`No user found with base device ${baseDeviceId}`);
      return;
    }

    // Find the base device to get thingid
    let thingId = null;
    let baseDevice = null;
    
    for (const space of user.spaces) {
      const device = space.devices.find(d => d.device_id === baseDeviceId);
      if (device) {
        baseDevice = device;
        thingId = device.thing_name || device.thingid;
        break;
      }
    }

    if (!thingId) {
      logger.error(`No thingId found for base device ${baseDeviceId}`);
      return;
    }

    // ✅ 1. Save the response to device_responses collection
    try {
      await saveDeviceResponse(
        thingId,
        baseDeviceId,
        'slave_response',
        message
      );
      logger.info(`✅ Slave response saved to device_responses collection`);
    } catch (saveError) {
      logger.error(`❌ Error saving to device_responses: ${saveError.message}`);
    }

    // ✅ 2. Save to tank_readings collection (for tracking tank connection)
    try {
      await saveMqttDataToMongo({
        deviceid: slaveId, // Use the tank/slave device ID
        sensor_no: sensorNo,
        message_type: 'slave_response',
        timestamp: message.timestamp || new Date(),
        thingid: thingId,
        channel: message.channel,
        address_l: message.address_l,
        address_h: message.address_h,
        raw_data: message
      });
      logger.info(`✅ Slave response saved to tank_readings for tank ${slaveId}`);
    } catch (saveError) {
      logger.error(`❌ Error saving slave response to tank_readings: ${saveError.message}`);
    }

    // ✅ 3. Update tank device in MongoDB user document
    for (const space of user.spaces) {
      const tankDevice = space.devices.find(d => d.device_id === slaveId);
      
      if (tankDevice) {
        // Update tank connection info
        tankDevice.channel = message.channel || tankDevice.channel;
        tankDevice.address_l = message.address_l || tankDevice.address_l;
        tankDevice.address_h = message.address_h || tankDevice.address_h;
        tankDevice.last_updated = new Date();
        
        await user.save();
        logger.info(`✅ Tank device ${slaveId} connection info updated in user document`);
        
        // Create notification
        await createNotification({
          type: "TANK_CONNECTED",
          title: "Tank Device Connected",
          message: `Tank ${tankDevice.device_name} successfully connected to base`,
          user_id: user._id,
          data: {
            device_id: slaveId,
            device_name: tankDevice.device_name,
            base_device_id: baseDeviceId,
            sensor_no: sensorNo,
            space_name: space.space_name,
            space_id: space._id,
          },
        });
        
        break;
      }
    }

    // 🔥 4. REQUEST STATUS UPDATE FROM THE TANK DEVICE
    // This triggers the device to publish actual sensor readings to update topic
    logger.info(`🚀 Requesting status update for tank ${slaveId} via sensor ${sensorNo}`);
    
    try {
      const statusRequestTopic = `mqtt/device/${thingId}/status_request`;
      const statusRequestPayload = {
        deviceid: baseDeviceId,
        sensor_no: sensorNo,  // Request specific sensor data
        requested_at: new Date().toISOString()
      };
      
      // Publish status request
      publish(statusRequestTopic, statusRequestPayload);
      
      logger.info(`✅ Status request sent to ${statusRequestTopic}`);
      logger.info(`⏳ Waiting for update topic with actual sensor readings...`);
      
    } catch (publishError) {
      logger.error(`❌ Error publishing status request: ${publishError.message}`);
    }

  } catch (error) {
    logger.error("Error handling slave response message:", error);
  }
}

// Handle device health messages
export async function handleHealthMessage(topic, message) {
  try {
    // Extract deviceId from message
    const deviceId = message.deviceid;

    if (!deviceId) {
      logger.error("Health message missing deviceid:", message);
      return;
    }

    // Log health message
    logger.info(`Received health message from device ${deviceId}`);

    // ✅ Save to tank_readings collection
    try {
      await saveMqttDataToMongo({
        deviceid: deviceId,
        message_type: 'health_reply',
        timestamp: message.timestamp || new Date(),
        thingid: message.thingId,
        raw_data: message
      });
      logger.info(`✅ Health message saved to tank_readings for device ${deviceId}`);
    } catch (saveError) {
      logger.error(`❌ Error saving health message: ${saveError.message}`);
    }

  } catch (error) {
    logger.error("Error handling health message:", error);
  }
}

// Handle offline detection (if device hasn't sent alive message in a while)
export async function handleDeviceOffline(deviceId) {
  try {
    // Find the device in the database
    const user = await User.findOne({ "spaces.devices.device_id": deviceId });

    if (!user) {
      logger.error(`No user found with device ${deviceId}`);
      return;
    }

    // Update device online status
    let deviceUpdated = false;

    for (const space of user.spaces) {
      const deviceIndex = space.devices.findIndex(
        (d) => d.device_id === deviceId
      );

      if (deviceIndex !== -1) {
        const device = space.devices[deviceIndex];

        // Update online status
        if (device.online_status) {
          device.online_status = false;
          device.last_updated = new Date();
          deviceUpdated = true;

          logger.info(`Device ${deviceId} is now offline`);

          // Create notification for device going offline
          await createNotification({
            type: "DEVICE_OFFLINE",
            title: "Device Offline",
            message: `${device.device_name} is offline`,
            user_id: user._id,
            data: {
              device_id: deviceId,
              device_name: device.device_name,
              device_type: device.device_type,
              space_name: space.space_name,
              space_id: space._id,
            },
          });
        }

        // Save if device was updated
        if (deviceUpdated) {
          await user.save();
        }

        break;
      }
    }
  } catch (error) {
    logger.error("Error handling device offline:", error);
  }
}