// src/iot/index.js
import { getMqttClient, subscribe } from "../utils/mqttHelper.js";
import { getTopic, TOPIC_SUFFIXES } from "../config/awsIotConfig.js";
import {
  handleUpdateMessage,
  handleAliveMessage,
  handleSlaveResponseMessage,
  handleHealthMessage,
  handleDeviceOffline,
} from "./topicHandlers.js";
import logger from "../utils/logger.js";
import { User } from "../config/dbconfig.js";

// Keep track of active device heartbeats
const deviceHeartbeats = new Map();
const OFFLINE_TIMEOUT = 300000; // 5 minutes without heartbeat = offline

// Check for offline devices periodically
setInterval(() => {
  const now = Date.now();

  deviceHeartbeats.forEach((lastSeen, deviceId) => {
    if (now - lastSeen > OFFLINE_TIMEOUT) {
      // Mark device as offline
      handleDeviceOffline(deviceId);
      // Remove from tracking map
      deviceHeartbeats.delete(deviceId);
    }
  });
}, 60000); // Check every minute

// Initialize IoT service and subscribe to relevant topics
export async function initializeIotService() {
  try {
    // Connect to MQTT
    const mqttClient = getMqttClient();

    // Get all device thing_names from database
    const users = await User.find({});
    const thingNames = new Set();

    // Collect all device thing_names from all users
    users.forEach((user) => {
      user.spaces.forEach((space) => {
        space.devices.forEach((device) => {
          if (device.thing_name) {
            thingNames.add(device.thing_name);
          }
        });
      });
    });

    logger.info(`Found ${thingNames.size} devices with thing_names`);

    // Subscribe to topics for each device
    thingNames.forEach((thingName) => {
      // Subscribe to update topic
      const updateTopic = getTopic("update", thingName, "update");
      subscribe(updateTopic);

      // Subscribe to alive topic
      const aliveTopic = getTopic("alive", thingName, "alive");
      subscribe(aliveTopic);

      // Subscribe to health topic
      const healthTopic = getTopic("health", thingName, "health");
      subscribe(healthTopic);

      // ✅ Subscribe to slave_response - FIXED to use correct $aws/things/ prefix
      const slaveResponseTopic = getTopic("slaveResponse", thingName, "slaveResponse");
      subscribe(slaveResponseTopic);
      
      logger.info(`Subscribed to topics for device ${thingName}`);
    });

    // Set up message handler
    mqttClient.on("message", (topic, payload) => {
      const message = payload.toString();

      try {
        const data = JSON.parse(message);

        // Update device heartbeat if deviceid is present
        if (data.deviceid) {
          deviceHeartbeats.set(data.deviceid, Date.now());
        }

        // Route message to appropriate handler based on topic
        if (topic.endsWith(TOPIC_SUFFIXES.update)) {
          handleUpdateMessage(topic, data);
        } else if (topic.endsWith(TOPIC_SUFFIXES.alive)) {
          handleAliveMessage(topic, data);
        } else if (topic.endsWith(TOPIC_SUFFIXES.health)) {
          handleHealthMessage(topic, data);
        } else if (topic.endsWith(TOPIC_SUFFIXES.slaveResponse)) {
          // ✅ FIXED: Now properly detects $aws/things/{thingId}/slave_response
          handleSlaveResponseMessage(topic, data);
        } else {
          logger.info(`Received message on unhandled topic ${topic}`);
        }
      } catch (error) {
        logger.error(
          `Error processing message on topic ${topic}: ${error.message}`
        );
      }
    });

    logger.info("IoT service initialized successfully");
    return true;
  } catch (error) {
    logger.error(`Error initializing IoT service: ${error.message}`);
    return false;
  }
}

// Subscribe to topics for a newly added device
export function subscribeToDeviceTopics(thingName) {
  try {
    if (!thingName) {
      logger.error("No thing_name provided for subscription");
      return false;
    }

    // Subscribe to all relevant topics for this device
    const updateTopic = getTopic("update", thingName, "update");
    const aliveTopic = getTopic("alive", thingName, "alive");
    const healthTopic = getTopic("health", thingName, "health");
    
    // ✅ FIXED: Use correct $aws/things/ prefix for slave_response
    const slaveResponseTopic = getTopic("slaveResponse", thingName, "slaveResponse");

    subscribe(updateTopic);
    subscribe(aliveTopic);
    subscribe(healthTopic);
    subscribe(slaveResponseTopic);

    logger.info(`Subscribed to topics for device ${thingName}`);
    return true;
  } catch (error) {
    logger.error(`Error subscribing to device topics: ${error.message}`);
    return false;
  }
}