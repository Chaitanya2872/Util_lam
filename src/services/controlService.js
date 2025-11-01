// src/services/controlService.js
import { publishToIoT, subscribe } from "../utils/mqttHelper.js";
import {
  getThingIdByDeviceId,
  waitForSlaveResponseFromMongoDB,
  checkBaseRespondedInMongo,
  checkTankRespondedInMongo,
} from "../services/migratedControlService.js";
import logger from "../utils/logger.js";
import { getTopic } from "../config/awsIotConfig.js";

/**
 * Control device via AWS Lambda (IAM-secured)
 */
export async function control(req, res) {
  logger.info("🚀 CONTROL endpoint called", {
    user: req.user?.mobile_number,
    deviceid: req.body?.deviceid,
  });

  try {
    const { deviceid } = req.body;
    if (!deviceid) {
      return res.status(400).json({ success: false, error: "Missing deviceid in request" });
    }

    const thingid = await getThingIdByDeviceId(deviceid);
    if (!thingid) {
      return res.status(404).json({ success: false, error: "DeviceId not found or no associated thing ID" });
    }

    const topic = getTopic("control", thingid, "control");
    const payload = {
      ...req.body,
      requestedBy: req.user?.mobile_number,
      timestamp: new Date().toISOString(),
    };

    await publishToIoT(topic, payload);

    logger.info("✅ Control command published via Lambda", { deviceid, thingid });
    res.status(200).json({
      success: true,
      message: "Control command published successfully",
      topic,
    });
  } catch (error) {
    logger.error("❌ Control publish error:", error);
    res.status(500).json({ success: false, error: error.message });
  }
}

/**
 * Send device settings via AWS Lambda
 */
export async function setting(deviceid, payload) {
  try {
    const thingid = await getThingIdByDeviceId(deviceid);
    if (!thingid) throw new Error(`No thingid found for deviceid: ${deviceid}`);

    const topic = getTopic("setting", thingid, "setting");
    await publishToIoT(topic, payload);

    logger.info(`✅ Settings published via Lambda for thing: ${thingid}`);
    return { success: true, topic };
  } catch (error) {
    logger.error("❌ Settings publish error:", error);
    throw new Error(`MQTT Publish Failed: ${error.message}`);
  }
}

/**
 * Send slave request and wait for response via MongoDB
 */
// Replace the slaveRequest function in controlService.js

export async function slaveRequest(req, res) {
  logger.info("🚀 SLAVE REQUEST endpoint called", {
    user: req.user?.mobile_number,
    deviceid: req.body?.deviceid,
  });

  try {
    const { deviceid, sensor_no, mode, channel, address_l, address_h, range, capacity, slaveid } = req.body;
    
    if (!deviceid || !sensor_no) {
      return res.status(400).json({ 
        success: false, 
        error: "Missing deviceid or sensor_no" 
      });
    }

    const thingid = await getThingIdByDeviceId(deviceid);
    if (!thingid) {
      return res.status(404).json({ 
        success: false, 
        error: "DeviceId not found or no associated thing ID" 
      });
    }

    const topic = getTopic("slaveRequest", thingid, "slaveRequest");
    
    // Build payload exactly as device expects
    const payload = {
      deviceid,
      sensor_no,
      mode: parseInt(mode) || 3,
      channel: parseInt(channel),
      address_l,
      address_h,
      range: parseInt(range),
      capacity: parseInt(capacity)
    };

    // Add optional slaveid
    if (slaveid) {
      payload.slaveid = slaveid;
    }

    // Publish via Lambda
    await publishToIoT(topic, payload);
    
    logger.info("✅ Slave request published via Lambda");

    // Wait for response from MongoDB
    const response = await waitForSlaveResponseFromMongoDB(thingid, 10000);

    if (response) {
      return res.status(200).json({
        success: true,
        message: "Published and response received",
        data: {
          status: response.response_data?.status || "success",
          deviceid: response.deviceid,
          thingid: response.thingid,
          channel: response.response_data?.channel 
            ? parseInt(response.response_data.channel) 
            : parseInt(channel),
          address_l: response.response_data?.address_l || address_l,
          address_h: response.response_data?.address_h || address_h,
          sensor_no: response.response_data?.sensor_no || sensor_no,
          slaveid: response.response_data?.slaveid,
          timestamp: response.inserted_at
        }
      });
    } else {
      return res.status(200).json({
        success: true,
        message: "Published but no response received",
        data: null
      });
    }
  } catch (error) {
    logger.error("❌ Slave request error:", error);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * Check if base device responded
 */
export async function isBaseResponded(req, res) {
  const { deviceid } = req.params;
  if (!deviceid) {
    return res.status(400).json({ success: false, message: "Device ID is required." });
  }

  try {
    logger.info(`Checking base response for device: ${deviceid}`);
    const responded = await checkBaseRespondedInMongo(deviceid, 5000);
    return res.status(responded ? 200 : 404).json({
      success: responded,
      message: responded ? "Base responded successfully." : "Base did not respond.",
    });
  } catch (error) {
    logger.error("Error checking base response:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error.",
      details: error.message,
    });
  }
}

/**
 * Check if tank device responded
 */
export async function isTankResponded(req, res) {
  const { deviceid, sensorNumber } = req.params;
  if (!deviceid || !sensorNumber) {
    return res.status(400).json({
      success: false,
      message: "Device ID and Sensor Number are required.",
    });
  }

  try {
    logger.info(`Checking tank response for device: ${deviceid}, sensor: ${sensorNumber}`);
    const responded = await checkTankRespondedInMongo(deviceid, sensorNumber, 10000);
    return res.status(responded ? 200 : 404).json({
      success: responded,
      message: responded ? "Tank responded successfully." : "Tank did not respond.",
    });
  } catch (error) {
    logger.error("Error checking tank response:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error.",
      details: error.message,
    });
  }
}

export default {
  control,
  setting,
  slaveRequest,
  isBaseResponded,
  isTankResponded,
};
