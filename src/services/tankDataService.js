// src/services/tankDataService.js
import { getLatestSensorData, getLatestSwitchStatus, TankReading } from './migratedDataService.js';
import { publishToIoT } from '../utils/mqttHelper.js';
import { getTopic } from '../config/awsIotConfig.js';
import { User } from '../config/dbconfig.js';
import logger from '../utils/logger.js';

/**
 * Fetch latest tank sensor data from MongoDB
 * ✅ UPDATED: Now also publishes update request to trigger fresh data
 */
export async function sensorData(req, res) {
  const { deviceid, sensorNumber } = req.params;

  if (!deviceid || !sensorNumber) {
    return res.status(400).json({
      success: false,
      message: 'deviceId and sensorNumber are required.',
    });
  }

  try {
    // 🔥 NEW: Find the device and publish update request
    const user = await User.findOne({
      "spaces.devices.device_id": deviceid
    });

    if (user) {
      for (const space of user.spaces) {
        const device = space.devices.find(d => d.device_id === deviceid);
        
        if (device) {
          // Found the device, now publish update request
          if (device.device_type === "tank") {
            // For tank devices, find the parent base device
            const parentDevice = space.devices.find(d => 
              d.device_id === device.parent_device_id && 
              d.switch_no === device.parent_switch_no
            );
            
            if (parentDevice?.thing_name) {
              try {
                const updateTopic = getTopic("update", parentDevice.thing_name, "update");
                const updateMsg = {
                  deviceid: device.parent_device_id,
                  device: "tank",
                  sensor_no: sensorNumber, // Use the sensorNumber from params
                  slaveid: deviceid,
                  switch_no: device.parent_switch_no,
                  request_type: "poll" // Indicate this is a polling request
                };
                
                await publishToIoT(updateTopic, updateMsg);
                logger.info(`📤 Published update request for tank ${deviceid} via base ${parentDevice.device_id}`, updateMsg);
              } catch (publishError) {
                logger.error(`Error publishing update request: ${publishError.message}`);
                // Continue to fetch data even if publish fails
              }
            }
          } else if (device.device_type === "base" && device.thing_name) {
            // For base devices
            try {
              const updateTopic = getTopic("update", device.thing_name, "update");
              const updateMsg = {
                deviceid: deviceid,
                device: "base",
                switch_no: sensorNumber, // For base, sensorNumber is switch_no
                status: device.status || "off",
                sensor_no: sensorNumber,
                value: device.status === "on" ? "1" : "0",
                request_type: "poll"
              };
              
              await publishToIoT(updateTopic, updateMsg);
              logger.info(`📤 Published update request for base ${deviceid}`, updateMsg);
            } catch (publishError) {
              logger.error(`Error publishing update request: ${publishError.message}`);
            }
          }
          break; // Found device, no need to continue
        }
      }
    }

    // Fetch latest reading from MongoDB
    const mongoResult = await getLatestSensorData(deviceid, sensorNumber);

    if (!mongoResult) {
      return res.status(404).json({
        success: false,
        message: 'No data found for the given sensor.',
      });
    }

    const formattedResult = {
      deviceid: mongoResult.deviceid,
      sensor_no: mongoResult.sensor_no,
      switch_no: mongoResult.switch_no,
      level: mongoResult.level,
      value: mongoResult.value || mongoResult.level,
      status: mongoResult.status,
      message_type: mongoResult.message_type,
      timestamp: mongoResult.timestamp,
      thingid: mongoResult.thingid,
    };

    res.json({ success: true, data: [formattedResult] });
  } catch (error) {
    logger.error('Error fetching latest sensor data:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      details: error.message,
    });
  }
}

/**
 * Fetch latest switch status from MongoDB
 * ✅ UPDATED: Now also publishes update request to trigger fresh data
 */
export async function switchStatus(req, res) {
  const { deviceid, switchNumber } = req.params;

  if (!deviceid || !switchNumber) {
    return res.status(400).json({
      success: false,
      message: 'deviceId and switchNumber are required.',
    });
  }

  try {
    // 🔥 NEW: Find the device and publish update request
    const user = await User.findOne({
      "spaces.devices.device_id": deviceid
    });

    if (user) {
      for (const space of user.spaces) {
        const device = space.devices.find(d => 
          d.device_id === deviceid && 
          d.switch_no === switchNumber
        );
        
        if (device && device.thing_name) {
          try {
            const updateTopic = getTopic("update", device.thing_name, "update");
            const updateMsg = {
              deviceid: deviceid,
              device: "base",
              switch_no: switchNumber,
              status: device.status || "off",
              sensor_no: switchNumber,
              value: device.status === "on" ? "1" : "0",
              request_type: "poll"
            };
            
            await publishToIoT(updateTopic, updateMsg);
            logger.info(`📤 Published update request for switch ${deviceid}/${switchNumber}`, updateMsg);
          } catch (publishError) {
            logger.error(`Error publishing update request: ${publishError.message}`);
          }
          break;
        }
      }
    }

    // Fetch latest switch status from MongoDB
    const mongoResult = await getLatestSwitchStatus(deviceid, switchNumber);

    if (!mongoResult) {
      return res.status(404).json({
        success: false,
        message: 'No data found for the given switch.',
      });
    }

    const formattedResult = {
      deviceid: mongoResult.deviceid,
      sensor_no: mongoResult.sensor_no,
      switch_no: mongoResult.switch_no,
      value: mongoResult.value || mongoResult.level,
      status: mongoResult.status === 'true' ? 'on' : 'off',
      timestamp: mongoResult.timestamp,
      thingid: mongoResult.thingid,
    };

    res.json({ success: true, data: [formattedResult] });
  } catch (error) {
    logger.error('Error fetching latest switch data:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      details: error.message,
    });
  }
}

/**
 * Handle IoT message forwarding through Lambda (optional helper)
 * Used if you want to notify IoT Core about changes or requests
 */
export async function forwardToIoT(deviceid, messageType, payload) {
  try {
    const topic = getTopic(messageType, deviceid, messageType);
    await publishToIoT(topic, payload);

    logger.info(`✅ Forwarded ${messageType} message to IoT Core via Lambda for ${deviceid}`);
  } catch (err) {
    logger.error(`❌ Error forwarding message to IoT for ${deviceid}: ${err.message}`);
  }
}

/**
 * Handle incoming MQTT data (used by AWS IoT → Lambda → MongoDB path)
 */
export async function handleMqttIncomingData(message) {
  try {
    const { deviceid, sensor_no, switch_no, value, status, timestamp, thingId } = message;

    if (!deviceid) {
      logger.warn('Received MQTT message without deviceid');
      return;
    }

    const now = timestamp ? new Date(timestamp) : new Date();

    const tankReading = new TankReading({
      deviceid,
      sensor_no,
      switch_no,
      level: value,
      value,
      status,
      message_type: 'mqtt_update',
      timestamp: now,
      thingid: thingId || message.thingid,
      raw_data: message,
    });

    await tankReading.save();
    logger.info(`✅ MQTT data saved to MongoDB for device ${deviceid}`);
  } catch (err) {
    logger.error(`❌ Error handling MQTT data: ${err.message}`);
  }
}

/**
 * Get historical sensor data for a device
 */
export async function getHistoricalData(req, res) {
  const { deviceid, sensorNumber } = req.params;
  const { startDate, endDate, limit = 1000 } = req.query;

  if (!deviceid) {
    return res.status(400).json({
      success: false,
      message: 'deviceId is required.',
    });
  }

  try {
    const query = { deviceid };

    if (sensorNumber) query.sensor_no = sensorNumber;
    if (startDate || endDate) {
      query.timestamp = {};
      if (startDate) query.timestamp.$gte = new Date(startDate);
      if (endDate) query.timestamp.$lte = new Date(endDate);
    }

    const readings = await TankReading.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .lean();

    res.json({
      success: true,
      data: readings,
      count: readings.length,
    });
  } catch (error) {
    logger.error('Error fetching historical data:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      details: error.message,
    });
  }
}

export default {
  sensorData,
  switchStatus,
  handleMqttIncomingData,
  getHistoricalData,
  forwardToIoT,
};