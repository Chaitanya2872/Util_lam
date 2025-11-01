/**
 * Control Service Helper - MongoDB Only
 * 
 * This service provides helper functions to query MongoDB for device control operations
 */

import mongoose from "mongoose";
import logger from "../utils/logger.js";

const db = () => mongoose.connection.db;

/**
 * Get thingid by deviceid from MongoDB
 * Checks in sensor_metadata collection first, then falls back to users.spaces.devices
 */
/**
 * Get thingid by deviceid from MongoDB
 * Handles both base and tank devices
 */
export async function getThingIdByDeviceId(deviceid) {
  if (!deviceid) {
    logger.warn('getThingIdByDeviceId called without deviceid');
    return null;
  }

  const database = db();

  // 1) Try sensor_metadata collection
  try {
    const sensorColl = database.collection("sensor_metadata");
    const row = await sensorColl.findOne({ deviceid });
    if (row && (row.thingid || row.thingId || row.thing_id)) {
      logger.info(`✅ Found thingid in sensor_metadata: ${row.thingid || row.thingId || row.thing_id}`);
      return row.thingid || row.thingId || row.thing_id;
    }
  } catch (err) {
    logger.error(`Error querying sensor_metadata: ${err.message}`);
  }

  // 2) Search users -> spaces -> devices
  try {
    const usersColl = database.collection("users");
    const userDoc = await usersColl.findOne(
      { "spaces.devices.device_id": deviceid },
      { projection: { spaces: 1 } }
    );

    if (userDoc && userDoc.spaces) {
      for (const space of userDoc.spaces) {
        if (!space.devices) continue;
        
        for (const device of space.devices) {
          if (device.device_id === deviceid) {
            // If it's a base device, return its thing_name
            if (device.device_type === "base") {
              const thingName = device.thing_name || device.thingid || device.thingId || device.thing_id;
              if (thingName) {
                logger.info(`✅ Found thingid for base device: ${thingName}`);
                return thingName;
              }
            }
            
            // If it's a tank device, find its parent base device
            if (device.device_type === "tank") {
              const parentDeviceId = device.parent_device_id;
              if (!parentDeviceId) {
                logger.warn(`Tank device ${deviceid} has no parent_device_id`);
                return null;
              }
              
              // Find the parent base device in the same space
              const baseDevice = space.devices.find(
                (d) => d.device_id === parentDeviceId && d.device_type === "base"
              );
              
              if (baseDevice) {
                const thingName = baseDevice.thing_name || baseDevice.thingid || baseDevice.thingId || baseDevice.thing_id;
                if (thingName) {
                  logger.info(`✅ Found thingid from parent base device: ${thingName}`);
                  return thingName;
                }
              } else {
                logger.warn(`Parent base device ${parentDeviceId} not found for tank ${deviceid}`);
              }
            }
            
            // Fallback for other device types
            const thingName = device.thing_name || device.thingid || device.thingId || device.thing_id;
            if (thingName) {
              logger.info(`✅ Found thingid: ${thingName}`);
              return thingName;
            }
          }
        }
      }
    }
  } catch (err) {
    logger.error(`Error querying users collection: ${err.message}`);
  }

  logger.warn(`❌ No thingid found for deviceid: ${deviceid}`);
  return null;
}

/**
 * Poll the device_responses collection in MongoDB for a matching thingid
 * Returns the document or null on timeout
 */
/**
 * Wait for slave response for a given thingid.
 * Optimized for Lambda's latest insert format.
 */
// Replace waitForSlaveResponseFromMongoDB in migratedControlService.js

export async function waitForSlaveResponseFromMongoDB(thingid, timeoutMs = 10000) {
  const database = db();
  const coll = database.collection("device_responses");
  const interval = 500; // Poll every 500ms
  const start = Date.now();

  logger.info(`⏳ Polling for slave response. ThingID: ${thingid}`);

  while (Date.now() - start < timeoutMs) {
    const since = new Date(Date.now() - 15000); // Look for responses in last 15 seconds

    try {
      const doc = await coll.findOne(
        {
          thingid,
          inserted_at: { $gte: since },
          response_type: 'slave_response'
        },
        { sort: { inserted_at: -1 } }
      );

      if (doc) {
        logger.info(`✅ Slave response found for thingid: ${thingid}`, doc);
        return doc;
      }
    } catch (err) {
      logger.error(`Error polling slave response: ${err.message}`);
    }

    // Wait before next poll
    await new Promise((resolve) => setTimeout(resolve, interval));
  }

  logger.warn(`⏰ Timeout waiting for slave response. ThingID: ${thingid}`);
  return null;
}


/**
 * Check if base device responded by looking in tank_readings collection for alive_reply
 */
export async function checkBaseRespondedInMongo(deviceid, timeoutMs = 5000) {
  const database = db();
  const coll = database.collection("tank_readings");
  const pollInterval = 1000;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const since = new Date(Date.now() - 10000);
    
    try {
      const doc = await coll.findOne(
        {
          deviceid,
          message_type: "alive_reply",
          timestamp: { $gte: since }
        },
        { sort: { timestamp: -1 } }
      );

      if (doc) {
        logger.info(`✅ Base device ${deviceid} responded`);
        return true;
      }
    } catch (err) {
      logger.error(`Error checking base response: ${err.message}`);
    }

    await new Promise((resolve) => setTimeout(resolve, pollInterval));
  }

  logger.warn(`Base device ${deviceid} did not respond within ${timeoutMs}ms`);
  return false;
}

/**
 * Check if tank device responded by looking in tank_readings collection for update of sensor_no
 */
export async function checkTankRespondedInMongo(deviceid, sensorNo, timeoutMs = 10000) {
  const database = db();
  const coll = database.collection("tank_readings");
  const pollInterval = 1000;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const since = new Date(Date.now() - 10000);
    
    try {
      const doc = await coll.findOne(
        {
          deviceid,
          sensor_no: sensorNo,
          message_type: "update",
          timestamp: { $gte: since }
        },
        { sort: { timestamp: -1 } }
      );

      if (doc) {
        logger.info(`✅ Tank device ${deviceid} (${sensorNo}) responded`);
        return true;
      }
    } catch (err) {
      logger.error(`Error checking tank response: ${err.message}`);
    }

    await new Promise((resolve) => setTimeout(resolve, pollInterval));
  }

  logger.warn(`Tank device ${deviceid} (${sensorNo}) did not respond within ${timeoutMs}ms`);
  return false;
}

/**
 * Sanitize MongoDB document by converting _id to string
 */
function sanitizeMongoDoc(doc) {
  if (!doc) return doc;
  const sanitized = { ...doc };
  if (sanitized._id) sanitized._id = sanitized._id.toString();
  return sanitized;
}

/**
 * Get recent device responses from MongoDB
 */
export async function getRecentDeviceResponses(thingid, seconds = 10) {
  const database = db();
  const coll = database.collection("device_responses");
  const cutoffTime = new Date(Date.now() - (seconds * 1000));

  try {
    const results = await coll.find({
      thingid,
      inserted_at: { $gte: cutoffTime }
    })
    .sort({ inserted_at: -1 })
    .toArray();

    return results.map(sanitizeMongoDoc);
  } catch (err) {
    logger.error(`Error getting recent device responses: ${err.message}`);
    return [];
  }
}

export default {
  getThingIdByDeviceId,
  waitForSlaveResponseFromMongoDB,
  checkBaseRespondedInMongo,
  checkTankRespondedInMongo,
  getRecentDeviceResponses
};