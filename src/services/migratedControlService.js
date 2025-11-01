/**
 * Control Service Helper - MongoDB Only - FIXED VERSION
 * 
 * This service provides helper functions to query MongoDB for device control operations
 */

import mongoose from "mongoose";
import logger from "../utils/logger.js";

const db = () => mongoose.connection.db;

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
 * 🔥 NEW: Debug function to check slave_requests collection
 */
export async function debugSlaveRequests(thingid, deviceid) {
  const database = db();
  const coll = database.collection("slave_requests");
  
  try {
    const since = new Date(Date.now() - 60000); // Last 1 minute
    
    // Get all recent requests for this thing/device
    const allRecent = await coll.find({
      $or: [
        { thingid: thingid },
        { deviceid: deviceid }
      ],
      requested_at: { $gte: since }
    }).sort({ requested_at: -1 }).limit(10).toArray();
    
    // Count pending and completed
    const pendingCount = await coll.countDocuments({
      thingid: thingid,
      status: "pending",
      requested_at: { $gte: since }
    });
    
    const completedCount = await coll.countDocuments({
      thingid: thingid,
      status: "completed",
      requested_at: { $gte: since }
    });
    
    return {
      pending_count: pendingCount,
      completed_count: completedCount,
      recent_requests: allRecent.map(doc => ({
        _id: doc._id.toString(),
        deviceid: doc.deviceid,
        thingid: doc.thingid,
        sensor_no: doc.sensor_no,
        status: doc.status,
        requested_at: doc.requested_at,
        completed_at: doc.completed_at,
        slaveid: doc.slaveid
      }))
    };
  } catch (err) {
    logger.error(`Error in debugSlaveRequests: ${err.message}`);
    return {
      error: err.message,
      pending_count: 0,
      completed_count: 0,
      recent_requests: []
    };
  }
}

/**
 * 🔥 FIXED: Wait for slave response for a given thingid
 */
export async function waitForSlaveResponseFromMongoDB(thingid, timeoutMs = 15000) {
  const database = db();
  const coll = database.collection("slave_requests");
  const interval = 500; // Poll every 500ms
  const start = Date.now();
  let pollCount = 0;

  logger.info(`⏳ Polling for slave response. ThingID: ${thingid}, Timeout: ${timeoutMs}ms`);

  while (Date.now() - start < timeoutMs) {
    pollCount++;
    const elapsed = Date.now() - start;
    
    // Look back 30 seconds for the request
    const since = new Date(Date.now() - 30000);
    
    try {
      // Query for completed requests
      const query = {
        thingid: thingid,
        requested_at: { $gte: since },
        status: "completed"
      };
      
      logger.info(`🔍 Poll #${pollCount} (${elapsed}ms elapsed) - Query:`, JSON.stringify(query));
      
      const doc = await coll.findOne(
        query,
        { sort: { completed_at: -1, requested_at: -1 } }
      );

      if (doc) {
        logger.info(`✅ Slave response found on poll #${pollCount} after ${elapsed}ms:`, {
          _id: doc._id.toString(),
          deviceid: doc.deviceid,
          thingid: doc.thingid,
          status: doc.status,
          completed_at: doc.completed_at,
          requested_at: doc.requested_at
        });
        
        // Return the document with response data
        return {
          ...doc,
          _id: doc._id.toString(),
          response_data: doc.response || doc.response_data || {}
        };
      }
      
      // Log every 5th poll to avoid spam
      if (pollCount % 5 === 0) {
        logger.info(`⏳ Still waiting... Poll #${pollCount}, ${elapsed}ms elapsed`);
      }
      
    } catch (err) {
      logger.error(`❌ Error polling slave response on attempt #${pollCount}:`, err.message);
    }

    // Wait before next poll
    await new Promise((resolve) => setTimeout(resolve, interval));
  }

  logger.warn(`⏰ Timeout waiting for slave response after ${pollCount} polls (${timeoutMs}ms). ThingID: ${thingid}`);
  
  // Final debug: check what's in the database
  try {
    const finalCheck = await coll.find({
      thingid: thingid,
      requested_at: { $gte: new Date(Date.now() - 30000) }
    }).toArray();
    
    logger.warn(`📊 Final check - Found ${finalCheck.length} requests:`, 
      finalCheck.map(d => ({
        status: d.status,
        requested_at: d.requested_at,
        completed_at: d.completed_at
      }))
    );
  } catch (err) {
    logger.error(`Error in final check: ${err.message}`);
  }
  
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

  logger.info(`⏳ Checking tank response for device: ${deviceid}, sensor: ${sensorNo}`);

  while (Date.now() - start < timeoutMs) {
    const since = new Date(Date.now() - 15000); // 15 seconds lookback
    
    try {
      const doc = await coll.findOne(
        {
          deviceid: deviceid,
          sensor_no: sensorNo,
          message_type: "update",
          timestamp: { $gte: since }
        },
        { 
          sort: { timestamp: -1 },
          projection: { _id: 1, deviceid: 1, sensor_no: 1, level: 1, timestamp: 1 }
        }
      );

      if (doc) {
        logger.info(`✅ Tank device ${deviceid} (${sensorNo}) responded with data:`, {
          level: doc.level,
          timestamp: doc.timestamp
        });
        return true;
      }
    } catch (err) {
      logger.error(`❌ Error checking tank response: ${err.message}`);
      logger.error(err.stack);
    }

    await new Promise((resolve) => setTimeout(resolve, pollInterval));
  }

  logger.warn(`⏰ Tank device ${deviceid} (${sensorNo}) did not respond within ${timeoutMs}ms`);
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
  getRecentDeviceResponses,
  debugSlaveRequests
};