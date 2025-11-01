/**
 * Migrated Data Service - MongoDB Only
 * 
 * Provides methods to query device data from MongoDB collections
 */

import mongoose from 'mongoose';
import logger from '../utils/logger.js';

// MongoDB Schemas for device data
const tankReadingSchema = new mongoose.Schema({
  deviceid: { type: String, required: true, index: true },
  sensor_no: { type: String, index: true },
  switch_no: { type: String, index: true },
  level: { type: Number },
  value: { type: Number },
  status: { type: String },
  message_type: { type: String, index: true },
  timestamp: { type: Date, required: true, index: true },
  thingid: { type: String, index: true },
  raw_data: { type: mongoose.Schema.Types.Mixed },
  migrated_at: { type: Date, default: Date.now }
}, {
  timestamps: true,
  collection: 'tank_readings'
});

const sensorMetadataSchema = new mongoose.Schema({
  deviceid: { type: String, required: true, unique: true, index: true },
  thingid: { type: String, required: true, index: true },
  device_type: { type: String },
  connection_info: { type: mongoose.Schema.Types.Mixed },
  first_seen: { type: Date },
  last_seen: { type: Date },
  migrated_at: { type: Date, default: Date.now }
}, {
  timestamps: true,
  collection: 'sensor_metadata'
});

const deviceResponseSchema = new mongoose.Schema({
  thingid: { type: String, required: true, index: true },
  deviceid: { type: String, index: true },
  response_type: { type: String, index: true },
  response_data: { type: mongoose.Schema.Types.Mixed },
  inserted_at: { type: Date, required: true, index: true },
  migrated_at: { type: Date, default: Date.now }
}, {
  timestamps: true,
  collection: 'device_responses'
});

// Create models
const TankReading = mongoose.model('TankReading', tankReadingSchema);
const SensorMetadata = mongoose.model('SensorMetadata', sensorMetadataSchema);
const DeviceResponse = mongoose.model('DeviceResponse', deviceResponseSchema);

/**
 * Save MQTT data to MongoDB
 */
export async function saveMqttDataToMongo(data) {
  try {
    const tankReading = new TankReading({
      deviceid: data.deviceid,
      sensor_no: data.sensor_no,
      switch_no: data.switch_no,
      level: data.level || data.value,
      value: data.value,
      status: data.status,
      message_type: data.message_type || 'mqtt_update',
      timestamp: data.timestamp ? new Date(data.timestamp) : new Date(),
      thingid: data.thingid,
      raw_data: data
    });

    await tankReading.save();
    logger.info(`✅ MQTT data saved to MongoDB for device ${data.deviceid}`);
    return tankReading;
  } catch (error) {
    logger.error(`❌ Error saving MQTT data to MongoDB: ${error.message}`);
    throw error;
  }
}

/**
 * Get latest sensor data for a device
 */
export async function getLatestSensorData(deviceId, sensorNumber) {
  try {
    const result = await TankReading.findOne({
      deviceid: deviceId,
      sensor_no: sensorNumber
    })
    .sort({ timestamp: -1 })
    .lean();

    return result;
  } catch (error) {
    logger.error('Error fetching latest sensor data from MongoDB:', error);
    throw error;
  }
}

/**
 * Get latest switch status for a device
 */
export async function getLatestSwitchStatus(deviceId, switchNumber) {
  try {
    const result = await TankReading.findOne({
      deviceid: deviceId,
      switch_no: switchNumber,
      status: { $ne: null }
    })
    .sort({ timestamp: -1 })
    .lean();

    return result;
  } catch (error) {
    logger.error('Error fetching latest switch status from MongoDB:', error);
    throw error;
  }
}

/**
 * Get historical sensor data for a device within a time range
 */
export async function getHistoricalSensorData(deviceId, sensorNumber = null, startDate, endDate, limit = 1000) {
  try {
    const query = {
      deviceid: deviceId,
      timestamp: {
        $gte: startDate,
        $lte: endDate
      }
    };

    if (sensorNumber) {
      query.sensor_no = sensorNumber;
    }

    const results = await TankReading.find(query)
      .sort({ timestamp: -1 })
      .limit(limit)
      .lean();

    return results;
  } catch (error) {
    logger.error('Error fetching historical sensor data from MongoDB:', error);
    throw error;
  }
}

/**
 * Get sensor metadata by device ID
 */
export async function getSensorMetadata(deviceId) {
  try {
    const result = await SensorMetadata.findOne({ deviceid: deviceId }).lean();
    return result;
  } catch (error) {
    logger.error('Error fetching sensor metadata from MongoDB:', error);
    throw error;
  }
}

/**
 * Get thing ID for a device
 */
export async function getThingIdByDeviceId(deviceId) {
  try {
    const result = await SensorMetadata.findOne({ deviceid: deviceId }, { thingid: 1 }).lean();
    return result ? result.thingid : null;
  } catch (error) {
    logger.error('Error fetching thing ID from MongoDB:', error);
    throw error;
  }
}

/**
 * Get recent device responses
 */
export async function getRecentDeviceResponses(thingId, seconds = 10) {
  try {
    const cutoffTime = new Date(Date.now() - (seconds * 1000));
    
    const results = await DeviceResponse.find({
      thingid: thingId,
      inserted_at: { $gte: cutoffTime }
    })
    .sort({ inserted_at: -1 })
    .lean();

    return results;
  } catch (error) {
    logger.error('Error fetching recent device responses from MongoDB:', error);
    throw error;
  }
}

/**
 * Get tank data with specific message type and time range
 */
export async function getTankDataByMessageType(deviceId, messageType, sensorNo = null, seconds = 10) {
  try {
    const cutoffTime = new Date(Date.now() - (seconds * 1000));
    
    const query = {
      deviceid: deviceId,
      message_type: messageType,
      timestamp: { $gte: cutoffTime }
    };

    if (sensorNo) {
      query.sensor_no = sensorNo;
    }

    const results = await TankReading.find(query)
      .sort({ timestamp: -1 })
      .lean();

    return results;
  } catch (error) {
    logger.error('Error fetching tank data by message type from MongoDB:', error);
    throw error;
  }
}

/**
 * Get aggregated sensor statistics
 */
export async function getSensorStatistics(deviceId, sensorNumber, startDate, endDate) {
  try {
    const pipeline = [
      {
        $match: {
          deviceid: deviceId,
          sensor_no: sensorNumber,
          timestamp: { $gte: startDate, $lte: endDate },
          level: { $ne: null }
        }
      },
      {
        $group: {
          _id: null,
          avgLevel: { $avg: '$level' },
          minLevel: { $min: '$level' },
          maxLevel: { $max: '$level' },
          totalReadings: { $sum: 1 },
          firstReading: { $min: '$timestamp' },
          lastReading: { $max: '$timestamp' }
        }
      }
    ];

    const result = await TankReading.aggregate(pipeline);
    return result[0] || null;
  } catch (error) {
    logger.error('Error fetching sensor statistics from MongoDB:', error);
    throw error;
  }
}

/**
 * Search tank readings with flexible criteria
 */
export async function searchTankReadings(criteria = {}, options = {}) {
  try {
    const {
      sort = { timestamp: -1 },
      limit = 100,
      skip = 0
    } = options;

    const results = await TankReading.find(criteria)
      .sort(sort)
      .limit(limit)
      .skip(skip)
      .lean();

    return results;
  } catch (error) {
    logger.error('Error searching tank readings from MongoDB:', error);
    throw error;
  }
}

/**
 * Get collection statistics
 */
export async function getMigrationStatistics() {
  try {
    const tankReadingsCount = await TankReading.countDocuments();
    const sensorMetadataCount = await SensorMetadata.countDocuments();
    const deviceResponsesCount = await DeviceResponse.countDocuments();

    // Get date ranges
    const oldestTankReading = await TankReading.findOne({}, { timestamp: 1 }).sort({ timestamp: 1 });
    const newestTankReading = await TankReading.findOne({}, { timestamp: 1 }).sort({ timestamp: -1 });

    return {
      collections: {
        tank_readings: tankReadingsCount,
        sensor_metadata: sensorMetadataCount,
        device_responses: deviceResponsesCount
      },
      dateRange: {
        oldest: oldestTankReading ? oldestTankReading.timestamp : null,
        newest: newestTankReading ? newestTankReading.timestamp : null
      }
    };
  } catch (error) {
    logger.error('Error fetching migration statistics:', error);
    throw error;
  }
}

/**
 * Update or create sensor metadata
 */
export async function upsertSensorMetadata(deviceId, thingId, deviceType, connectionInfo) {
  try {
    const result = await SensorMetadata.findOneAndUpdate(
      { deviceid: deviceId },
      {
        $set: {
          thingid: thingId,
          device_type: deviceType,
          connection_info: connectionInfo,
          last_seen: new Date()
        },
        $setOnInsert: {
          first_seen: new Date()
        }
      },
      { upsert: true, new: true }
    );

    logger.info(`✅ Sensor metadata updated for device ${deviceId}`);
    return result;
  } catch (error) {
    logger.error(`❌ Error upserting sensor metadata: ${error.message}`);
    throw error;
  }
}

/**
 * Save device response
 */
export async function saveDeviceResponse(thingId, deviceId, responseType, responseData) {
  try {
    const deviceResponse = new DeviceResponse({
      thingid: thingId,
      deviceid: deviceId,
      response_type: responseType,
      response_data: responseData,
      inserted_at: new Date()
    });

    await deviceResponse.save();
    logger.info(`✅ Device response saved for thingid ${thingId}`);
    return deviceResponse;
  } catch (error) {
    logger.error(`❌ Error saving device response: ${error.message}`);
    throw error;
  }
}

export {
  TankReading,
  SensorMetadata,
  DeviceResponse
};