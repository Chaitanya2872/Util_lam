#!/usr/bin/env node

import pkg from "pg";
import mongoose from "mongoose";
import dotenv from "dotenv";

const { Client } = pkg;
dotenv.config();

// MongoDB Schemas
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

const TankReading = mongoose.model('TankReading', tankReadingSchema);
const SensorMetadata = mongoose.model('SensorMetadata', sensorMetadataSchema);
const DeviceResponse = mongoose.model('DeviceResponse', deviceResponseSchema);

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runRobustMigration() {
  console.log('🚀 Starting Robust Migration...');
  
  // Connect to PostgreSQL
  const pgClient = new Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT || 5432,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false }
  });

  try {
    console.log('Connecting to databases...');
    await pgClient.connect();
    await mongoose.connect(process.env.MONGO_URI);
    console.log('✅ Connected to both databases');

    // Check existing data
    const existingTankCount = await TankReading.countDocuments();
    const existingSensorCount = await SensorMetadata.countDocuments();
    const existingResponseCount = await DeviceResponse.countDocuments();

    console.log(`Existing data - Tank: ${existingTankCount}, Sensor: ${existingSensorCount}, Response: ${existingResponseCount}`);

    // Migrate tank_data with smaller batches and delays
    console.log('\\n📊 Migrating tank_data...');
    const tankCountResult = await pgClient.query('SELECT COUNT(*) FROM tank_data');
    const totalTankRecords = parseInt(tankCountResult.rows[0].count);
    console.log(`Total tank_data records: ${totalTankRecords}`);

    const batchSize = 100; // Smaller batch size
    let offset = existingTankCount; // Resume from where we left off
    let processed = existingTankCount;

    while (offset < totalTankRecords) {
      try {
        const batchResult = await pgClient.query(
          'SELECT * FROM tank_data ORDER BY timestamp ASC LIMIT $1 OFFSET $2',
          [batchSize, offset]
        );

        if (batchResult.rows.length === 0) break;

        const batch = batchResult.rows.map(row => ({
          deviceid: row.deviceid,
          sensor_no: row.sensor_no,
          switch_no: row.switch_no,
          level: row.level || row.value,
          value: row.value,
          status: row.status,
          message_type: row.message_type,
          timestamp: row.timestamp,
          thingid: row.thingid,
          raw_data: row
        }));

        await TankReading.insertMany(batch, { ordered: false });
        processed += batch.length;
        offset += batchSize;

        console.log(`Processed ${processed}/${totalTankRecords} tank records (${((processed/totalTankRecords)*100).toFixed(1)}%)`);
        
        // Small delay to prevent overwhelming the connection
        await sleep(100);

      } catch (error) {
        console.error(`Error processing batch at offset ${offset}:`, error.message);
        offset += batchSize; // Skip this batch and continue
      }
    }

    // Migrate sensor_data
    console.log('\\n🔧 Migrating sensor_data...');
    if (existingSensorCount === 0) {
      const sensorDataResult = await pgClient.query('SELECT * FROM sensor_data');
      console.log(`Found ${sensorDataResult.rows.length} sensor_data records`);

      for (const row of sensorDataResult.rows) {
        try {
          const sensorMetadata = new SensorMetadata({
            deviceid: row.deviceid,
            thingid: row.thingid,
            device_type: row.device_type,
            connection_info: row,
            first_seen: row.created_at || row.first_seen,
            last_seen: row.updated_at || row.last_seen
          });

          await sensorMetadata.save();
        } catch (error) {
          console.log(`Skipping sensor ${row.deviceid}: ${error.message}`);
        }
      }
    } else {
      console.log('Sensor data already migrated, skipping...');
    }

    // Migrate slave_response
    console.log('\\n📡 Migrating slave_response...');
    if (existingResponseCount === 0) {
      const responseCountResult = await pgClient.query('SELECT COUNT(*) FROM slave_response');
      const totalResponseRecords = parseInt(responseCountResult.rows[0].count);
      console.log(`Total slave_response records: ${totalResponseRecords}`);

      let responseOffset = 0;
      let responseProcessed = 0;

      while (responseOffset < totalResponseRecords) {
        try {
          const batchResult = await pgClient.query(
            'SELECT * FROM slave_response ORDER BY inserted_at ASC LIMIT $1 OFFSET $2',
            [batchSize, responseOffset]
          );

          if (batchResult.rows.length === 0) break;

          const batch = batchResult.rows.map(row => ({
            thingid: row.thingid,
            deviceid: row.deviceid,
            response_type: 'slave_response',
            response_data: row,
            inserted_at: row.inserted_at
          }));

          await DeviceResponse.insertMany(batch, { ordered: false });
          responseProcessed += batch.length;
          responseOffset += batchSize;

          console.log(`Processed ${responseProcessed}/${totalResponseRecords} response records`);
          await sleep(100);

        } catch (error) {
          console.error(`Error processing response batch at offset ${responseOffset}:`, error.message);
          responseOffset += batchSize;
        }
      }
    } else {
      console.log('Response data already migrated, skipping...');
    }

    // Create indexes
    console.log('\\n🔍 Creating indexes...');
    await TankReading.createIndexes();
    await SensorMetadata.createIndexes();
    await DeviceResponse.createIndexes();

    // Final counts
    console.log('\\n📋 Final Results:');
    const tankCount = await TankReading.countDocuments();
    const sensorCount = await SensorMetadata.countDocuments();
    const responseCount = await DeviceResponse.countDocuments();

    console.log(`Tank readings: ${tankCount}`);
    console.log(`Sensor metadata: ${sensorCount}`);
    console.log(`Device responses: ${responseCount}`);

    // Test a sample query
    console.log('\\n🧪 Testing sample queries...');
    const latestReading = await TankReading.findOne().sort({ timestamp: -1 });
    if (latestReading) {
      console.log('Latest reading:', {
        deviceid: latestReading.deviceid,
        sensor_no: latestReading.sensor_no,
        value: latestReading.value,
        timestamp: latestReading.timestamp
      });
    }

    console.log('\\n🎉 Migration completed successfully!');
    console.log('\\nNext steps:');
    console.log('1. Run validation: node validate-migration.js');
    console.log('2. Set USE_MIGRATED_DATA=true in .env');
    console.log('3. Restart your application');

  } catch (error) {
    console.error('❌ Migration failed:', error.message);
    console.error(error.stack);
  } finally {
    if (pgClient) await pgClient.end();
    if (mongoose.connection.readyState === 1) await mongoose.connection.close();
    console.log('Connections closed');
  }
}

runRobustMigration();