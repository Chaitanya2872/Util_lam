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

async function runMigration() {
  console.log('🚀 Starting Simple Migration...');
  
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

    // Clear existing data
    console.log('Clearing existing MongoDB collections...');
    await TankReading.deleteMany({});
    await SensorMetadata.deleteMany({});
    await DeviceResponse.deleteMany({});
    console.log('✅ Cleared existing data');

    // Migrate tank_data
    console.log('\\n📊 Migrating tank_data...');
    const tankDataResult = await pgClient.query('SELECT * FROM tank_data ORDER BY timestamp ASC');
    console.log(`Found ${tankDataResult.rows.length} tank_data records`);

    const batchSize = 1000;
    let processed = 0;

    for (let i = 0; i < tankDataResult.rows.length; i += batchSize) {
      const batch = tankDataResult.rows.slice(i, i + batchSize).map(row => ({
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
      console.log(`Processed ${processed}/${tankDataResult.rows.length} tank records`);
    }

    // Migrate sensor_data
    console.log('\\n🔧 Migrating sensor_data...');
    const sensorDataResult = await pgClient.query('SELECT * FROM sensor_data');
    console.log(`Found ${sensorDataResult.rows.length} sensor_data records`);

    for (const row of sensorDataResult.rows) {
      const sensorMetadata = new SensorMetadata({
        deviceid: row.deviceid,
        thingid: row.thingid,
        device_type: row.device_type,
        connection_info: row,
        first_seen: row.created_at || row.first_seen,
        last_seen: row.updated_at || row.last_seen
      });

      try {
        await sensorMetadata.save();
      } catch (error) {
        console.log(`Skipping duplicate sensor: ${row.deviceid}`);
      }
    }

    // Migrate slave_response
    console.log('\\n📡 Migrating slave_response...');
    const slaveResponseResult = await pgClient.query('SELECT * FROM slave_response ORDER BY inserted_at ASC');
    console.log(`Found ${slaveResponseResult.rows.length} slave_response records`);

    processed = 0;
    for (let i = 0; i < slaveResponseResult.rows.length; i += batchSize) {
      const batch = slaveResponseResult.rows.slice(i, i + batchSize).map(row => ({
        thingid: row.thingid,
        deviceid: row.deviceid,
        response_type: 'slave_response',
        response_data: row,
        inserted_at: row.inserted_at
      }));

      await DeviceResponse.insertMany(batch, { ordered: false });
      processed += batch.length;
      console.log(`Processed ${processed}/${slaveResponseResult.rows.length} response records`);
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

    console.log('\\n🎉 Migration completed successfully!');

  } catch (error) {
    console.error('❌ Migration failed:', error.message);
    console.error(error.stack);
  } finally {
    if (pgClient) await pgClient.end();
    if (mongoose.connection.readyState === 1) await mongoose.connection.close();
    console.log('Connections closed');
  }
}

runMigration();