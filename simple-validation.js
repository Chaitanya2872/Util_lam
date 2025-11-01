#!/usr/bin/env node

import pkg from "pg";
import mongoose from "mongoose";
import dotenv from "dotenv";

const { Client } = pkg;
dotenv.config();

// Import MongoDB models
import { TankReading, SensorMetadata, DeviceResponse } from './src/services/migratedDataService.js';

async function validateMigration() {
  console.log('🔍 Validating migration...');
  
  const pgClient = new Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT || 5432,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await pgClient.connect();
    await mongoose.connect(process.env.MONGO_URI);
    console.log('✅ Connected to both databases');

    // Check counts
    const pgTankCount = await pgClient.query('SELECT COUNT(*) FROM tank_data');
    const pgSensorCount = await pgClient.query('SELECT COUNT(*) FROM sensor_data');
    const pgResponseCount = await pgClient.query('SELECT COUNT(*) FROM slave_response');

    const mongoTankCount = await TankReading.countDocuments();
    const mongoSensorCount = await SensorMetadata.countDocuments();
    const mongoResponseCount = await DeviceResponse.countDocuments();

    console.log('\\n📊 Record Counts:');
    console.log(`Tank Data - PostgreSQL: ${pgTankCount.rows[0].count}, MongoDB: ${mongoTankCount}`);
    console.log(`Sensor Data - PostgreSQL: ${pgSensorCount.rows[0].count}, MongoDB: ${mongoSensorCount}`);
    console.log(`Response Data - PostgreSQL: ${pgResponseCount.rows[0].count}, MongoDB: ${mongoResponseCount}`);

    // Test sample queries
    console.log('\\n🧪 Testing sample queries...');
    
    // Test latest sensor reading
    const pgLatest = await pgClient.query('SELECT * FROM tank_data ORDER BY timestamp DESC LIMIT 1');
    const mongoLatest = await TankReading.findOne().sort({ timestamp: -1 });

    if (pgLatest.rows.length > 0 && mongoLatest) {
      console.log('PostgreSQL latest:', {
        deviceid: pgLatest.rows[0].deviceid,
        sensor_no: pgLatest.rows[0].sensor_no,
        value: pgLatest.rows[0].value,
        timestamp: pgLatest.rows[0].timestamp
      });
      console.log('MongoDB latest:', {
        deviceid: mongoLatest.deviceid,
        sensor_no: mongoLatest.sensor_no,
        value: mongoLatest.value,
        timestamp: mongoLatest.timestamp
      });
    }

    // Test specific device query
    const testDeviceId = 'IOTIQBM_A0525001';
    const testSensorNo = 'TM1';
    
    const pgSpecific = await pgClient.query(
      'SELECT * FROM tank_data WHERE deviceid = $1 AND sensor_no = $2 ORDER BY timestamp DESC LIMIT 1',
      [testDeviceId, testSensorNo]
    );
    
    const mongoSpecific = await TankReading.findOne({
      deviceid: testDeviceId,
      sensor_no: testSensorNo
    }).sort({ timestamp: -1 });

    console.log('\\n🎯 Specific device query test:');
    if (pgSpecific.rows.length > 0) {
      console.log('PostgreSQL result:', {
        deviceid: pgSpecific.rows[0].deviceid,
        sensor_no: pgSpecific.rows[0].sensor_no,
        value: pgSpecific.rows[0].value
      });
    }
    
    if (mongoSpecific) {
      console.log('MongoDB result:', {
        deviceid: mongoSpecific.deviceid,
        sensor_no: mongoSpecific.sensor_no,
        value: mongoSpecific.value
      });
    }

    console.log('\\n✅ Validation completed!');
    console.log('\\nYou can now:');
    console.log('1. Add USE_MIGRATED_DATA=true to your .env file');
    console.log('2. Restart your application');
    console.log('3. Test your API endpoints');

  } catch (error) {
    console.error('❌ Validation failed:', error.message);
  } finally {
    if (pgClient) await pgClient.end();
    if (mongoose.connection.readyState === 1) await mongoose.connection.close();
  }
}

validateMigration();