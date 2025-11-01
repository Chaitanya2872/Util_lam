#!/usr/bin/env node

import dotenv from "dotenv";
import mongoose from "mongoose";
import { getLatestSensorData, getLatestSwitchStatus } from './src/services/migratedDataService.js';

dotenv.config();

async function testMongoDBAPI() {
  console.log('🧪 Testing MongoDB API functions...');
  
  try {
    // Connect to MongoDB first
    console.log('Connecting to MongoDB...');
    await mongoose.connect(process.env.MONGO_URI);
    console.log('✅ Connected to MongoDB');
    // Test getLatestSensorData
    console.log('\\n📊 Testing getLatestSensorData...');
    const sensorData = await getLatestSensorData('IOTIQBM_A0525001', 'TM1');
    
    if (sensorData) {
      console.log('✅ Sensor data found:', {
        deviceid: sensorData.deviceid,
        sensor_no: sensorData.sensor_no,
        value: sensorData.value,
        level: sensorData.level,
        timestamp: sensorData.timestamp
      });
    } else {
      console.log('❌ No sensor data found');
    }

    // Test getLatestSwitchStatus
    console.log('\\n🔌 Testing getLatestSwitchStatus...');
    const switchData = await getLatestSwitchStatus('IOTIQBM_A0525001', 'BM1');
    
    if (switchData) {
      console.log('✅ Switch data found:', {
        deviceid: switchData.deviceid,
        switch_no: switchData.switch_no,
        status: switchData.status,
        timestamp: switchData.timestamp
      });
    } else {
      console.log('❌ No switch data found');
    }

    // Test with different device
    console.log('\\n📊 Testing with different device...');
    const sensorData2 = await getLatestSensorData('IOTIQBM_A0525011', 'TM1');
    
    if (sensorData2) {
      console.log('✅ Second device data found:', {
        deviceid: sensorData2.deviceid,
        sensor_no: sensorData2.sensor_no,
        value: sensorData2.value,
        timestamp: sensorData2.timestamp
      });
    } else {
      console.log('❌ No data found for second device');
    }

    console.log('\\n🎉 MongoDB API test completed!');
    console.log('\\nYour application should now be using MongoDB for sensor data.');
    console.log('Tank levels and switch status will come from MongoDB instead of PostgreSQL.');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error(error.stack);
  } finally {
    if (mongoose.connection.readyState === 1) {
      await mongoose.connection.close();
    }
    process.exit(0);
  }
}

testMongoDBAPI();