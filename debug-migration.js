#!/usr/bin/env node

import pkg from "pg";
import mongoose from "mongoose";
import dotenv from "dotenv";

const { Client } = pkg;
dotenv.config();

console.log('🚀 Starting debug migration...');

// Test PostgreSQL connection
const pgClient = new Client({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: { rejectUnauthorized: false }
});

try {
  console.log('Connecting to PostgreSQL...');
  await pgClient.connect();
  console.log('✅ PostgreSQL connected');

  // Test MongoDB connection
  console.log('Connecting to MongoDB...');
  await mongoose.connect(process.env.MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  console.log('✅ MongoDB connected');

  // Test query
  console.log('Testing PostgreSQL query...');
  const result = await pgClient.query('SELECT COUNT(*) FROM tank_data');
  console.log(`PostgreSQL tank_data count: ${result.rows[0].count}`);

  // Test MongoDB
  console.log('Testing MongoDB...');
  const TankReading = mongoose.model('TankReading', new mongoose.Schema({
    deviceid: String,
    value: Number,
    timestamp: Date
  }, { collection: 'tank_readings' }));

  const mongoCount = await TankReading.countDocuments();
  console.log(`MongoDB tank_readings count: ${mongoCount}`);

  // If no data in MongoDB, let's migrate a small batch
  if (mongoCount === 0) {
    console.log('No data in MongoDB, migrating 10 records...');
    
    const sampleData = await pgClient.query('SELECT * FROM tank_data LIMIT 10');
    console.log(`Got ${sampleData.rows.length} records from PostgreSQL`);
    
    const batch = sampleData.rows.map(row => ({
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

    await TankReading.insertMany(batch);
    console.log(`✅ Inserted ${batch.length} records into MongoDB`);
    
    // Verify
    const newCount = await TankReading.countDocuments();
    console.log(`New MongoDB count: ${newCount}`);
  }

} catch (error) {
  console.error('❌ Error:', error.message);
  console.error(error.stack);
} finally {
  if (pgClient) await pgClient.end();
  if (mongoose.connection.readyState === 1) await mongoose.connection.close();
  console.log('Connections closed');
}