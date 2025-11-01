#!/usr/bin/env node

/**
 * PostgreSQL to MongoDB Migration Script
 * 
 * This script migrates IoT sensor data from PostgreSQL to MongoDB
 * without disturbing the existing application functionality.
 * 
 * Collections created:
 * - tank_readings: Historical tank sensor data
 * - sensor_metadata: Device sensor information
 * - device_responses: Device communication responses
 * 
 * Usage: node migrate-postgres-to-mongodb.js
 */

import pkg from "pg";
import mongoose from "mongoose";
import dotenv from "dotenv";
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const { Client } = pkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables
dotenv.config({ path: join(__dirname, '.env') });

// MongoDB Schemas for migrated data
const tankReadingSchema = new mongoose.Schema({
  deviceid: { type: String, required: true, index: true },
  sensor_no: { type: String, index: true },
  switch_no: { type: String, index: true },
  level: { type: Number },
  value: { type: Number }, // Original value field from PostgreSQL
  status: { type: String },
  message_type: { type: String, index: true },
  timestamp: { type: Date, required: true, index: true },
  thingid: { type: String, index: true },
  raw_data: { type: mongoose.Schema.Types.Mixed }, // Store any additional fields
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

class PostgresToMongoMigrator {
  constructor() {
    this.pgClient = null;
    this.mongoConnection = null;
    this.migrationStats = {
      tank_readings: { total: 0, migrated: 0, errors: 0 },
      sensor_metadata: { total: 0, migrated: 0, errors: 0 },
      device_responses: { total: 0, migrated: 0, errors: 0 }
    };
  }

  async initialize() {
    console.log('🚀 Starting PostgreSQL to MongoDB migration...');
    
    // Connect to PostgreSQL
    this.pgClient = new Client({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT || 5432,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      ssl: { rejectUnauthorized: false }
    });

    try {
      await this.pgClient.connect();
      console.log('✅ Connected to PostgreSQL');
    } catch (error) {
      console.error('❌ Failed to connect to PostgreSQL:', error.message);
      throw error;
    }

    // Connect to MongoDB
    try {
      await mongoose.connect(process.env.MONGO_URI);
      console.log('✅ Connected to MongoDB');
    } catch (error) {
      console.error('❌ Failed to connect to MongoDB:', error.message);
      throw error;
    }
  }

  async migrateTankData() {
    console.log('\\n📊 Migrating tank_data table...');
    
    try {
      // Get total count
      const countResult = await this.pgClient.query('SELECT COUNT(*) FROM tank_data');
      const totalRecords = parseInt(countResult.rows[0].count);
      this.migrationStats.tank_readings.total = totalRecords;
      
      console.log(`Found ${totalRecords} records in tank_data table`);
      
      if (totalRecords === 0) {
        console.log('No tank_data records to migrate');
        return;
      }

      // Migrate in batches to handle large datasets
      const batchSize = 1000;
      let offset = 0;
      let migratedCount = 0;

      while (offset < totalRecords) {
        const query = `
          SELECT * FROM tank_data 
          ORDER BY timestamp ASC 
          LIMIT $1 OFFSET $2
        `;
        
        const result = await this.pgClient.query(query, [batchSize, offset]);
        const batch = [];

        for (const row of result.rows) {
          const tankReading = {
            deviceid: row.deviceid,
            sensor_no: row.sensor_no,
            switch_no: row.switch_no,
            level: row.level || row.value, // Handle both 'level' and 'value' fields
            value: row.value, // Keep original value field
            status: row.status,
            message_type: row.message_type,
            timestamp: row.timestamp,
            thingid: row.thingid,
            raw_data: row // Store complete original record
          };

          batch.push(tankReading);
        }

        if (batch.length > 0) {
          try {
            await TankReading.insertMany(batch, { ordered: false });
            migratedCount += batch.length;
            this.migrationStats.tank_readings.migrated += batch.length;
            
            const progress = ((migratedCount / totalRecords) * 100).toFixed(1);
            console.log(`Progress: ${migratedCount}/${totalRecords} (${progress}%)`);
          } catch (error) {
            console.error(`Error inserting batch at offset ${offset}:`, error.message);
            this.migrationStats.tank_readings.errors += batch.length;
          }
        }

        offset += batchSize;
      }

      console.log(`✅ Tank data migration completed: ${migratedCount}/${totalRecords} records`);
    } catch (error) {
      console.error('❌ Error migrating tank_data:', error.message);
      throw error;
    }
  }

  async migrateSensorData() {
    console.log('\\n🔧 Migrating sensor_data table...');
    
    try {
      // Check if sensor_data table exists
      const tableCheck = await this.pgClient.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'sensor_data'
        );
      `);

      if (!tableCheck.rows[0].exists) {
        console.log('sensor_data table does not exist, skipping...');
        return;
      }

      const countResult = await this.pgClient.query('SELECT COUNT(*) FROM sensor_data');
      const totalRecords = parseInt(countResult.rows[0].count);
      this.migrationStats.sensor_metadata.total = totalRecords;
      
      console.log(`Found ${totalRecords} records in sensor_data table`);
      
      if (totalRecords === 0) {
        console.log('No sensor_data records to migrate');
        return;
      }

      const result = await this.pgClient.query('SELECT * FROM sensor_data ORDER BY deviceid');
      
      for (const row of result.rows) {
        try {
          const sensorMetadata = new SensorMetadata({
            deviceid: row.deviceid,
            thingid: row.thingid,
            device_type: row.device_type,
            connection_info: {
              // Store any connection-related fields
              ...Object.keys(row).reduce((acc, key) => {
                if (!['deviceid', 'thingid', 'device_type'].includes(key)) {
                  acc[key] = row[key];
                }
                return acc;
              }, {})
            },
            first_seen: row.created_at || row.first_seen,
            last_seen: row.updated_at || row.last_seen
          });

          await sensorMetadata.save();
          this.migrationStats.sensor_metadata.migrated++;
        } catch (error) {
          console.error(`Error migrating sensor record ${row.deviceid}:`, error.message);
          this.migrationStats.sensor_metadata.errors++;
        }
      }

      console.log(`✅ Sensor metadata migration completed: ${this.migrationStats.sensor_metadata.migrated}/${totalRecords} records`);
    } catch (error) {
      console.error('❌ Error migrating sensor_data:', error.message);
      // Don't throw error if table doesn't exist
      if (!error.message.includes('does not exist')) {
        throw error;
      }
    }
  }

  async migrateSlaveResponses() {
    console.log('\\n📡 Migrating slave_response table...');
    
    try {
      // Check if slave_response table exists
      const tableCheck = await this.pgClient.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'slave_response'
        );
      `);

      if (!tableCheck.rows[0].exists) {
        console.log('slave_response table does not exist, skipping...');
        return;
      }

      const countResult = await this.pgClient.query('SELECT COUNT(*) FROM slave_response');
      const totalRecords = parseInt(countResult.rows[0].count);
      this.migrationStats.device_responses.total = totalRecords;
      
      console.log(`Found ${totalRecords} records in slave_response table`);
      
      if (totalRecords === 0) {
        console.log('No slave_response records to migrate');
        return;
      }

      // Migrate in batches
      const batchSize = 1000;
      let offset = 0;
      let migratedCount = 0;

      while (offset < totalRecords) {
        const query = `
          SELECT * FROM slave_response 
          ORDER BY inserted_at ASC 
          LIMIT $1 OFFSET $2
        `;
        
        const result = await this.pgClient.query(query, [batchSize, offset]);
        const batch = [];

        for (const row of result.rows) {
          const deviceResponse = {
            thingid: row.thingid,
            deviceid: row.deviceid,
            response_type: 'slave_response',
            response_data: row,
            inserted_at: row.inserted_at
          };

          batch.push(deviceResponse);
        }

        if (batch.length > 0) {
          try {
            await DeviceResponse.insertMany(batch, { ordered: false });
            migratedCount += batch.length;
            this.migrationStats.device_responses.migrated += batch.length;
            
            const progress = ((migratedCount / totalRecords) * 100).toFixed(1);
            console.log(`Progress: ${migratedCount}/${totalRecords} (${progress}%)`);
          } catch (error) {
            console.error(`Error inserting response batch at offset ${offset}:`, error.message);
            this.migrationStats.device_responses.errors += batch.length;
          }
        }

        offset += batchSize;
      }

      console.log(`✅ Device responses migration completed: ${migratedCount}/${totalRecords} records`);
    } catch (error) {
      console.error('❌ Error migrating slave_response:', error.message);
      // Don't throw error if table doesn't exist
      if (!error.message.includes('does not exist')) {
        throw error;
      }
    }
  }

  async createIndexes() {
    console.log('\\n🔍 Creating MongoDB indexes...');
    
    try {
      // Create indexes for better query performance
      await TankReading.createIndexes();
      await SensorMetadata.createIndexes();
      await DeviceResponse.createIndexes();
      
      // Create compound indexes for common queries
      await TankReading.collection.createIndex({ deviceid: 1, timestamp: -1 });
      await TankReading.collection.createIndex({ sensor_no: 1, timestamp: -1 });
      await TankReading.collection.createIndex({ message_type: 1, timestamp: -1 });
      
      console.log('✅ Indexes created successfully');
    } catch (error) {
      console.error('❌ Error creating indexes:', error.message);
    }
  }

  async generateMigrationReport() {
    console.log('\\n📋 Migration Report');
    console.log('==================');
    
    Object.entries(this.migrationStats).forEach(([collection, stats]) => {
      console.log(`\\n${collection.toUpperCase()}:`);
      console.log(`  Total records: ${stats.total}`);
      console.log(`  Migrated: ${stats.migrated}`);
      console.log(`  Errors: ${stats.errors}`);
      console.log(`  Success rate: ${stats.total > 0 ? ((stats.migrated / stats.total) * 100).toFixed(1) : 0}%`);
    });

    // Verify collections in MongoDB
    console.log('\\n📊 MongoDB Collections Status:');
    try {
      const tankCount = await TankReading.countDocuments();
      const sensorCount = await SensorMetadata.countDocuments();
      const responseCount = await DeviceResponse.countDocuments();
      
      console.log(`  tank_readings: ${tankCount} documents`);
      console.log(`  sensor_metadata: ${sensorCount} documents`);
      console.log(`  device_responses: ${responseCount} documents`);
    } catch (error) {
      console.error('Error getting collection counts:', error.message);
    }
  }

  async cleanup() {
    console.log('\\n🧹 Cleaning up connections...');
    
    if (this.pgClient) {
      await this.pgClient.end();
      console.log('✅ PostgreSQL connection closed');
    }
    
    if (mongoose.connection.readyState === 1) {
      await mongoose.connection.close();
      console.log('✅ MongoDB connection closed');
    }
  }

  async run() {
    try {
      await this.initialize();
      
      // Run migrations
      await this.migrateTankData();
      await this.migrateSensorData();
      await this.migrateSlaveResponses();
      
      // Create indexes for performance
      await this.createIndexes();
      
      // Generate report
      await this.generateMigrationReport();
      
      console.log('\\n🎉 Migration completed successfully!');
      
    } catch (error) {
      console.error('\\n💥 Migration failed:', error.message);
      console.error(error.stack);
      process.exit(1);
    } finally {
      await this.cleanup();
    }
  }
}

// Run migration if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const migrator = new PostgresToMongoMigrator();
  migrator.run().then(() => {
    console.log('Migration process completed');
    process.exit(0);
  }).catch((error) => {
    console.error('Migration process failed:', error);
    process.exit(1);
  });
}

export default PostgresToMongoMigrator;