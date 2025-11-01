#!/usr/bin/env node

/**
 * Migration Validation Script
 * 
 * This script validates that the PostgreSQL to MongoDB migration
 * was successful by comparing record counts and sample data.
 * 
 * Usage: node validate-migration.js
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

// Import MongoDB models
import { TankReading, SensorMetadata, DeviceResponse } from './src/services/migratedDataService.js';

class MigrationValidator {
  constructor() {
    this.pgClient = null;
    this.validationResults = {
      tank_data: { postgres: 0, mongodb: 0, match: false, sampleMatch: false },
      sensor_data: { postgres: 0, mongodb: 0, match: false, sampleMatch: false },
      slave_response: { postgres: 0, mongodb: 0, match: false, sampleMatch: false }
    };
  }

  async initialize() {
    console.log('🔍 Starting migration validation...');
    
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
      await mongoose.connect(process.env.MONGO_URI, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });
      console.log('✅ Connected to MongoDB');
    } catch (error) {
      console.error('❌ Failed to connect to MongoDB:', error.message);
      throw error;
    }
  }

  async validateTankData() {
    console.log('\\n📊 Validating tank_data migration...');
    
    try {
      // Get PostgreSQL count
      const pgResult = await this.pgClient.query('SELECT COUNT(*) FROM tank_data');
      const pgCount = parseInt(pgResult.rows[0].count);
      
      // Get MongoDB count
      const mongoCount = await TankReading.countDocuments();
      
      this.validationResults.tank_data.postgres = pgCount;
      this.validationResults.tank_data.mongodb = mongoCount;
      this.validationResults.tank_data.match = pgCount === mongoCount;
      
      console.log(`PostgreSQL records: ${pgCount}`);
      console.log(`MongoDB records: ${mongoCount}`);
      console.log(`Counts match: ${this.validationResults.tank_data.match ? '✅' : '❌'}`);
      
      // Sample data validation
      if (pgCount > 0) {
        const pgSample = await this.pgClient.query(
          'SELECT * FROM tank_data ORDER BY timestamp DESC LIMIT 1'
        );
        
        const mongoSample = await TankReading.findOne().sort({ timestamp: -1 }).lean();
        
        if (pgSample.rows.length > 0 && mongoSample) {
          const pgRecord = pgSample.rows[0];
          const sampleMatch = (
            pgRecord.deviceid === mongoSample.deviceid &&
            pgRecord.sensor_no === mongoSample.sensor_no &&
            pgRecord.level === mongoSample.level
          );
          
          this.validationResults.tank_data.sampleMatch = sampleMatch;
          console.log(`Sample data match: ${sampleMatch ? '✅' : '❌'}`);
          
          if (!sampleMatch) {
            console.log('PostgreSQL sample:', {
              deviceid: pgRecord.deviceid,
              sensor_no: pgRecord.sensor_no,
              level: pgRecord.level,
              timestamp: pgRecord.timestamp
            });
            console.log('MongoDB sample:', {
              deviceid: mongoSample.deviceid,
              sensor_no: mongoSample.sensor_no,
              level: mongoSample.level,
              timestamp: mongoSample.timestamp
            });
          }
        }
      }
      
    } catch (error) {
      console.error('❌ Error validating tank_data:', error.message);
    }
  }

  async validateSensorData() {
    console.log('\\n🔧 Validating sensor_data migration...');
    
    try {
      // Check if table exists
      const tableCheck = await this.pgClient.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'sensor_data'
        );
      `);

      if (!tableCheck.rows[0].exists) {
        console.log('sensor_data table does not exist in PostgreSQL');
        this.validationResults.sensor_data.postgres = 0;
        this.validationResults.sensor_data.mongodb = await SensorMetadata.countDocuments();
        this.validationResults.sensor_data.match = this.validationResults.sensor_data.mongodb === 0;
        return;
      }

      // Get PostgreSQL count
      const pgResult = await this.pgClient.query('SELECT COUNT(*) FROM sensor_data');
      const pgCount = parseInt(pgResult.rows[0].count);
      
      // Get MongoDB count
      const mongoCount = await SensorMetadata.countDocuments();
      
      this.validationResults.sensor_data.postgres = pgCount;
      this.validationResults.sensor_data.mongodb = mongoCount;
      this.validationResults.sensor_data.match = pgCount === mongoCount;
      
      console.log(`PostgreSQL records: ${pgCount}`);
      console.log(`MongoDB records: ${mongoCount}`);
      console.log(`Counts match: ${this.validationResults.sensor_data.match ? '✅' : '❌'}`);
      
      // Sample data validation
      if (pgCount > 0) {
        const pgSample = await this.pgClient.query('SELECT * FROM sensor_data LIMIT 1');
        const mongoSample = await SensorMetadata.findOne().lean();
        
        if (pgSample.rows.length > 0 && mongoSample) {
          const pgRecord = pgSample.rows[0];
          const sampleMatch = (
            pgRecord.deviceid === mongoSample.deviceid &&
            pgRecord.thingid === mongoSample.thingid
          );
          
          this.validationResults.sensor_data.sampleMatch = sampleMatch;
          console.log(`Sample data match: ${sampleMatch ? '✅' : '❌'}`);
        }
      }
      
    } catch (error) {
      console.error('❌ Error validating sensor_data:', error.message);
    }
  }

  async validateSlaveResponse() {
    console.log('\\n📡 Validating slave_response migration...');
    
    try {
      // Check if table exists
      const tableCheck = await this.pgClient.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'slave_response'
        );
      `);

      if (!tableCheck.rows[0].exists) {
        console.log('slave_response table does not exist in PostgreSQL');
        this.validationResults.slave_response.postgres = 0;
        this.validationResults.slave_response.mongodb = await DeviceResponse.countDocuments();
        this.validationResults.slave_response.match = this.validationResults.slave_response.mongodb === 0;
        return;
      }

      // Get PostgreSQL count
      const pgResult = await this.pgClient.query('SELECT COUNT(*) FROM slave_response');
      const pgCount = parseInt(pgResult.rows[0].count);
      
      // Get MongoDB count
      const mongoCount = await DeviceResponse.countDocuments();
      
      this.validationResults.slave_response.postgres = pgCount;
      this.validationResults.slave_response.mongodb = mongoCount;
      this.validationResults.slave_response.match = pgCount === mongoCount;
      
      console.log(`PostgreSQL records: ${pgCount}`);
      console.log(`MongoDB records: ${mongoCount}`);
      console.log(`Counts match: ${this.validationResults.slave_response.match ? '✅' : '❌'}`);
      
      // Sample data validation
      if (pgCount > 0) {
        const pgSample = await this.pgClient.query(
          'SELECT * FROM slave_response ORDER BY inserted_at DESC LIMIT 1'
        );
        const mongoSample = await DeviceResponse.findOne().sort({ inserted_at: -1 }).lean();
        
        if (pgSample.rows.length > 0 && mongoSample) {
          const pgRecord = pgSample.rows[0];
          const sampleMatch = (
            pgRecord.thingid === mongoSample.thingid &&
            pgRecord.deviceid === mongoSample.deviceid
          );
          
          this.validationResults.slave_response.sampleMatch = sampleMatch;
          console.log(`Sample data match: ${sampleMatch ? '✅' : '❌'}`);
        }
      }
      
    } catch (error) {
      console.error('❌ Error validating slave_response:', error.message);
    }
  }

  async validateIndexes() {
    console.log('\\n🔍 Validating MongoDB indexes...');
    
    try {
      // Check TankReading indexes
      const tankIndexes = await TankReading.collection.getIndexes();
      console.log(`TankReading indexes: ${Object.keys(tankIndexes).length}`);
      
      // Check SensorMetadata indexes
      const sensorIndexes = await SensorMetadata.collection.getIndexes();
      console.log(`SensorMetadata indexes: ${Object.keys(sensorIndexes).length}`);
      
      // Check DeviceResponse indexes
      const responseIndexes = await DeviceResponse.collection.getIndexes();
      console.log(`DeviceResponse indexes: ${Object.keys(responseIndexes).length}`);
      
      console.log('✅ Index validation completed');
    } catch (error) {
      console.error('❌ Error validating indexes:', error.message);
    }
  }

  async generateValidationReport() {
    console.log('\\n📋 Validation Report');
    console.log('====================');
    
    let allValid = true;
    
    Object.entries(this.validationResults).forEach(([table, results]) => {
      console.log(`\\n${table.toUpperCase()}:`);
      console.log(`  PostgreSQL: ${results.postgres} records`);
      console.log(`  MongoDB: ${results.mongodb} records`);
      console.log(`  Count Match: ${results.match ? '✅ PASS' : '❌ FAIL'}`);
      console.log(`  Sample Match: ${results.sampleMatch ? '✅ PASS' : '❌ FAIL'}`);
      
      if (!results.match) {
        allValid = false;
      }
    });
    
    console.log('\\n' + '='.repeat(40));
    console.log(`Overall Validation: ${allValid ? '✅ PASSED' : '❌ FAILED'}`);
    
    if (allValid) {
      console.log('\\n🎉 Migration validation successful!');
      console.log('You can now safely switch to MongoDB by setting USE_MIGRATED_DATA=true');
    } else {
      console.log('\\n⚠️  Migration validation failed!');
      console.log('Please review the migration process and fix any issues before switching.');
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
      
      // Run validations
      await this.validateTankData();
      await this.validateSensorData();
      await this.validateSlaveResponse();
      await this.validateIndexes();
      
      // Generate report
      await this.generateValidationReport();
      
    } catch (error) {
      console.error('\\n💥 Validation failed:', error.message);
      console.error(error.stack);
      process.exit(1);
    } finally {
      await this.cleanup();
    }
  }
}

// Run validation if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const validator = new MigrationValidator();
  validator.run().then(() => {
    console.log('Validation process completed');
    process.exit(0);
  }).catch((error) => {
    console.error('Validation process failed:', error);
    process.exit(1);
  });
}

export default MigrationValidator;