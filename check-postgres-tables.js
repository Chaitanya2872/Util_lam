#!/usr/bin/env node

import pkg from "pg";
import dotenv from "dotenv";

const { Client } = pkg;
dotenv.config();

async function checkPostgresTables() {
  const client = new Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT || 5432,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    console.log('✅ Connected to PostgreSQL');

    // List all tables
    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      ORDER BY table_name;
    `);

    console.log('\n📋 Available tables:');
    tablesResult.rows.forEach(row => {
      console.log(`  - ${row.table_name}`);
    });

    // Check specific tables we're interested in
    const tablesToCheck = ['tank_data', 'sensor_data', 'slave_response'];
    
    for (const tableName of tablesToCheck) {
      try {
        const countResult = await client.query(`SELECT COUNT(*) FROM ${tableName}`);
        const count = parseInt(countResult.rows[0].count);
        console.log(`\n📊 ${tableName}: ${count} records`);
        
        if (count > 0) {
          // Show sample record
          const sampleResult = await client.query(`SELECT * FROM ${tableName} LIMIT 1`);
          console.log(`Sample record:`, sampleResult.rows[0]);
        }
      } catch (error) {
        console.log(`\n❌ Table ${tableName} does not exist or error: ${error.message}`);
      }
    }

  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await client.end();
  }
}

checkPostgresTables();