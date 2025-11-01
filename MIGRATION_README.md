# PostgreSQL to MongoDB Migration Guide

This guide explains how to migrate your IoT sensor data from PostgreSQL to MongoDB without disrupting your existing application.

## Overview

The migration creates separate MongoDB collections for your PostgreSQL data:
- `tank_readings` - Historical tank sensor data (from `tank_data` table)
- `sensor_metadata` - Device sensor information (from `sensor_data` table)  
- `device_responses` - Device communication responses (from `slave_response` table)

## Files Created

1. **`migrate-postgres-to-mongodb.js`** - Main migration script
2. **`src/services/migratedDataService.js`** - Service to query migrated data
3. **`src/services/migratedControlService.js`** - Helper for control operations
4. **Updated `src/services/tankDataService.js`** - Now supports both databases

## Pre-Migration Steps

1. **Backup your databases** (both PostgreSQL and MongoDB)
   ```bash
   # PostgreSQL backup
   pg_dump -h your-host -U your-user -d your-database > backup.sql
   
   # MongoDB backup
   mongodump --uri="your-mongo-uri" --out=./mongodb-backup
   ```

2. **Verify environment variables** in `.env`:
   ```env
   # PostgreSQL (existing)
   DB_HOST=your-postgres-host
   DB_USER=your-postgres-user
   DB_PASSWORD=your-postgres-password
   DB_NAME=your-postgres-database
   
   # MongoDB (existing)
   MONGO_URI=your-mongodb-connection-string
   
   # Migration control (new)
   USE_MIGRATED_DATA=false  # Set to true after migration
   ```

## Running the Migration

1. **Install dependencies** (if not already installed):
   ```bash
   npm install
   ```

2. **Run the migration script**:
   ```bash
   node migrate-postgres-to-mongodb.js
   ```

3. **Monitor the progress**:
   The script will show:
   - Connection status to both databases
   - Migration progress for each table
   - Final statistics and success rates

## Post-Migration Steps

1. **Verify the migration**:
   ```bash
   # Check MongoDB collections
   mongosh "your-mongo-uri"
   > use your-database
   > db.tank_readings.countDocuments()
   > db.sensor_metadata.countDocuments()
   > db.device_responses.countDocuments()
   ```

2. **Test with migrated data**:
   ```bash
   # Set environment variable to use MongoDB
   echo "USE_MIGRATED_DATA=true" >> .env
   
   # Restart your application
   npm start
   ```

3. **Verify API responses**:
   Test your existing API endpoints to ensure they return the same data.

## Migration Features

### Batch Processing
- Processes large datasets in batches of 1000 records
- Shows progress indicators
- Handles memory efficiently

### Error Handling
- Continues migration even if some records fail
- Reports error counts and success rates
- Preserves original data structure

### Data Integrity
- Creates indexes for optimal query performance
- Maintains original timestamps and relationships
- Stores complete original records in `raw_data` field

### Zero Downtime
- Migration runs independently of your application
- Original PostgreSQL data remains untouched
- Switch between databases using environment variable

## Using Migrated Data

### Automatic Switching
Your existing API endpoints will automatically use MongoDB when `USE_MIGRATED_DATA=true`:

```javascript
// GET /api/sensor/:deviceid/:sensorNumber
// GET /api/switch/:deviceid/:switchNumber
```

### Direct MongoDB Queries
Use the new service for custom queries:

```javascript
import { 
  getLatestSensorData, 
  getHistoricalSensorData,
  getSensorStatistics 
} from './src/services/migratedDataService.js';

// Get latest reading
const latest = await getLatestSensorData('device123', 'TM1');

// Get historical data
const history = await getHistoricalSensorData(
  'device123', 
  'TM1', 
  new Date('2024-01-01'), 
  new Date('2024-01-31')
);

// Get statistics
const stats = await getSensorStatistics(
  'device123', 
  'TM1', 
  new Date('2024-01-01'), 
  new Date('2024-01-31')
);
```

## Performance Benefits

### MongoDB Advantages
- **Flexible Schema**: Easy to add new fields without migrations
- **Better Scaling**: Horizontal scaling capabilities
- **Faster Queries**: Optimized for IoT time-series data
- **Rich Aggregations**: Built-in analytics capabilities

### Optimized Indexes
The migration creates indexes for:
- Device ID + Timestamp (for latest readings)
- Sensor Number + Timestamp (for sensor-specific queries)
- Message Type + Timestamp (for filtering by message type)

## Rollback Plan

If you need to rollback to PostgreSQL:

1. **Stop your application**
2. **Update environment variable**:
   ```bash
   # Set back to PostgreSQL
   USE_MIGRATED_DATA=false
   ```
3. **Restart your application**

Your original PostgreSQL data is never modified during migration.

## Monitoring and Maintenance

### Collection Statistics
Check migration statistics anytime:

```javascript
import { getMigrationStatistics } from './src/services/migratedDataService.js';

const stats = await getMigrationStatistics();
console.log(stats);
```

### Data Validation
Compare record counts between databases:

```sql
-- PostgreSQL
SELECT COUNT(*) FROM tank_data;
SELECT COUNT(*) FROM sensor_data;
SELECT COUNT(*) FROM slave_response;
```

```javascript
// MongoDB
db.tank_readings.countDocuments()
db.sensor_metadata.countDocuments()
db.device_responses.countDocuments()
```

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify database credentials in `.env`
   - Check network connectivity
   - Ensure databases are running

2. **Memory Issues**:
   - Migration processes data in batches
   - Monitor system memory during migration
   - Adjust batch size if needed

3. **Duplicate Key Errors**:
   - Migration uses `insertMany` with `ordered: false`
   - Duplicate records are skipped automatically
   - Check error counts in final report

4. **Performance Issues**:
   - Ensure indexes are created after migration
   - Monitor MongoDB performance metrics
   - Consider sharding for very large datasets

### Getting Help

If you encounter issues:
1. Check the migration logs for specific error messages
2. Verify your environment configuration
3. Test with a small dataset first
4. Monitor both database connections during migration

## Next Steps

After successful migration:
1. **Monitor Performance**: Compare query times between databases
2. **Optimize Queries**: Use MongoDB's aggregation framework for analytics
3. **Plan Decommission**: Once stable, plan PostgreSQL decommission
4. **Update Documentation**: Update your API documentation if needed

## Data Retention

Consider implementing data retention policies:
- Archive old sensor readings
- Implement TTL (Time To Live) indexes
- Regular cleanup of historical data

This migration provides a solid foundation for scaling your IoT data infrastructure while maintaining backward compatibility.