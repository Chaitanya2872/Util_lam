// routes/traceRoutes.js
import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';

const router = express.Router();

/**
 * GET /api/logs - Get paginated logs with enhanced filtering
 * Query params:
 * - limit: number of logs per page (default: 100, max: 1000)
 * - page: page number (default: 1)
 * - level: filter by log level (error, warn, info, debug)
 * - type: filter by log type (request, response, error)
 * - method: filter by HTTP method (GET, POST, PUT, DELETE)
 * - route: filter by route/path
 * - function: filter by function name
 * - status: filter by HTTP status code
 * - user_id: filter by user ID
 * - device_id: filter by device ID
 * - source: filter by source (frontend, backend, mobile app)
 * - traceId: filter by trace ID
 * - startDate: filter logs from this date
 * - endDate: filter logs until this date
 * - search: search in message, route, function, and traceId
 * - sortBy: sort field (timestamp, duration, status) - default: timestamp
 * - sortOrder: asc or desc (default: desc)
 * - hasError: filter for logs with errors (true/false)
 * - minDuration: minimum response duration in ms
 * - maxDuration: maximum response duration in ms
 */
router.get('/api/logs', async (req, res) => {
  let client;
  try {
    const {
      limit = 100,
      page = 1,
      level,
      type,
      method,
      route,
      function: functionName,
      status,
      user_id,
      device_id,
      source,
      traceId,
      startDate,
      endDate,
      search,
      sortBy = 'timestamp',
      sortOrder = 'desc',
      hasError,
      minDuration,
      maxDuration
    } = req.query;

    // Validate and limit the limit parameter
    const limitNum = Math.min(parseInt(limit) || 100, 1000);
    const pageNum = parseInt(page) || 1;

    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const db = client.db("test");
    const collection = db.collection("applogs");

    // Build comprehensive query filter
    const filter = {};
    
    if (level) {
      if (Array.isArray(level)) {
        filter.level = { $in: level };
      } else {
        filter.level = level;
      }
    }
    
    if (type) {
      if (Array.isArray(type)) {
        filter.type = { $in: type };
      } else {
        filter.type = type;
      }
    }
    
    if (method) {
      if (Array.isArray(method)) {
        filter['meta.method'] = { $in: method };
      } else {
        filter['meta.method'] = method;
      }
    }
    
    if (route) {
      filter['meta.route'] = { $regex: route, $options: 'i' };
    }
    
    if (functionName) {
      filter['meta.function'] = { $regex: functionName, $options: 'i' };
    }
    
    if (status) {
      if (Array.isArray(status)) {
        filter['meta.status'] = { $in: status.map(s => parseInt(s)) };
      } else {
        filter['meta.status'] = parseInt(status);
      }
    }
    
    if (user_id) {
      filter['meta.user_id'] = user_id;
    }
    
    if (device_id) {
      filter['meta.device_id'] = device_id;
    }
    
    if (source) {
      if (Array.isArray(source)) {
        filter['meta.source'] = { $in: source };
      } else {
        filter['meta.source'] = source;
      }
    }
    
    if (traceId) {
      filter['meta.traceId'] = { $regex: traceId, $options: 'i' };
    }
    
    // Date range filter
    if (startDate || endDate) {
      filter.timestamp = {};
      if (startDate) filter.timestamp.$gte = new Date(startDate);
      if (endDate) filter.timestamp.$lte = new Date(endDate);
    }
    
    // Duration filter
    if (minDuration || maxDuration) {
      filter['meta.duration'] = {};
      if (minDuration) filter['meta.duration'].$gte = parseInt(minDuration);
      if (maxDuration) filter['meta.duration'].$lte = parseInt(maxDuration);
    }
    
    // Error filter
    if (hasError === 'true') {
      filter.$or = [
        { level: 'error' },
        { 'meta.status': { $gte: 400 } },
        { 'meta.error': { $exists: true } }
      ];
    }
    
    // Search filter
    if (search) {
      filter.$or = [
        { message: { $regex: search, $options: 'i' } },
        { route: { $regex: search, $options: 'i' } },
        { function: { $regex: search, $options: 'i' } },
        { 'meta.traceId': { $regex: search, $options: 'i' } },
        { 'meta.spanId': { $regex: search, $options: 'i' } },
        { 'meta.route': { $regex: search, $options: 'i' } },
        { 'meta.function': { $regex: search, $options: 'i' } }
      ];
    }

    const skip = (pageNum - 1) * limitNum;
    
    // Build sort object
    const sortObj = {};
    sortObj[sortBy] = sortOrder === 'asc' ? 1 : -1;
    
    // Get logs with pagination and sorting
    const logs = await collection
      .find(filter)
      .sort(sortObj)
      .skip(skip)
      .limit(limitNum)
      .toArray();

    // Get total count for pagination
    const totalCount = await collection.countDocuments(filter);

    // Get filter counts for frontend
    const filterCounts = await collection
      .aggregate([
        { $match: filter },
        {
          $group: {
            _id: null,
            levels: { $push: "$level" },
            types: { $push: "$type" },
            methods: { $push: "$meta.method" },
            sources: { $push: "$meta.source" },
            statusCodes: { $push: "$meta.status" }
          }
        }
      ])
      .toArray();

    res.json({
      success: true,
      data: {
        logs,
        pagination: {
          currentPage: pageNum,
          totalPages: Math.ceil(totalCount / limitNum),
          totalCount,
          limit: limitNum,
          hasNext: skip + limitNum < totalCount,
          hasPrev: pageNum > 1
        },
        filters: {
          applied: {
            level, type, method, route, functionName, status,
            user_id, device_id, source, traceId, startDate, endDate,
            search, hasError, minDuration, maxDuration
          },
          available: filterCounts[0] || {}
        }
      }
    });

  } catch (error) {
    console.error("Error fetching logs:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to fetch logs",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

/**
 * GET /api/logs/stats - Get comprehensive log statistics
 */
// Replace the existing GET /api/logs/stats endpoint with this:
router.get('/api/logs/stats', async (req, res) => {
  let client;
  try {
    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const db = client.db("test");
    const collection = db.collection("applogs");

    // Get total count
    const totalCount = await collection.countDocuments({});

    // Get counts by level
    const levelStats = await collection
      .aggregate([
        {
          $group: {
            _id: "$level",
            count: { $sum: 1 }
          }
        }
      ])
      .toArray();

    // Get unique users count
    const uniqueUsers = await collection
      .aggregate([
        {
          $group: {
            _id: "$meta.user_id"
          }
        },
        {
          $count: "uniqueUsers"
        }
      ])
      .toArray();

    // Format stats for frontend
    const stats = {
      totalLogs: totalCount,
      errorCount: levelStats.find(s => s._id === 'error')?.count || 0,
      warnCount: levelStats.find(s => s._id === 'warn')?.count || 0,
      infoCount: levelStats.find(s => s._id === 'info')?.count || 0,
      uniqueUsers: uniqueUsers[0]?.uniqueUsers || 0
    };

    res.json({
      success: true,
      data: stats
    });

  } catch (error) {
    console.error("Error fetching log stats:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to fetch log statistics",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

/**
 * GET /api/logs/filters - Get available filter options
 */
router.get('/api/logs/filters', async (req, res) => {
  let client;
  try {
    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const db = client.db("test");
    const collection = db.collection("applogs");

    const filterOptions = await collection
      .aggregate([
        {
          $group: {
            _id: null,
            levels: { $addToSet: "$level" },
            types: { $addToSet: "$type" },
            methods: { $addToSet: "$meta.method" },
            sources: { $addToSet: "$meta.source" },
            routes: { $addToSet: "$meta.route" },
            functions: { $addToSet: "$meta.function" },
            traces : { $addToSet : "$meta.traceId"}
          }
        }
      ])
      .toArray();

    res.json({
      success: true,
      data: {
        levels: (filterOptions[0]?.levels || []).filter(Boolean).sort(),
        types: (filterOptions[0]?.types || []).filter(Boolean).sort(),
        methods: (filterOptions[0]?.methods || []).filter(Boolean).sort(),
        sources: (filterOptions[0]?.sources || []).filter(Boolean).sort(),
        routes: (filterOptions[0]?.routes || []).filter(Boolean).sort().slice(0, 50), // Limit for performance
        functions: (filterOptions[0]?.functions || []).filter(Boolean).sort().slice(0, 50)
      }
    });

  } catch (error) {
    console.error("Error fetching filter options:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to fetch filter options",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

/**
 * DELETE /api/logs - Clear logs (with optional filters)
 */
router.delete('/api/logs', async (req, res) => {
  let client;
  try {
    const { level, type, olderThan, user_id, source } = req.body;

    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const filter = {};
    
    if (level) filter.level = level;
    if (type) filter.type = type;
    if (user_id) filter['meta.user_id'] = user_id;
    if (source) filter['meta.source'] = source;
    if (olderThan) filter.timestamp = { $lt: new Date(olderThan) };

    const result = await client
      .db("test")
      .collection("applogs")
      .deleteMany(filter);

    res.json({
      success: true,
      data: {
        deletedCount: result.deletedCount,
        message: `Successfully deleted ${result.deletedCount} log entries`,
        filter: filter
      }
    });

  } catch (error) {
    console.error("Error deleting logs:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to delete logs",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

/**
 * GET /api/logs/export - Export logs with all filters
 */
router.get('/api/logs/export', async (req, res) => {
  let client;
  try {
    const {
      format = 'json',
      level, type, method, route, function: functionName,
      status, user_id, device_id, source, traceId,
      startDate, endDate, search, limit = 5000
    } = req.query;

    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    // Build the same filter as the main logs endpoint
    const filter = {};
    if (level) filter.level = level;
    if (type) filter.type = type;
    if (method) filter['meta.method'] = method;
    if (route) filter['meta.route'] = { $regex: route, $options: 'i' };
    if (functionName) filter['meta.function'] = { $regex: functionName, $options: 'i' };
    if (status) filter['meta.status'] = parseInt(status);
    if (user_id) filter['meta.user_id'] = user_id;
    if (device_id) filter['meta.device_id'] = device_id;
    if (source) filter['meta.source'] = source;
    if (traceId) filter['meta.traceId'] = { $regex: traceId, $options: 'i' };
    
    if (startDate || endDate) {
      filter.timestamp = {};
      if (startDate) filter.timestamp.$gte = new Date(startDate);
      if (endDate) filter.timestamp.$lte = new Date(endDate);
    }
    
    if (search) {
      filter.$or = [
        { message: { $regex: search, $options: 'i' } },
        { route: { $regex: search, $options: 'i' } },
        { function: { $regex: search, $options: 'i' } },
        { 'meta.traceId': { $regex: search, $options: 'i' } }
      ];
    }

    const logs = await client
      .db("test")
      .collection("applogs")
      .find(filter)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .toArray();

    if (format === 'csv') {
      const csvHeaders = [
        'timestamp', 'level', 'type', 'message', 'route', 'function', 
        'method', 'status', 'duration', 'user_id', 'device_id', 
        'source', 'traceId', 'spanId', 'clientIp'
      ];
      
      const csvRows = logs.map(log => [
        log.timestamp,
        log.level,
        log.type || '',
        log.message?.replace(/"/g, '""') || '',
        log.route || log.meta?.route || '',
        log.function || log.meta?.function || '',
        log.meta?.method || '',
        log.meta?.status || '',
        log.meta?.duration || '',
        log.meta?.user_id || '',
        log.meta?.device_id || '',
        log.meta?.source || '',
        log.meta?.traceId || '',
        log.meta?.spanId || '',
        log.meta?.clientIp || ''
      ]);
      
      const csvContent = [
        csvHeaders.join(','),
        ...csvRows.map(row => row.map(field => `"${field}"`).join(','))
      ].join('\n');

      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="logs_${new Date().toISOString().split('T')[0]}.csv"`);
      res.send(csvContent);
    } else {
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Content-Disposition', `attachment; filename="logs_${new Date().toISOString().split('T')[0]}.json"`);
      res.json({
        success: true,
        exportedAt: new Date(),
        count: logs.length,
        filters: filter,
        data: logs
      });
    }

  } catch (error) {
    console.error("Error exporting logs:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to export logs",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

/**
 * GET /api/logs/:id - Get specific log by ID
 */
router.get('/api/logs/:id', async (req, res) => {
  let client;
  try {
    const { id } = req.params;

    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const log = await client
      .db("test")
      .collection("applogs")
      .findOne({ _id: new ObjectId(id) });

    if (!log) {
      return res.status(404).json({
        success: false,
        error: "Log not found"
      });
    }

    res.json({
      success: true,
      data: log
    });

  } catch (error) {
    console.error("Error fetching log:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to fetch log",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

/**
 * GET /dashboard - Render the dashboard with logs
 */
router.get('/dashboard', async (req, res) => {
  let client;
  try {
    client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const db = client.db("test");
    const collection = db.collection("applogs");

    // Fetch logs for the dashboard (customize the query as needed)
    const logs = await collection
      .find()
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();

    // ...fetch other necessary data for the dashboard...

    res.render('dashboard', { logs, /* otherData */ });

  } catch (error) {
    console.error("Error rendering dashboard:", error);
    res.status(500).json({ 
      success: false,
      error: "Failed to render dashboard",
      message: error.message 
    });
  } finally {
    if (client) {
      await client.close();
    }
  }
});

export default router;