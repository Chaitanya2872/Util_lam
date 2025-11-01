import logger from "../utils/logger.js";
import { MongoClient } from "mongodb";
import { context, trace } from '@opentelemetry/api';

let collection;

export async function initLoggerMiddleware() {
  const client = new MongoClient(process.env.MONGO_URI, { useUnifiedTopology: true });
  await client.connect();
  collection = client.db("test").collection("applogs");
}

// Get current OpenTelemetry trace context
function getTraceInfo() {
  const span = trace.getSpan(context.active());
  if (!span) return {};
  const spanContext = span.spanContext();
  return {
    traceId: spanContext.traceId,
    spanId: spanContext.spanId,
  };
}

// Extract function name from stack trace
function getCurrentFunction(req) {
  const stack = new Error().stack;
  const stackLines = stack.split('\n');
  
  // Look for route handler in stack (usually after middleware calls)
  for (let i = 2; i < Math.min(stackLines.length, 10); i++) {
    const line = stackLines[i];
    if (line.includes('at ') && !line.includes('middleware') && !line.includes('express')) {
      const match = line.match(/at\s+(\w+)/);
      if (match) return match[1];
    }
  }
  
  // Fallback: try to determine from route path
  const routePath = req.route?.path || req.originalUrl;
  if (routePath) {
    const segments = routePath.split('/').filter(s => s && !s.startsWith(':'));
    return segments.length > 0 ? `${req.method.toLowerCase()}${segments.join('_')}` : 'unknown';
  }
  
  return 'unknown';
}

// Sanitize request body (remove sensitive data)
function sanitizeBody(body) {
  if (!body || typeof body !== 'object') return body;
  
  const sensitiveFields = ['password', 'token', 'secret', 'key', 'auth', 'credential'];
  const sanitized = { ...body };
  
  Object.keys(sanitized).forEach(key => {
    if (sensitiveFields.some(field => key.toLowerCase().includes(field))) {
      sanitized[key] = '[REDACTED]';
    }
  });
  
  return sanitized;
}

const logRequests = (req, res, next) => {
  const start = Date.now();
  const { method, originalUrl, body, headers, query, params } = req;
  const userAgent = headers['user-agent'] || '';
  const clientIp = req.ip || req.connection.remoteAddress || 'unknown';
  
  // Get trace information
  const traceInfo = getTraceInfo();
  
  // Get current function name
  const functionName = getCurrentFunction(req);
  
  // Enhanced request log with all required fields
  const requestLog = {
    timestamp: new Date(),
    level: "info",
    type: "request",
    message: `Incoming Request: ${method} ${originalUrl}`,
    route: originalUrl,
    function: functionName,
    requestBody: sanitizeBody(body),
    meta: { 
      method, 
      path: originalUrl,
      route: req.route?.path || originalUrl,
      function: functionName,
      body: sanitizeBody(body),
      query,
      params,
      headers: {
        'user-agent': userAgent,
        'content-type': headers['content-type'],
        'x-source': headers['x-source'] || 'unknown'
      },
      clientIp,
      user_id: req.user?.id || req.userId || null,
      device_id: req.headers['x-device-id'] || null,
      source: headers['x-source'] || 'backend',
      ...traceInfo
    },
  };

  if (collection) {
    collection.insertOne(requestLog).catch(err => {
      console.error("❌ Failed to log request:", err.message);
    });
  }

  // Log response when finished
  res.on("finish", () => {
    const duration = Date.now() - start;
    const responseLog = {
      timestamp: new Date(),
      level: res.statusCode >= 400 ? (res.statusCode >= 500 ? "error" : "warn") : "info",
      type: "response",
      message: `Response: ${method} ${originalUrl} | Status: ${res.statusCode} | Time: ${duration}ms`,
      route: originalUrl,
      function: functionName,
      requestBody: sanitizeBody(body),
      meta: { 
        method, 
        path: originalUrl,
        route: req.route?.path || originalUrl,
        function: functionName,
        status: res.statusCode,
        duration,
        body: sanitizeBody(body),
        query,
        params,
        clientIp,
        user_id: req.user?.id || req.userId || null,
        device_id: req.headers['x-device-id'] || null,
        source: headers['x-source'] || 'backend',
        ...traceInfo
      },
    };

    if (collection) {
      collection.insertOne(responseLog).catch(err => {
        console.error("❌ Failed to log response:", err.message);
      });
    }
  });

  // Log errors
  res.on("error", (error) => {
    const errorLog = {
      timestamp: new Date(),
      level: "error",
      type: "error",
      message: `Request Error: ${method} ${originalUrl} | Error: ${error.message}`,
      route: originalUrl,
      function: functionName,
      requestBody: sanitizeBody(body),
      meta: {
        method,
        path: originalUrl,
        route: req.route?.path || originalUrl,
        function: functionName,
        error: error.message,
        stack: error.stack,
        body: sanitizeBody(body),
        query,
        params,
        clientIp,
        user_id: req.user?.id || req.userId || null,
        device_id: req.headers['x-device-id'] || null,
        source: headers['x-source'] || 'backend',
        ...traceInfo
      }
    };

    if (collection) {
      collection.insertOne(errorLog).catch(err => {
        console.error("❌ Failed to log error:", err.message);
      });
    }
  });

  next();
};

export default logRequests;