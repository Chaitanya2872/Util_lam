// tracing MUST be the first import
import './utils/tracing.js'; // ✅ OTel must load before anything else

import express from "express";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import cookieParser from "cookie-parser";
import cors from "cors";
import dotenv from "dotenv";
import authRoutes from "./routes/authRoutes.js";
import userRoutes from "./routes/userRoutes.js";
import protectedRoutes from "./routes/protectedRoutes.js";
import controlRoutes from "./routes/controlRoute.js";
import { errorHandler } from "./middlewares/errorMiddleware.js";
import { initLoggerMiddleware } from "./middlewares/loggerMiddleware.js";
import logRequests from "./middlewares/loggerMiddleware.js";
import tankDataRoutes from "./routes/tankDataRoute.js";
import { autoRefreshMiddleware } from "./middlewares/authMiddleware.js";
import setupRoutes from './routes/setupRoutes.js';
import { MongoClient } from "mongodb"; // ✅ Used for /logs route
import traceRoutes from "./routes/traceRoutes.js"; // ✅ Import trace routes
import path from 'path';
import crypto from 'crypto';

dotenv.config();

const app = express();

// Generate nonce for inline scripts (for CSP)
app.use((req, res, next) => {
  res.locals.nonce = crypto.randomBytes(16).toString('base64');
  next();
});

// Security headers with updated CSP for dashboard
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'"], // Allow inline styles
      scriptSrc: ["'self'", (req, res) => `'nonce-${res.locals.nonce}'`], // Allow nonce-based inline scripts
      scriptSrcAttr: ["'self'", "'unsafe-inline'"], // Allow inline event handlers
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'"],
      fontSrc: ["'self'"],
      objectSrc: ["'none'"],
      mediaSrc: ["'self'"],
      frameSrc: ["'none'"],
    },
  },
}));

app.set("trust proxy", 1);

// CORS setup
app.use(
  cors({
    origin: process.env.ALLOWED_ORIGINS?.split(",") || "http://localhost:5000",
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// Middleware stack
app.use(express.json());
app.use(cookieParser());
await initLoggerMiddleware(); // Add this before app.use(logRequests)
app.use(logRequests);
app.use(express.static(path.join(process.cwd(), 'public')));

// Optional rate limiter
// const apiLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 100 });
// app.use("/", apiLimiter);

// App routes
app.use("/", authRoutes);
app.use(autoRefreshMiddleware);
app.use("/", protectedRoutes);
app.use("/", controlRoutes);
app.use("/", userRoutes);
app.use("/", tankDataRoutes);
app.use("/", setupRoutes);
app.use('/', traceRoutes); // or app.use('/traces', traceRoutes);

// ✅ NEW: Serve logs from MongoDB for the dashboard
//test commit at 3:24 pm on 11/4
app.get("/logs", async (req, res) => {
  try {
    const client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    const logs = await client
      .db("test")
      .collection("applogs")
      .find({})
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();

    await client.close();
    res.json(logs);
  } catch (err) {
    console.error("Error fetching logs:", err);
    res.status(500).send("Failed to fetch logs");
  }
});

app.get('/dashboard', async (req, res) => {
  try {
    const client = new MongoClient(process.env.MONGO_URI);
    await client.connect();

    // You can add filters here if needed (e.g., from req.query)
    const logs = await client
      .db("test")
      .collection("applogs")
      .find({})
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();

    await client.close();
    res.render('dashboard', { 
      logs,
      nonce: res.locals.nonce // Pass nonce to template
    });
  } catch (err) {
    console.error("Error rendering dashboard:", err);
    res.status(500).send("Failed to render dashboard");
  }
});

app.set('view engine', 'ejs');
app.set('views', path.join(process.cwd(), 'views')); // or your preferred views folder

// Global error handler
app.use(errorHandler);
export default app;