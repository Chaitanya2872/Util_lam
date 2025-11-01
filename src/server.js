import app from './app.js';
import logger from './utils/logger.js';
import dotenv from "dotenv";
import { connectDB } from "./config/dbconfig.js";
import { connectPostDB, client } from "./config/postgres.js";


dotenv.config();

// Optional MongoDB
connectDB()
  .then(
    () => console.log("MongoDB connection established successfully")
  )
  .catch((err) => {
    console.error("MongoDB connection error:", err);
    process.exit(1);
  });

/*
connectPostDB()
  .then(() => {
    console.log("Postgres connection established successfully");

    // Attach the client to the Express app
    app.locals.dbClient = client;

    const PORT = process.env.PORT || 5000;
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  })
  .catch((err) => {
    console.error("Postgres connection error:", err);
    process.exit(1);
  });
  */

const PORT = process.env.PORT || 5000;


app.listen(PORT, () => {
  logger.info(`✅ App started on port ${PORT}`);
});
