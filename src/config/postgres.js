import pkg from "pg";
const { Client } = pkg;

const dbConfig = {
  host: process.env.DB_HOST,
  port: 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: { rejectUnauthorized: false },
};

const client = new Client(dbConfig);

async function connectPostDB() {
  try { 
    await client.connect();
    console.log("PostgreSQL connected");
  } catch (error) {
    console.error("Failed to connect to PostgreSQL:", error);
    throw error;
  }
}

async function disconnectDB() {
  try {
    await client.end();
    console.log("PostgreSQL disconnected");
  } catch (error) {
    console.error("Failed to disconnect PostgreSQL:", error);
  }
}

export { client, connectPostDB, disconnectDB };

