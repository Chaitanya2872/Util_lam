// src/config/awsIotConfig.js
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ✅ Path to AWS IoT certificates (optional for local testing)
const certPath =
  process.env.IOT_CERT_PATH || path.join(__dirname, "../../certs");

export const AWS_IOT_CONFIG = {
  keyPath:
    process.env.AWS_IOT_KEY_PATH || path.join(certPath, "private.pem.key"),
  certPath:
    process.env.AWS_IOT_CERT_PATH || path.join(certPath, "certificate.pem.crt"),
  caPath:
    process.env.AWS_IOT_CA_PATH || path.join(certPath, "root-CA.crt"),
  clientId:
    process.env.AWS_IOT_CLIENT_ID || "water-management-backend",
  host:
    process.env.AWS_IOT_ENDPOINT ||
    "your-iot-endpoint.iot.region.amazonaws.com",
  region: process.env.AWS_REGION || "ap-south-1",
};

// ✅ Topic prefixes
export const TOPIC_PREFIXES = {
  alive: "$aws/things/",
  health: "$aws/things/",
  update: "$aws/things/",
  otaValidate: "$aws/things/",
  slaveRequest: "mqtt/device/",
  slaveResponse: "$aws/things/",
  config: "mqtt/device/",
  setting: "mqtt/device/",
  control: "mqtt/device/",
  reset: "mqtt/device/",
};

// ✅ Topic suffixes
export const TOPIC_SUFFIXES = {
  alive: "/alive_reply",
  health: "/health_reply",
  update: "/update",
  otaValidate: "/ota/validate",
  slaveRequest: "/slave_request",
  slaveResponse: "/slave_response",
  config: "/config",
  setting: "/setting",
  control: "/control",
  reset: "/reset",
};

// ✅ Helper to build full topic path
export function getTopic(prefix, thingId, suffix) {
  const prefixValue = TOPIC_PREFIXES[prefix];
  const suffixValue = TOPIC_SUFFIXES[suffix];

  if (!prefixValue || !suffixValue) {
    throw new Error(`Invalid prefix or suffix: ${prefix}, ${suffix}`);
  }

  return `${prefixValue}${thingId}${suffixValue}`;
}

// ✅ Default export for convenience
export default {
  AWS_IOT_CONFIG,
  getTopic,
  TOPIC_PREFIXES,
  TOPIC_SUFFIXES,
};
