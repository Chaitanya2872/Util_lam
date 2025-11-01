// src/utils/mqttHelper.js
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import logger from "./logger.js";

const REGION = process.env.AWS_REGION || "ap-south-1";
const FUNCTION_NAME = process.env.AWS_LAMBDA_FUNCTION_NAME || "SlaveResponseToDB";

// Create reusable Lambda client
const lambdaClient = new LambdaClient({ region: REGION });

/**
 * Publish a message to AWS IoT Core via Lambda.
 * The Lambda function (iotPublishHandler) does the actual IoTDataPlane publish.
 */
export async function publishToIoT(topic, message) {
  try {
    console.log("🔍 Invoking Lambda:", FUNCTION_NAME, "with payload:", { topic, message });

    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify({ mqttTopic: topic, payload: message }),
    });

    const response = await lambdaClient.send(command);
    const result = new TextDecoder().decode(response.Payload);

    console.log("✅ Lambda response:", result);
    return result;
  } catch (error) {
    console.error("❌ Lambda invocation failed:", error);
    throw error;
  }
}


/**
 * Dummy subscribe placeholder — in AWS IoT, this is handled by IoT Rules or Lambda Triggers.
 */
export function subscribe(topic) {
  logger.info(`ℹ️ Subscriptions are handled by AWS IoT Rules. Ignored: ${topic}`);
}

/**
 * Connection initializer for logging only.
 */
export function connectMqtt() {
  logger.info("🌐 Using AWS Lambda + IAM-based IoT communication. No MQTT certs required.");
}
