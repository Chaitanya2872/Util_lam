import { User } from "../config/dbconfig.js";
import { publishToIoT } from "../utils/mqttHelper.js";
import { getTopic, AWS_IOT_CONFIG } from "../config/awsIotConfig.js";
import logger from "../utils/logger.js";
import { trace, context } from "@opentelemetry/api";




async function safePublish(topic, message) {
  try {
    publish(topic, message);
    logger.info(`✅ MQTT Published to ${topic}`, message);
  } catch (err) {
    logger.error(`❌ MQTT publish failed for ${topic}: ${err.message}`);
  }
}

// Helper function to check if device exists globally
async function checkDeviceExistsGlobally(deviceId, thingName = null) {
  const query = {
    $or: [
      { "spaces.devices.device_id": deviceId }
    ]
  };

  // If thing_name is provided, also check for it
  if (thingName) {
    query.$or.push({ "spaces.devices.thing_name": thingName });
  }

  const existingUser = await User.findOne(query);
  
  if (existingUser) {
    // Find the specific device and space details
    for (const space of existingUser.spaces) {
      const device = space.devices.find(d => 
        d.device_id === deviceId || (thingName && d.thing_name === thingName)
      );
      if (device) {
        return {
          exists: true,
          user: existingUser,
          space: space,
          device: device
        };
      }
    }
  }
  
  return { exists: false };
}

// Get all devices in a space
export async function getSpaceDevices(mobileNumber, spaceId) {
  const user = await User.findOne({ mobile_number: mobileNumber });
  if (!user) {
    throw new Error("User not found");
  }

  const space = user.spaces.find((space) => space._id.toString() === spaceId);
  if (!space) {
    throw new Error("Space not found");
  }

  // Transform the devices to include space information
  const devicesWithSpaceInfo = space.devices.map((device) => ({
    ...device.toObject(),
    space_id: space._id,
    space_name: space.space_name,
  }));

  return devicesWithSpaceInfo || [];
}

// Get all devices for a user across all spaces
export async function getAllUserDevices(mobileNumber, userId) {
  // Verify that the mobile number matches the requested userId for security
  const user = await User.findOne({
    mobile_number: mobileNumber,
    _id: userId,
  });

  if (!user) {
    throw new Error("User not found");
  }

  // Collect devices from all spaces
  const allDevices = [];

  user.spaces.forEach((space) => {
    if (space.devices && space.devices.length > 0) {
      // Add space information to each device
      const devicesWithSpaceInfo = space.devices.map((device) => ({
        ...device.toObject(),
        space_id: space._id,
        space_name: space.space_name,
      }));

      allDevices.push(...devicesWithSpaceInfo);
    }
  });

  return allDevices;
}

// Get a specific device by ID
export async function getDeviceById(mobileNumber, spaceId, deviceId) {
  const user = await User.findOne({ mobile_number: mobileNumber });
  if (!user) {
    throw new Error("User not found");
  }

  const space = user.spaces.find((space) => space._id.toString() === spaceId);
  if (!space) {
    throw new Error("Space not found");
  }

  const device = space.devices.find((device) => device.device_id === deviceId);
  if (!device) {
    throw new Error("Device not found");
  }

  // Add space information to the device
  const deviceWithSpaceInfo = {
    ...device.toObject(),
    space_id: space._id,
    space_name: space.space_name,
  };

  return deviceWithSpaceInfo;
}

// Check if device can be transferred or needs to be removed first
export async function checkDeviceAvailability(deviceId, thingName = null) {
  const result = await checkDeviceExistsGlobally(deviceId, thingName);
  
  if (result.exists) {
    return {
      available: false,
      currentOwner: {
        mobile_number: result.user.mobile_number,
        user_name: result.user.user_name,
        space_name: result.space.space_name,
        space_id: result.space._id
      },
      device: result.device
    };
  }
  
  return { available: true };
}

// Add a base device to a space
// deviceService.js
export async function addDevice(mobileNumber, spaceId, deviceData) {
  const span = trace.getSpan(context.active());
  const traceInfo = span ? span.spanContext() : {};

  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      logger.error("User not found", { mobileNumber, traceId: traceInfo.traceId });
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(space => space._id.toString() === spaceId);
    if (spaceIndex === -1) {
      logger.error("Space not found", { spaceId, mobileNumber, traceId: traceInfo.traceId });
      throw new Error("Space not found");
    }

    // Check for global duplicate device
    const globalCheck = await checkDeviceExistsGlobally(deviceData.device_id, deviceData.thing_name);
    if (globalCheck.exists) {
      logger.warn("Device already registered", {
        device_id: deviceData.device_id,
        registeredTo: globalCheck.user.mobile_number,
        traceId: traceInfo.traceId
      });

      if (globalCheck.user.mobile_number === mobileNumber) {
        throw new Error(`Device '${deviceData.device_id}' already in your space '${globalCheck.space.space_name}'`);
      } else {
        throw new Error(`Device '${deviceData.device_id}' is registered to another account`);
      }
    }

    // Validate WiFi creds if WiFi mode
    if (deviceData.connection_type === "wifi") {
      if (!deviceData.ssid || !deviceData.password) {
        throw new Error("SSID and password are required for WiFi devices");
      }
    }

    // Set default thing_name if base and not provided
    if (!deviceData.thing_name && deviceData.device_type === "base") {
      deviceData.thing_name = deviceData.device_id;
    }

    // Auto assign switch_no if base device
    if (deviceData.device_type === "base") {
      const currentDevices = user.spaces[spaceIndex].devices;
      const existingSwitches = currentDevices
        .filter(d => d.device_type === "base")
        .map(d => d.switch_no);
      const available = ["BM1", "BM2"].find(sw => !existingSwitches.includes(sw));

      if (!available) {
        throw new Error("Maximum 2 base devices already assigned in this space");
      }

      deviceData.switch_no = available;
      logger.info("Assigned switch_no", {
        switch_no: available,
        device_id: deviceData.device_id,
        traceId: traceInfo.traceId
      });
    }

    // Create BM1 and BM2 clones
    const bm1Device = { ...deviceData, switch_no: "BM1", status: "off" };
    const bm2Device = { ...deviceData, switch_no: "BM2", status: "off" };

    // Push to DB
    user.spaces[spaceIndex].devices.push(bm1Device, bm2Device);
    await user.save();

    // ---------- MQTT PUBLISH LOGIC STARTS HERE ---------- //
    if (deviceData.connection_type === "wifi" && deviceData.thing_name) {
      try {
        // 1️⃣ CONFIG TOPIC
        const configTopic = getTopic("config", deviceData.thing_name, "config");
        const configMsg = {
          deviceid: deviceData.device_id,
          ssid: deviceData.ssid,
          password: deviceData.password,
          mode: 1,
          ota_host: ""
        };
        await safePublish(configTopic, configMsg);
        logger.info("✅ Published MQTT Config", { topic: configTopic, configMsg });

        // 2️⃣ ALIVE_REPLY TOPIC
        const aliveTopic = getTopic("alive", deviceData.thing_name, "alive");
        const aliveMsg = {
          deviceid: deviceData.device_id,
          thingId: deviceData.thing_name,
          ssid: deviceData.ssid,
          password: deviceData.password,
          ipaddress: deviceData.ipaddress || "192.168.0.100",
          macaddress: deviceData.macaddress || "30:C9:22:3A:09:24",
          firmware_version: deviceData.firmware_version || "2.1.0"
        };
        await safePublish(aliveTopic, aliveMsg);
        logger.info("✅ Published MQTT Alive Reply", { topic: aliveTopic, aliveMsg });

        // 3️⃣ HEALTH_REPLY TOPIC
        const healthTopic = getTopic("health", deviceData.thing_name, "health");
        const healthMsg = {
          deviceid: deviceData.device_id,
          heap: "20480",
          rssi: "-45",
          internet_speed: "1mb/s",
          chip_model: "ESP32",
          chip_revision: "3",
          chip_core: "2",
          chip_frequency: "240MHz"
        };
        await safePublish(healthTopic, healthMsg);
        logger.info("✅ Published MQTT Health Reply", { topic: healthTopic, healthMsg });

        // 4️⃣ UPDATE TOPIC (Initial Device Status)
        const updateTopic = getTopic("update", deviceData.thing_name, "update");
        const updateMsg = {
          deviceid: deviceData.device_id,
          device: "base",
          switch_no: deviceData.switch_no || "BM1",
          status: "off",
          sensor_no: "TM1",
          value: "0"
        };
        await safePublish(updateTopic, updateMsg);
        logger.info("✅ Published MQTT Update", { topic: updateTopic, updateMsg });

      } catch (mqttError) {
        logger.error("❌ MQTT publish error", { error: mqttError.message, traceId: traceInfo.traceId });
      }
    }
    // ---------- MQTT PUBLISH LOGIC ENDS HERE ---------- //

    logger.info("Base device added successfully", {
      device_id: deviceData.device_id,
      spaceId,
      traceId: traceInfo.traceId
    });

    return [bm1Device, bm2Device];
  } catch (error) {
    logger.error("Error in addDevice", {
      error: error.message,
      device_id: deviceData.device_id,
      traceId: traceInfo.traceId
    });
    throw error;
  }
}

// Add a tank device to a space and connect to base device
// Updated addTankDevice function in deviceService.js
// Updated addTankDevice function in deviceService.js
// deviceService.js
export async function addTankDevice(
  mobileNumber,
  spaceId,
  baseDeviceId,
  switchNo,
  tankData
) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    // Find base device
    const baseDevice = user.spaces[spaceIndex].devices.find(
      (device) =>
        device.device_id === baseDeviceId &&
        device.device_type === "base" &&
        device.switch_no === switchNo
    );

    if (!baseDevice) {
      throw new Error(
        `Base device with ID '${baseDeviceId}' and switch '${switchNo}' not found`
      );
    }

    // Validate switch_no
    if (!["BM1", "BM2"].includes(switchNo)) {
      throw new Error("Switch number must be either 'BM1' or 'BM2'");
    }

    // Find connected tanks for this base switch
    const connectedTanks = user.spaces[spaceIndex].devices.filter(
      (device) =>
        device.device_type === "tank" &&
        device.parent_device_id === baseDeviceId &&
        device.parent_switch_no === switchNo
    );

    if (connectedTanks.length >= 2) {
      const tankNames = connectedTanks.map((t) => t.device_name).join(", ");
      throw new Error(
        `Base switch '${switchNo}' already has 2 tanks connected (${tankNames}).`
      );
    }

    // Assign available slave name (TM1/TM2)
    const slaveMapping = {
      BM1: ["TM1", "TM2"],
      BM2: ["TM1", "TM2"],
    };
    const usedSlaveNames = connectedTanks.map((tank) => tank.slave_name);
    const availableSlaveNames = slaveMapping[switchNo].filter(
      (s) => !usedSlaveNames.includes(s)
    );

    if (availableSlaveNames.length === 0) {
      throw new Error(`No available slave names for switch ${switchNo}`);
    }

    const autoAssignedSlaveName = availableSlaveNames[0];
    tankData.slave_name = autoAssignedSlaveName;

    logger.info(
      `Auto-assigned slave name: ${autoAssignedSlaveName} for tank: ${tankData.device_id}`
    );

    // Global device check
    const globalCheck = await checkDeviceExistsGlobally(tankData.device_id);
    if (globalCheck.exists) {
      if (globalCheck.user.mobile_number === mobileNumber) {
        throw new Error(
          `Tank device '${tankData.device_id}' already exists in your space '${globalCheck.space.space_name}'.`
        );
      } else {
        throw new Error(
          `Tank device '${tankData.device_id}' is registered to another account.`
        );
      }
    }

    // Set tank properties
    tankData.device_type = "tank";
    tankData.parent_device_id = baseDeviceId;
    tankData.parent_switch_no = switchNo;
    tankData.level = 0;

    // Default connection type if not provided
    if (!tankData.connection_type) {
      tankData.connection_type = "without_wifi";
    }

    // Default fields for non-Wi-Fi tanks
    if (tankData.connection_type === "without_wifi") {
      tankData.channel = tankData.channel || "24";
      tankData.address_l = tankData.address_l || "0x01";
      tankData.address_h = tankData.address_h || "0x01";
      tankData.range = tankData.range || 100;
      tankData.capacity = tankData.capacity || 1000;
    }

    // Save to DB
    user.spaces[spaceIndex].devices.push(tankData);
    await user.save();

    const newTankDevice =
      user.spaces[spaceIndex].devices[
        user.spaces[spaceIndex].devices.length - 1
      ];

    // ---------- MQTT PUBLISH SECTION ---------- //
    if (baseDevice.thing_name) {
      try {
        // ✅ Fixed: baseDevice instead of basedevice
        const slaveRequestTopic = getTopic("slaveRequest", baseDevice.thing_name, "slaveRequest");

        // Build payload based on connection type
        let slaveRequestMessage = {
          deviceid: baseDeviceId,
          sensor_no: autoAssignedSlaveName, // TM1, TM2
          slaveid: tankData.device_id, // Unique tank device id
        };

        if (tankData.connection_type === "without_wifi") {
          // Mode 3 = without Wi-Fi
          slaveRequestMessage = {
            ...slaveRequestMessage,
            mode: 3,
            channel: parseInt(tankData.channel) || 24,
            address_l: tankData.address_l,
            address_h: tankData.address_h,
            range: parseInt(tankData.range) || 100,
            capacity: parseInt(tankData.capacity) || 1000,
          };
        } else if (tankData.connection_type === "wifi") {
          // Mode 1 = Wi-Fi mode
          slaveRequestMessage = {
            ...slaveRequestMessage,
            mode: 1,
            ssid: tankData.ssid || "ABCDE_RCD",
            password: tankData.password || "1234567890",
            range: parseInt(tankData.range) || 100,
            capacity: parseInt(tankData.capacity) || 1000,
          };
        }

        // Publish MQTT message via Lambda
        await publishToIoT(slaveRequestTopic, slaveRequestMessage);

        logger.info("✅ Published MQTT Slave Request via Lambda", {
          topic: slaveRequestTopic,
          message: slaveRequestMessage,
        });

      } catch (mqttError) {
        logger.error(
          `❌ Error sending MQTT slave request: ${mqttError.message}`
        );
        // Don't throw - tank is already saved in DB
      }
    } else {
      logger.warn(`⚠️ Base device ${baseDeviceId} has no thing_name, skipping MQTT publish`);
    }
    // ---------- END MQTT PUBLISH SECTION ---------- //

    logger.info(`✅ Tank device added successfully: ${tankData.device_id}`);

    return newTankDevice;
  } catch (error) {
    logger.error(`❌ Error adding tank device: ${error.message}`);
    throw error;
  }
}

// Transfer device to another space within the same account
export async function transferDevice(mobileNumber, fromSpaceId, toSpaceId, deviceId) {
  const user = await User.findOne({ mobile_number: mobileNumber });
  if (!user) {
    throw new Error("User not found");
  }

  const fromSpaceIndex = user.spaces.findIndex(
    (space) => space._id.toString() === fromSpaceId
  );
  const toSpaceIndex = user.spaces.findIndex(
    (space) => space._id.toString() === toSpaceId
  );

  if (fromSpaceIndex === -1 || toSpaceIndex === -1) {
    throw new Error("Source or destination space not found");
  }

  // Find the device in the source space
  const deviceIndex = user.spaces[fromSpaceIndex].devices.findIndex(
    (device) => device.device_id === deviceId
  );

  if (deviceIndex === -1) {
    throw new Error("Device not found in source space");
  }

  const device = user.spaces[fromSpaceIndex].devices[deviceIndex];

  // If it's a base device, check for connected tank devices
  if (device.device_type === "base") {
    const connectedTanks = user.spaces[fromSpaceIndex].devices.filter(
      (d) => d.device_type === "tank" && d.parent_device_id === deviceId
    );

    if (connectedTanks.length > 0) {
      throw new Error(
        "Cannot transfer base device with connected tank devices. Please transfer or remove the tank devices first."
      );
    }
  }

  // Remove device from source space
  user.spaces[fromSpaceIndex].devices.splice(deviceIndex, 1);

  // Add device to destination space
  user.spaces[toSpaceIndex].devices.push(device);

  // Clean up any setups in the source space that used this device
  if (user.spaces[fromSpaceIndex].setups) {
    for (let i = 0; i < user.spaces[fromSpaceIndex].setups.length; i++) {
      const setup = user.spaces[fromSpaceIndex].setups[i];

      if (setup.condition.device_id === deviceId) {
        user.spaces[fromSpaceIndex].setups.splice(i, 1);
        i--;
        continue;
      }

      if (setup.condition.actions) {
        const actionIndex = setup.condition.actions.findIndex(
          (action) => action.device_id === deviceId
        );

        if (actionIndex !== -1) {
          setup.condition.actions.splice(actionIndex, 1);
          if (setup.condition.actions.length === 0) {
            user.spaces[fromSpaceIndex].setups.splice(i, 1);
            i--;
          }
        }
      }
    }
  }

  await user.save();

  return {
    success: true,
    message: `Device transferred successfully from ${user.spaces[fromSpaceIndex].space_name} to ${user.spaces[toSpaceIndex].space_name}`,
    device: device
  };
}

// Delete a device from a space
export async function deleteDevice(mobileNumber, spaceId, deviceId) {
  const user = await User.findOne({ mobile_number: mobileNumber });
  if (!user) {
    throw new Error("User not found");
  }

  const spaceIndex = user.spaces.findIndex(
    (space) => space._id.toString() === spaceId
  );
  if (spaceIndex === -1) {
    throw new Error("Space not found");
  }

  // Find the device to delete
  const deviceIndex = user.spaces[spaceIndex].devices.findIndex(
    (device) => device.device_id === deviceId
  );
  if (deviceIndex === -1) {
    throw new Error("Device not found");
  }

  const device = user.spaces[spaceIndex].devices[deviceIndex];

  // Check if this is a base device with connected tank devices
  if (device.device_type === "base") {
    const connectedTanks = user.spaces[spaceIndex].devices.filter(
      (d) => d.device_type === "tank" && d.parent_device_id === deviceId
    );

    if (connectedTanks.length > 0) {
      throw new Error(
        "Cannot delete base device with connected tank devices. Please remove the tank devices first."
      );
    }
  }

  // Remove the device
  user.spaces[spaceIndex].devices.splice(deviceIndex, 1);

  // If this device was used in any setups, remove or disable those setups
  if (user.spaces[spaceIndex].setups) {
    // Find setups that use this device as a condition
    for (let i = 0; i < user.spaces[spaceIndex].setups.length; i++) {
      const setup = user.spaces[spaceIndex].setups[i];

      // Check if this device is used as a condition device
      if (setup.condition.device_id === deviceId) {
        // Remove the setup entirely
        user.spaces[spaceIndex].setups.splice(i, 1);
        i--; // Adjust index since we removed an item
        continue;
      }

      // Check if this device is used in actions
      if (setup.condition.actions) {
        const actionIndex = setup.condition.actions.findIndex(
          (action) => action.device_id === deviceId
        );

        if (actionIndex !== -1) {
          // Remove this action
          setup.condition.actions.splice(actionIndex, 1);

          // If no actions remain, remove the entire setup
          if (setup.condition.actions.length === 0) {
            user.spaces[spaceIndex].setups.splice(i, 1);
            i--; // Adjust index since we removed an item
          }
        }
      }
    }
  }

  // Save the updated user document
  await user.save();

  return { success: true, message: "Device deleted successfully" };
}

// Update device status (for base devices)
// deviceService.js
export async function updateDeviceStatus(
  mobileNumber,
  spaceId,
  deviceId,
  status
) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    // Find the device
    const deviceIndex = user.spaces[spaceIndex].devices.findIndex(
      (device) => device.device_id === deviceId
    );
    if (deviceIndex === -1) {
      throw new Error("Device not found");
    }

    const device = user.spaces[spaceIndex].devices[deviceIndex];

    // Only base devices can have their status updated
    if (device.device_type !== "base") {
      throw new Error("Only base devices can have their status updated");
    }

    // Validate status
    if (!status || !["on", "off"].includes(status)) {
      throw new Error("Status must be 'on' or 'off'");
    }

    // Update device status in DB
    user.spaces[spaceIndex].devices[deviceIndex].status = status;
    user.spaces[spaceIndex].devices[deviceIndex].last_updated = new Date();
    await user.save();

    // ---------- MQTT PUBLISH SECTION ---------- //
    if (device.thing_name) {
      try {
        // ✅ Control Topic (spec-compliant)
        const controlTopic = getTopic("control", device.thing_name, "control");
        const controlMessage = {
          deviceid: device.device_id,
          switch_no: device.switch_no || "BM1",
          status: status // "on" / "off"
        };

        await safePublish(controlTopic, controlMessage);
        logger.info("✅ Published MQTT Control Command", {
          topic: controlTopic,
          message: controlMessage
        });

        // ✅ Update Topic ($aws/things/(thingId)/update)
        const updateTopic = getTopic("update", device.thing_name, "update");
        const updateMessage = {
          deviceid: device.device_id,
          device: "base",
          switch_no: device.switch_no || "BM1",
          status: status,
          sensor_no: "TM1", // Optional: update related sensor
          value: status === "on" ? "1" : "0"
        };

        await safePublish(updateTopic, updateMessage);
        logger.info("✅ Published MQTT Update", {
          topic: updateTopic,
          message: updateMessage
        });

      } catch (mqttError) {
        logger.error(`❌ MQTT publish error: ${mqttError.message}`);
        // Continue DB update even if MQTT fails
      }
    }
    // ---------- END MQTT PUBLISH SECTION ---------- //

    // Return updated device info
    return {
      ...device.toObject(),
      status,
      space_id: user.spaces[spaceIndex]._id,
      space_name: user.spaces[spaceIndex].space_name,
    };
  } catch (error) {
    logger.error(`Error updating device status: ${error.message}`);
    throw error;
  }
}


// deviceService.js
export async function resetDevice(mobileNumber, spaceId, deviceId, slaveNo, slaveId = "") {
  try {
    // Fetch user
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) {
      throw new Error("User not found");
    }

    // Find the space
    const spaceIndex = user.spaces.findIndex(
      (space) => space._id.toString() === spaceId
    );
    if (spaceIndex === -1) {
      throw new Error("Space not found");
    }

    // Find the base or slave device
    const device = user.spaces[spaceIndex].devices.find(
      (d) => d.device_id === deviceId
    );
    if (!device) {
      throw new Error("Device not found");
    }

    // Determine thing_name (required for MQTT topic)
    const thingName = device.thing_name || device.device_id;
    if (!thingName) {
      throw new Error("Missing thingName or deviceId for MQTT topic");
    }

    // ✅ Construct MQTT topic
    const resetTopic = `mqtt/device/${thingName}/reset`;

    // ✅ Construct MQTT payload
    const resetMessage = {
      deviceid: device.device_id,
      slave_no: slaveNo,   // e.g., "TM1"
      slaveid: slaveId     // e.g., "IOTIQTM1_A1024001" or empty to reset whole device
    };

    // ✅ Publish MQTT reset command
    publish(resetTopic, resetMessage);
    logger.info("✅ Published MQTT Reset Command", {
      topic: resetTopic,
      message: resetMessage,
    });

    // Optionally, you can mark the device as pending reset in DB
    device.reset_requested = true;
    device.last_updated = new Date();
    await user.save();

    return {
      success: true,
      message: `Reset command sent to device ${deviceId} (${slaveNo})`,
      topic: resetTopic,
      payload: resetMessage,
    };
  } catch (error) {
    logger.error(`❌ Error in resetDevice: ${error.message}`);
    throw error;
  }
}
