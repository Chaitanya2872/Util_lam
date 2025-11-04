import { User } from "../config/dbconfig.js";
import { publishToIoT } from "../utils/mqttHelper.js";
import { getTopic, AWS_IOT_CONFIG } from "../config/awsIotConfig.js";
import logger from "../utils/logger.js";
import { trace, context } from "@opentelemetry/api";

// ✅ FIXED: Use publishToIoT instead of undefined publish
async function safePublish(topic, message) {
  try {
    await publishToIoT(topic, message);
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

  if (thingName) {
    query.$or.push({ "spaces.devices.thing_name": thingName });
  }

  const existingUser = await User.findOne(query);
  
  if (existingUser) {
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

// ✅ UPDATED: Publish update messages when getting devices
export async function getSpaceDevices(mobileNumber, spaceId) {
  const user = await User.findOne({ mobile_number: mobileNumber });
  if (!user) {
    throw new Error("User not found");
  }

  const space = user.spaces.find((space) => space._id.toString() === spaceId);
  if (!space) {
    throw new Error("Space not found");
  }

  const devicesWithSpaceInfo = space.devices.map((device) => {
    const deviceObj = device.toObject();
    return {
      ...deviceObj,
      space_id: space._id,
      space_name: space.space_name,
      sensor_no: device.device_type === "tank" ? device.slave_name : device.switch_no,
    };
  });

  // 🔥 NEW: Publish update requests for all devices with thing_name
  for (const device of devicesWithSpaceInfo) {
    if (device.thing_name) {
      try {
        const updateTopic = getTopic("update", device.thing_name, "update");
        
        if (device.device_type === "base") {
          // Request update for base device
          const updateMsg = {
            deviceid: device.device_id,
            device: "base",
            switch_no: device.switch_no,
            status: device.status || "off",
            sensor_no: device.switch_no,
            value: device.status === "on" ? "1" : "0",
            request_type: "poll" // Indicate this is a polling request
          };
          await safePublish(updateTopic, updateMsg);
          logger.info(`📤 Published update request for base device: ${device.device_id}`);
        } else if (device.device_type === "tank") {
          // Request update for tank device through parent
          const parentDevice = space.devices.find(d => 
            d.device_id === device.parent_device_id && 
            d.switch_no === device.parent_switch_no
          );
          
          if (parentDevice?.thing_name) {
            const updateMsg = {
              deviceid: device.parent_device_id,
              device: "tank",
              sensor_no: device.slave_name,
              slaveid: device.device_id,
              switch_no: device.parent_switch_no,
              request_type: "poll" // Indicate this is a polling request
            };
            await safePublish(updateTopic, updateMsg);
            logger.info(`📤 Published update request for tank device: ${device.device_id} via ${parentDevice.device_id}`);
          }
        }
      } catch (err) {
        logger.error(`Error publishing update for device ${device.device_id}: ${err.message}`);
        // Continue with other devices even if one fails
      }
    }
  }

  logger.info(`Retrieved ${devicesWithSpaceInfo.length} devices for space ${spaceId}`);
  
  return devicesWithSpaceInfo || [];
}

export async function getAllUserDevices(mobileNumber, userId) {
  const user = await User.findOne({
    mobile_number: mobileNumber,
    _id: userId,
  });

  if (!user) {
    throw new Error("User not found");
  }

  const allDevices = [];

  user.spaces.forEach((space) => {
    if (space.devices && space.devices.length > 0) {
      const devicesWithSpaceInfo = space.devices.map((device) => ({
        ...device.toObject(),
        space_id: space._id,
        space_name: space.space_name,
        sensor_no: device.device_type === "tank" ? device.slave_name : device.switch_no,
      }));

      allDevices.push(...devicesWithSpaceInfo);
    }
  });

  return allDevices;
}

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

  const deviceWithSpaceInfo = {
    ...device.toObject(),
    space_id: space._id,
    space_name: space.space_name,
    sensor_no: device.device_type === "tank" ? device.slave_name : device.switch_no,
  };

  return deviceWithSpaceInfo;
}

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

    if (deviceData.connection_type === "wifi") {
      if (!deviceData.ssid || !deviceData.password) {
        throw new Error("SSID and password are required for WiFi devices");
      }
    }

    if (!deviceData.thing_name && deviceData.device_type === "base") {
      deviceData.thing_name = deviceData.device_id;
    }

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

    const bm1Device = { ...deviceData, switch_no: "BM1", status: "off" };
    const bm2Device = { ...deviceData, switch_no: "BM2", status: "off" };

    user.spaces[spaceIndex].devices.push(bm1Device, bm2Device);
    await user.save();

    if (deviceData.connection_type === "wifi" && deviceData.thing_name) {
      try {
        const configTopic = getTopic("config", deviceData.thing_name, "config");
        const configMsg = {
          deviceid: deviceData.device_id,
          ssid: deviceData.ssid,
          password: deviceData.password,
          mode: 1,
          ota_host: ""
        };
        await safePublish(configTopic, configMsg);

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

      } catch (mqttError) {
        logger.error("❌ MQTT publish error", { error: mqttError.message, traceId: traceInfo.traceId });
      }
    }

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

export async function addTankDevice(mobileNumber, spaceId, switchNo, tankData) {
  try {
    const user = await User.findOne({ mobile_number: mobileNumber });
    if (!user) throw new Error("User not found");

    const space = user.spaces.id(spaceId);
    if (!space) throw new Error("Space not found");

    // Find the base device on this switch
    const baseDevice = space.devices.find(
      (d) => d.device_type === "base" && d.switch_no === switchNo
    );
    if (!baseDevice) throw new Error(`No base device found for switch ${switchNo}`);

    // Tanks connected to this base
    const connectedTanks = space.devices.filter(
      (d) => d.device_type === "tank" && d.parent_device_id === baseDevice.device_id
    );

    const usedSlaveNames = connectedTanks.map((t) => t.slave_name);
    const slaveMapping = {
      BM1: ["TM1", "TM2"],
      BM2: ["TM3", "TM4"],
    };

    if (!slaveMapping[switchNo])
      throw new Error(`Unknown switch number: ${switchNo}`);

    // 🧠 Step 1 — Check if client provided a sensor_no/slave_name
    let requestedSlave = tankData.sensor_no || tankData.slave_name;

    if (requestedSlave) {
      // Validate mapping
      if (!slaveMapping[switchNo].includes(requestedSlave)) {
        throw new Error(
          `Invalid sensor_no '${requestedSlave}' for switch ${switchNo}`
        );
      }
      // Validate availability
      if (usedSlaveNames.includes(requestedSlave)) {
        throw new Error(
          `Sensor_no '${requestedSlave}' already used for switch ${switchNo}`
        );
      }
    } else {
      // 🧠 Step 2 — Auto assign if not given
      const available = slaveMapping[switchNo].filter(
        (s) => !usedSlaveNames.includes(s)
      );
      if (available.length === 0)
        throw new Error(`No available sensors for switch ${switchNo}`);
      requestedSlave = available[0];
      logger.info(
        `Auto-assigned slave_name '${requestedSlave}' for switch ${switchNo}`
      );
    }

    // 🧠 Step 3 — Build tank record
    const newTank = {
      ...tankData,
      device_type: "tank",
      parent_device_id: baseDevice.device_id,
      parent_switch_no: switchNo,
      slave_name: requestedSlave,
      sensor_no: requestedSlave,
      created_at: new Date(),
    };

    // Add and save
    space.devices.push(newTank);
    await user.save();

    // 🧠 Step 4 — Send MQTT add message to base device
    if (baseDevice.thing_name) {
      const topic = getTopic("slaveAdd", baseDevice.thing_name, "slaveAdd");
      const message = {
        deviceid: baseDevice.device_id,
        slaveid: newTank.device_id,
        sensor_no: requestedSlave,
        action: "add",
      };
      await publishToIoT(topic, message);
      logger.info("✅ Published MQTT slave add", { topic, message });
    }

    return {
      success: true,
      message: `Tank device added successfully as ${requestedSlave}`,
      tank: newTank,
    };
  } catch (err) {
    logger.error(`❌ Error adding tank device: ${err.message}`);
    throw err;
  }
}


export async function debugTankDevice(mobileNumber, spaceId, deviceId) {
  const user = await User.findOne({ mobile_number: mobileNumber });
  if (!user) return null;
  
  const space = user.spaces.find(s => s._id.toString() === spaceId);
  if (!space) return null;
  
  const device = space.devices.find(d => d.device_id === deviceId);
  if (!device) return null;
  
  logger.info('🔍 Device in DB:', {
    device_id: device.device_id,
    device_type: device.device_type,
    slave_name: device.slave_name,
    parent_switch_no: device.parent_switch_no,
    all_fields: Object.keys(device.toObject())
  });
  
  return device.toObject();
}

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

  const deviceIndex = user.spaces[fromSpaceIndex].devices.findIndex(
    (device) => device.device_id === deviceId
  );

  if (deviceIndex === -1) {
    throw new Error("Device not found in source space");
  }

  const device = user.spaces[fromSpaceIndex].devices[deviceIndex];

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

  user.spaces[fromSpaceIndex].devices.splice(deviceIndex, 1);
  user.spaces[toSpaceIndex].devices.push(device);

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

export async function deleteDevice(mobileNumber, spaceId, deviceId) {
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

    const space = user.spaces[spaceIndex];
    const deviceIndex = space.devices.findIndex(
      (device) => device.device_id === deviceId
    );
    if (deviceIndex === -1) {
      throw new Error("Device not found");
    }

    const device = space.devices[deviceIndex];

    // --- 🧠 CASE 1: Base device deletion ---
    if (device.device_type === "base") {
      const connectedTanks = space.devices.filter(
        (d) => d.device_type === "tank" && d.parent_device_id === deviceId
      );

      if (connectedTanks.length > 0) {
        throw new Error(
          "Cannot delete base device with connected tank devices. Please remove the tank devices first."
        );
      }
    }

    // --- 🧠 CASE 2: Tank device deletion ---
    if (device.device_type === "tank") {
      const baseDevice = space.devices.find(
        (d) =>
          d.device_id === device.parent_device_id &&
          d.switch_no === device.parent_switch_no &&
          d.device_type === "base"
      );

      if (baseDevice && baseDevice.thing_name) {
        try {
          const topic = getTopic("slaveDelete", baseDevice.thing_name, "slaveDelete");
          const message = {
            deviceid: baseDevice.device_id,
            sensor_no: device.slave_name,
            slaveid: device.device_id,
            action: "delete"
          };

          await publishToIoT(topic, message);
          logger.info("✅ Published MQTT Slave Delete", { topic, message });
        } catch (mqttErr) {
          logger.error(`⚠️ Error publishing MQTT delete for tank ${deviceId}: ${mqttErr.message}`);
        }
      } else {
        logger.warn(
          `⚠️ Skipping MQTT cleanup — base device ${device.parent_device_id} has no thing_name`
        );
      }
    }

    // --- 🧹 Remove from setups if any reference exists ---
    if (space.setups) {
      for (let i = 0; i < space.setups.length; i++) {
        const setup = space.setups[i];

        // Condition match
        if (setup.condition?.device_id === deviceId) {
          space.setups.splice(i, 1);
          i--;
          continue;
        }

        // Action match
        if (setup.condition?.actions) {
          const actionIndex = setup.condition.actions.findIndex(
            (a) => a.device_id === deviceId
          );
          if (actionIndex !== -1) {
            setup.condition.actions.splice(actionIndex, 1);
            if (setup.condition.actions.length === 0) {
              space.setups.splice(i, 1);
              i--;
            }
          }
        }
      }
    }

    // --- 🗑️ Finally remove from DB ---
    space.devices.splice(deviceIndex, 1);
    await user.save();

    logger.info(
      `✅ Deleted device: ${device.device_id} (${device.device_type}) from space ${space.space_name}`
    );

    return {
      success: true,
      message: `Device '${device.device_name}' (${device.device_type}) deleted successfully.`,
      deleted_device_id: device.device_id,
      device_type: device.device_type
    };
  } catch (error) {
    logger.error(`❌ Error deleting device: ${error.message}`);
    throw error;
  }
}


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

    const deviceIndex = user.spaces[spaceIndex].devices.findIndex(
      (device) => device.device_id === deviceId
    );
    if (deviceIndex === -1) {
      throw new Error("Device not found");
    }

    const device = user.spaces[spaceIndex].devices[deviceIndex];

    if (device.device_type !== "base") {
      throw new Error("Only base devices can have their status updated");
    }

    if (!status || !["on", "off"].includes(status)) {
      throw new Error("Status must be 'on' or 'off'");
    }

    user.spaces[spaceIndex].devices[deviceIndex].status = status;
    user.spaces[spaceIndex].devices[deviceIndex].last_updated = new Date();
    await user.save();

    if (device.thing_name) {
      try {
        const controlTopic = getTopic("control", device.thing_name, "control");
        const controlMessage = {
          deviceid: device.device_id,
          switch_no: device.switch_no || "BM1",
          status: status
        };

        await safePublish(controlTopic, controlMessage);
        logger.info("✅ Published MQTT Control Command", {
          topic: controlTopic,
          message: controlMessage
        });

        const updateTopic = getTopic("update", device.thing_name, "update");
        const updateMessage = {
          deviceid: device.device_id,
          device: "base",
          switch_no: device.switch_no || "BM1",
          status: status,
          sensor_no: "TM1",
          value: status === "on" ? "1" : "0"
        };

        await safePublish(updateTopic, updateMessage);
        logger.info("✅ Published MQTT Update", {
          topic: updateTopic,
          message: updateMessage
        });

      } catch (mqttError) {
        logger.error(`❌ MQTT publish error: ${mqttError.message}`);
      }
    }

    return {
      ...device.toObject(),
      status,
      space_id: user.spaces[spaceIndex]._id,
      space_name: user.spaces[spaceIndex].space_name,
      sensor_no: device.switch_no,
    };
  } catch (error) {
    logger.error(`Error updating device status: ${error.message}`);
    throw error;
  }
}

export async function resetDevice(mobileNumber, spaceId, deviceId, slaveNo, slaveId = "") {
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

    const device = user.spaces[spaceIndex].devices.find(
      (d) => d.device_id === deviceId
    );
    if (!device) {
      throw new Error("Device not found");
    }

    const thingName = device.thing_name || device.device_id;
    if (!thingName) {
      throw new Error("Missing thingName or deviceId for MQTT topic");
    }

    const resetTopic = `mqtt/device/${thingName}/reset`;

    const resetMessage = {
      deviceid: device.device_id,
      slave_no: slaveNo,
      slaveid: slaveId
    };

    await publishToIoT(resetTopic, resetMessage);
    logger.info("✅ Published MQTT Reset Command", {
      topic: resetTopic,
      message: resetMessage,
    });

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