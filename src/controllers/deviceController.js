import * as deviceService from "../services/deviceService.js";

// Get all devices in a space
export const getAllDevices = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;
    const { type } = req.query;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    const devices = await deviceService.getSpaceDevices(mobile_number, spaceId);

    const filteredDevices = type
      ? devices.filter((device) => device.device_type === type)
      : devices;

    return res.status(200).json({
      success: true,
      data: filteredDevices
    });

  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found"
    ) {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message || "Failed to retrieve devices",
    });
  }
};


// Get a specific device by ID
export const getDeviceById = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, deviceId } = req.params;

    if (!spaceId || !deviceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Device ID are required",
      });
    }

    const device = await deviceService.getDeviceById(
      mobile_number,
      spaceId,
      deviceId
    );

    return res.status(200).json({
      success: true,
      data: device,
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Device not found"
    ) {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Get all devices for a user
export const getAllUserDevices = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { userId } = req.params;

    if (!userId) {
      return res.status(400).json({
        success: false,
        message: "User ID is required",
      });
    }

    const devices = await deviceService.getAllUserDevices(
      mobile_number,
      userId
    );

    return res.status(200).json({
      success: true,
      data: devices,
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (error.message === "User not found") {
      statusCode = 404;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Check device availability before adding
export const checkDeviceAvailability = async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { thingName } = req.query; // Optional thing name check

    if (!deviceId) {
      return res.status(400).json({
        success: false,
        message: "Device ID is required",
      });
    }

    const availability = await deviceService.checkDeviceAvailability(
      deviceId,
      thingName
    );

    if (availability.available) {
      return res.status(200).json({
        success: true,
        available: true,
        message: "Device is available for registration",
      });
    } else {
      return res.status(409).json({
        success: false,
        available: false,
        message: `Device is already registered to another ${
          availability.currentOwner.mobile_number === req.user.mobile_number
            ? "space"
            : "account"
        }`,
        currentOwner: availability.currentOwner,
        device: availability.device,
      });
    }
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to check device availability",
    });
  }
};

// Add a base device to a space
export const addDevice = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;
    const deviceData = req.body;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    // Validate required fields
    if (!deviceData.device_id) {
      return res.status(400).json({
        success: false,
        message: "Device ID is required",
      });
    }

    if (!deviceData.device_name) {
      return res.status(400).json({
        success: false,
        message: "Device name is required",
      });
    }

    if (!deviceData.device_type) {
      return res.status(400).json({
        success: false,
        message: "Device type is required",
      });
    }

    if (!deviceData.connection_type) {
      return res.status(400).json({
        success: false,
        message: "Connection type is required",
      });
    }



 const newDevices = await deviceService.addDevice(mobile_number, spaceId, deviceData);

return res.status(201).json({
  success: true,
  data: newDevices,
  message: "Device added successfully"
});


  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found"
    ) {
      statusCode = 404;
    }
    if (error.message.includes("already exists") || 
        error.message.includes("already registered")) {
      statusCode = 409; // Conflict
    }
    if (error.message.includes("required")) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Transfer device between spaces within the same account
export const transferDevice = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { fromSpaceId, toSpaceId, deviceId } = req.params;

    if (!fromSpaceId || !toSpaceId || !deviceId) {
      return res.status(400).json({
        success: false,
        message: "Source space ID, destination space ID, and device ID are required",
      });
    }

    if (fromSpaceId === toSpaceId) {
      return res.status(400).json({
        success: false,
        message: "Source and destination spaces cannot be the same",
      });
    }

    const result = await deviceService.transferDevice(
      mobile_number,
      fromSpaceId,
      toSpaceId,
      deviceId
    );

    return res.status(200).json({
      success: true,
      data: result.device,
      message: result.message,
    });
  } catch (error) {
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message.includes("space not found") ||
      error.message === "Device not found in source space"
    ) {
      statusCode = 404;
    }
    if (error.message.includes("Cannot transfer")) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Delete a device
export const deleteDevice = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, deviceId } = req.params;

    if (!spaceId || !deviceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Device ID are required",
      });
    }

    const result = await deviceService.deleteDevice(
      mobile_number,
      spaceId,
      deviceId
    );

    return res.status(200).json({
      success: true,
      message: result.message,
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Device not found"
    ) {
      statusCode = 404;
    }
    if (error.message.includes("Cannot delete")) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Add a tank device to a space
// Updated addTankDevice function in deviceController.js
export const addTankDevice = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, baseDeviceId, switchNo } = req.params; // Add switchNo parameter
    const tankData = req.body;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    if (!baseDeviceId) {
      return res.status(400).json({
        success: false,
        message: "Base device ID is required",
      });
    }

    if (!switchNo) {
      return res.status(400).json({
        success: false,
        message: "Switch number is required (BM1 or BM2)",
      });
    }

    // Validate switch number
    if (!["BM1", "BM2"].includes(switchNo)) {
      return res.status(400).json({
        success: false,
        message: "Switch number must be either 'BM1' or 'BM2'",
      });
    }

    // Validate required fields
    if (!tankData.device_id) {
      return res.status(400).json({
        success: false,
        message: "Device ID is required",
      });
    }

    if (!tankData.device_name) {
      return res.status(400).json({
        success: false,
        message: "Device name is required",
      });
    }

    const newDevice = await deviceService.addTankDevice(
      mobile_number,
      spaceId,
      baseDeviceId,
      switchNo, // Pass switch number
      tankData
    );

    return res.status(201).json({
      success: true,
      data: newDevice,
      message: `Tank device added successfully to switch ${switchNo}`,
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message.includes("Base device") && error.message.includes("not found")
    ) {
      statusCode = 404;
    }
    if (error.message.includes("already exists") || 
        error.message.includes("already registered") ||
        error.message.includes("already has")) {
      statusCode = 409; // Conflict
    }
    if (error.message.includes("required") || 
        error.message.includes("must be")) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Add a new function to get switch capacity for a base device
export const getBaseSwitchCapacity = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, baseDeviceId, switchNo } = req.params;

    if (!spaceId || !baseDeviceId || !switchNo) {
      return res.status(400).json({
        success: false,
        message: "Space ID, Base Device ID, and Switch Number are required",
      });
    }

    // Validate switch number
    if (!["BM1", "BM2"].includes(switchNo)) {
      return res.status(400).json({
        success: false,
        message: "Switch number must be either 'BM1' or 'BM2'",
      });
    }

    const devices = await deviceService.getSpaceDevices(mobile_number, spaceId);
    
    // Find the specific base device switch
    const baseDevice = devices.find(d => 
      d.device_id === baseDeviceId && 
      d.device_type === "base" && 
      d.switch_no === switchNo
    );
    
    if (!baseDevice) {
      return res.status(404).json({
        success: false,
        message: `Base device switch ${switchNo} not found`,
      });
    }

    // Count connected tanks for this specific switch
    const connectedTanks = devices.filter(d => 
      d.device_type === "tank" && 
      d.parent_device_id === baseDeviceId &&
      d.parent_switch_no === switchNo
    );

    // Determine available slave names based on switch
    const slaveMapping = {
      "BM1": ["TM1", "TM2"],
      "BM2": ["TM3", "TM4"]
    };

    const usedSlaveNames = connectedTanks.map(tank => tank.slave_name);
    const availableSlaveNames = slaveMapping[switchNo].filter(
      slaveName => !usedSlaveNames.includes(slaveName)
    );

    return res.status(200).json({
      success: true,
      data: {
        base_device: baseDevice,
        switch_no: switchNo,
        connected_tanks: connectedTanks.length,
        max_capacity: 2, // Each switch can handle 2 tanks
        available_slots: 2 - connectedTanks.length,
        available_slave_names: availableSlaveNames,
        tank_details: connectedTanks.map(tank => ({
          device_id: tank.device_id,
          device_name: tank.device_name,
          slave_name: tank.slave_name
        }))
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to get switch capacity",
    });
  }
};

// Update device status (for base devices)
export const updateDeviceStatus = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, deviceId } = req.params;
    const { status } = req.body;

    if (!spaceId || !deviceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Device ID are required",
      });
    }

    if (!status) {
      return res.status(400).json({
        success: false,
        message: "Status is required",
      });
    }

    const updatedDevice = await deviceService.updateDeviceStatus(
      mobile_number,
      spaceId,
      deviceId,
      status
    );

    return res.status(200).json({
      success: true,
      data: updatedDevice,
      message: `Device turned ${status} successfully`,
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (
      error.message === "User not found" ||
      error.message === "Space not found" ||
      error.message === "Device not found"
    ) {
      statusCode = 404;
    }
    if (
      error.message.includes("Only base devices") ||
      error.message.includes("Status must be")
    ) {
      statusCode = 400;
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Add this new function to deviceController.js
export const getBaseDeviceCapacity = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId, baseDeviceId } = req.params;

    if (!spaceId || !baseDeviceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID and Base Device ID are required",
      });
    }

    const devices = await deviceService.getSpaceDevices(mobile_number, spaceId);
    
    // Find the base device
    const baseDevice = devices.find(d => d.device_id === baseDeviceId && d.device_type === "base");
    if (!baseDevice) {
      return res.status(404).json({
        success: false,
        message: "Base device not found",
      });
    }

    // Count connected tanks
    const connectedTanks = devices.filter(d => 
      d.device_type === "tank" && d.parent_device_id === baseDeviceId
    );

    return res.status(200).json({
      success: true,
      data: {
        base_device: baseDevice,
        connected_tanks: connectedTanks.length,
        max_capacity: 4,
        available_slots: 4 - connectedTanks.length,
        tank_details: connectedTanks.map(tank => ({
          device_id: tank.device_id,
          device_name: tank.device_name,
          slave_name: tank.slave_name
        }))
      }
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to get base device capacity",
    });
  }
};