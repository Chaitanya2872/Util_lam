// dbconfig.js
import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

// Connect to MongoDB
const connectDB = async () => {
  try {
    const conn = await mongoose.connect(process.env.MONGO_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log(`MongoDB Connected: ${conn.connection.host}`);
  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
};

const actionSchema = new mongoose.Schema({
  device_id: {
    type: String,
    required: [true, "Device ID for action is required"],
  },
  switch_no: {
    type: String,
    enum: ["BM1", "BM2"],
    required: function () {
      return this.device_type === "base";
    }
  },
  set_status: {
    type: String,
    required: [true, "Status value for action is required"],
    enum: ["on", "off"],
  },
  delay: {
    type: Number,
    default: 0,
    min: 0
  }
});

const conditionSchema = new mongoose.Schema({
  device_id: {
    type: String,
    required: [true, "Device ID for condition is required"],
  },
  device_type: {
    type: String,
    required: [true, "Device type for condition is required"],
    enum: ["base", "tank"],
  },
  status: {
    type: String,
    enum: ["on", "off"],
    required: function () {
      return this.device_type === "base";
    },
  },
  switch_no: {
    type: String,
    enum: ["BM1", "BM2"],
    required: function () {
      return this.device_type === "base";
    }
  },
  level: {
    type: Number,
    min: 0,
    max: 100,
    required: function () {
      return this.device_type === "tank";
    },
    validate: {
      validator: function (value) {
        if (this.device_type === "tank") {
          return value >= 0 && value <= 100;
        }
        return true;
      },
      message: "Tank level threshold must be between 0 and 100",
    },
  },
  // NEW: Trigger value (replaces minimum)
  trigger: {
    type: Number,
    min: 0,
    max: 100,
    required: function() {
      return this.device_type === "tank";
    },
    default: function() {
      return this.device_type === "tank" ? 20 : undefined;
    },
    validate: {
      validator: function(value) {
        if (this.device_type === "tank") {
          return value >= 0 && value <= 100;
        }
        return true;
      },
      message: "Trigger value must be between 0 and 100"
    }
  },
  // NEW: Stop value (replaces maximum)
  stop: {
    type: Number,
    min: 0,
    max: 100,
    required: function() {
      return this.device_type === "tank";
    },
    default: function() {
      return this.device_type === "tank" ? 90 : undefined;
    },
    validate: {
      validator: function(value) {
        if (this.device_type === "tank") {
          const triggerValue = this.trigger;
          return value >= 0 && value <= 100 && (!triggerValue || value > triggerValue);
        }
        return true;
      },
      message: "Stop value must be between 0 and 100 and greater than trigger"
    }
  },
  // NEW: Slot field for tank devices
  slot: {
    type: String,
    enum: ["Primary1", "Primary2", "Secondary"],
    required: function() {
      return this.device_type === "tank";
    },
    validate: {
      validator: function(value) {
        if (this.device_type === "tank") {
          return ["Primary1", "Primary2", "Secondary"].includes(value);
        }
        return true;
      },
      message: "Slot must be 'Primary1', 'Primary2', or 'Secondary' for tank devices"
    }
  },
  operator: {
    type: String,
    enum: ["<", ">", "<=", ">=", "=="],
    required: function() {
      return this.device_type === "tank";
    },
    default: "<",
    validate: {
      validator: function(value) {
        if (this.device_type === "tank") {
          return ["<", ">", "<=", ">=", "=="].includes(value);
        }
        return true;
      },
      message: "Valid operator is required for tank devices"
    }
  },
  actions: [actionSchema],
});

const setupSchema = new mongoose.Schema({
  name: {
    type: String,
    required: [true, "Setup name is required"],
  },
  description: String,
  active: {
    type: Boolean,
    default: true,
  },
  condition: {
    type: conditionSchema,
    required: [true, "Condition is required"],
  },
  created_at: {
    type: Date,
    default: Date.now,
  },
  updated_at: {
    type: Date,
    default: Date.now,
  },
  last_triggered: {
    type: Date
  }
});

// Device Schema - Updated with properties for IoT integration
const deviceSchema = new mongoose.Schema(
  {
    device_id: {
      type: String,
      required: [true, "Device ID is required"],
    },
    device_type: {
      type: String,
      required: [true, "Device type is required"],
      enum: ["base", "tank"],
    },
    device_name: {
      type: String,
      required: [true, "Device name is required"],
    },

  switch_no: {
  type: String,
  enum: ["BM1", "BM2"],
  required: function () {
    return this.device_type === "base";
  }
},

    connection_type: {
      type: String,
      required: [true, "Connection type is required"],
      enum: ["wifi", "ble", "without_wifi"],
    },
    ssid: {
      type: String,
      required: function () {
        return this.connection_type === "wifi";
      },
    },
    password: {
      type: String,
      required: function () {
        return this.connection_type === "wifi";
      },
    },
    status: {
      type: String,
      enum: ["on", "off"],
      required: function () {
        return this.device_type === "base";
      },
      default: function () {
        return this.device_type === "base" ? "off" : undefined;
      },
    },
    level: {
      type: Number,
      min: 0,
      max: 100,
      required: function () {
        return this.device_type === "tank";
      },
      default: function () {
        return this.device_type === "tank" ? 0 : undefined;
      },
    },
    parent_device_id: {
      type: String,
      required: function () {
        return this.device_type === "tank"; // Tank models require a parent (base model)
      },
    },
    channel: {
      type: String,
      required: function () {
        return (
          this.device_type === "tank" && this.connection_type === "without_wifi"
        );
      },
    },
    address_l: {
      type: String,
      required: function () {
        return (
          this.device_type === "tank" && this.connection_type === "without_wifi"
        );
      },
    },
    address_h: {
      type: String,
      required: function () {
        return (
          this.device_type === "tank" && this.connection_type === "without_wifi"
        );
      },
    },
    range: {
      type: Number,
      required: function () {
        return (
          this.device_type === "tank" &&
          this.connection_type === "without_wifi"
        );
      },
    },
    capacity: {
      type: Number,
      required: function () {
        return (
          this.device_type === "tank" &&
          this.connection_type === "without_wifi"
        );
      },
    },
    address_h: {
      type: String,
      required: function () {
        return this.device_type === "tank";
      },
    },
    thing_name: {
      type: String, // AWS IoT thing name
      required: function () {
        return this.connection_type === "wifi" || this.device_type === "base";
      },
    },
    online_status: {
      type: Boolean,
      default: false,
    },
    last_updated: {
      type: Date,
      default: Date.now,
    },
    firmware_version: String,
  },
  { timestamps: true }
);

// Space Schema - Updated to include multiple setups
const spaceSchema = new mongoose.Schema({
  space_name: {
    type: String,
    required: [true, "Space name is required"],
  },
  address: {
    type: String,
    required: [true, "Address is required"],
  },
  devices: [deviceSchema],
  setups: [setupSchema], // Changed from single setup to array of setups
  created_at: {
    type: Date,
    default: Date.now,
  },
});

// OTP Record Schema
const otpRecordSchema = new mongoose.Schema({
  otp: {
    type: String,
    required: [true, "OTP is required"],
  },
  created_at: {
    type: Date,
    default: Date.now,
  },
  is_verified: {
    type: Boolean,
    default: false,
  },
});

// User Schema
const userSchema = new mongoose.Schema(
  {
    user_name: {
      type: String,
      required: [true, "User name is required"],
    },
    mobile_number: {
      type: String,
      required: [true, "Mobile number is required"],
      unique: true, // Ensure mobile numbers are unique across users
      validate: {
        validator: function (value) {
          // Add additional validation if needed (e.g., length, format)
          return /^\d+$/.test(value);
        },
        message: (props) => `${props.value} is not a valid mobile number!`,
      },
    },
    spaces: {
      type: [spaceSchema],
      validate: {
        validator: function (spaces) {
          // Check for unique space names within a user's spaces array
          const spaceNames = spaces.map((space) => space.space_name);
          const uniqueSpaceNames = new Set(spaceNames);
          return spaceNames.length === uniqueSpaceNames.size;
        },
        message: "Space names must be unique for each user",
      },
    },
    otp_record: otpRecordSchema,
    isActive: {
      type: Boolean,
      default: true,
    },
  },
  { timestamps: true }
);

// Create models
const User = mongoose.model("User", userSchema);
const Space = mongoose.model("Space", spaceSchema);
const Device = mongoose.model("Device", deviceSchema);

export { connectDB, User, Space, Device };
