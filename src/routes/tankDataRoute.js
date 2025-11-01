import express from "express";
import { authenticateToken } from "../middlewares/authMiddleware.js";
import { sensorData, switchStatus } from "../services/tankDataService.js";
import { isBaseResponded, isTankResponded } from "../services/controlService.js";

const router = express.Router();

router.get("/tank-data/latest/:deviceid/:sensorNumber",authenticateToken,sensorData);
router.get("/status/:deviceid/:switchNumber",authenticateToken,switchStatus)
router.get("/:deviceid/isResponded",isBaseResponded)
router.get("/:deviceid/:sensorNumber/isResponded",isTankResponded)

export default router;