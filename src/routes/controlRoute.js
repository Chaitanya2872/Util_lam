import express from "express";
import { authenticateToken } from "../middlewares/authMiddleware.js";
import { 
  control, 
  slaveRequest, 
  isBaseResponded, 
  isTankResponded 
} from "../services/controlService.js";

const router = express.Router();

router.post("/publish", authenticateToken, control);
router.post('/slave-request', authenticateToken, slaveRequest);

// ✅ Add these missing routes
router.get('/base-responded/:deviceid', authenticateToken, isBaseResponded);
router.get('/tank-responded/:deviceid/:sensorNumber', authenticateToken, isTankResponded);

export default router;