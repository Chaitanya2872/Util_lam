import express from "express";
import { authenticateToken } from "../middlewares/authMiddleware.js";
import { control,slaveRequest } from "../services/controlService.js";

const router = express.Router();

router.post("/publish", authenticateToken, control);
router.post('/slave-request', authenticateToken, slaveRequest);

export default router;