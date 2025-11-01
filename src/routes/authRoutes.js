// src/routes/authRoutes.js
import express from "express";
import {
  signupInitiate,
  signupVerifyOTP,
  signinInitiate,
  signinVerifyOTP,
  refreshToken,
  logout,
  signinResendOTP,
  signupResendOTP,
  getUser,
} from "../controllers/authController.js";
import { refreshTokenMiddleware, authenticateToken } from "../middlewares/authMiddleware.js";

const router = express.Router();

// Authentication endpoints - Sign In
router.post("/signin", signinInitiate);
router.post("/signin/otp", signinVerifyOTP);
router.post("/signin/resend-otp", signinResendOTP);

// Authentication endpoints - Sign Up
router.post("/signup", signupInitiate);
router.post("/signup/otp", signupVerifyOTP);
router.post("/signup/resend-otp", signupResendOTP);

// Token refresh endpoint
router.post("/refresh-token", refreshTokenMiddleware, refreshToken);

// Logout endpoint
router.post("/logout", logout);

// Get authenticated user details
router.get("/user", authenticateToken, getUser);

export default router;
