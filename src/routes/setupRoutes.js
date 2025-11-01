import express from 'express';
import { updateSetupStatus, deleteSetup } from '../services/setupService.js';
import { authenticateToken } from '../middlewares/authMiddleware.js';
import logger from '../utils/logger.js';

const router = express.Router();

// Use authenticateToken for both routes consistently
router.patch('/spaces/:spaceId/setups/:setupId/status', authenticateToken, async (req, res) => {
  try {
    const { spaceId, setupId } = req.params;
    const { active } = req.body;
    const mobileNumber = req.user.mobile_number;

    if (typeof active !== 'boolean') {
      return res.status(400).json({
        success: false,
        message: 'active must be a boolean value'
      });
    }

    const updatedSetup = await updateSetupStatus(mobileNumber, spaceId, setupId, active);

    res.json({
      success: true,
      data: updatedSetup,
      message: `Setup status updated to ${active}`
    });
  } catch (error) {
    logger.error('Setup status update error:', error);
    res.status(400).json({
      success: false,
      message: error.message
    });
  }
});

// Changed from autoRefreshMiddleware to authenticateToken
router.delete('/spaces/:spaceId/setups/:setupId', authenticateToken, async (req, res) => {
  try {
    const { spaceId, setupId } = req.params;
    const mobileNumber = req.user.mobile_number;

    logger.info(`Deleting setup ${setupId} from space ${spaceId}`);
    
    await deleteSetup(mobileNumber, spaceId, setupId);

    res.json({
      success: true,
      message: 'Setup deleted successfully'
    });
  } catch (error) {
    logger.error('Delete setup error:', error);
    res.status(400).json({
      success: false,
      message: error.message
    });
  }
});

export default router;