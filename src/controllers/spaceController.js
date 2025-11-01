import * as spaceService from "../services/spaceService.js";

export const getAllSpaces = async (req, res) => {
  try {
    // The mobile number should be set by the auth middleware
    const { mobile_number } = req.user;

    const spaces = await spaceService.getUserSpaces(mobile_number);

    return res.status(200).json({
      success: true,
      data: spaces,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to retrieve spaces",
    });
  }
};

// Get a specific space by ID
export const getSpaceById = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    const space = await spaceService.getUserSpaceById(mobile_number, spaceId);

    return res.status(200).json({
      success: true,
      data: space,
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (error.message === "User not found") statusCode = 404;
    if (error.message === "Space not found") statusCode = 404;

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Create a new space
export const createSpace = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const spaceData = req.body;
    console.log("Mobile Number:", mobile_number);

    // Validate required fields
    if (!spaceData.space_name) {
      return res.status(400).json({
        success: false,
        message: "Space name is required",
      });
    }

    const newSpace = await spaceService.createSpace(mobile_number, spaceData);

    return res.status(201).json({
      success: true,
      data: newSpace,
      message: "Space created successfully",
    });
  } catch (error) {
    // Determine appropriate status code based on error
    let statusCode = 500;
    if (error.message === "User not found") statusCode = 404;
    if (error.message.includes("already exists")) statusCode = 409; // Conflict

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Update an existing space
export const updateSpace = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;
    const spaceData = req.body;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    const updatedSpace = await spaceService.updateSpace(
      mobile_number,
      spaceId,
      spaceData
    );

    return res.status(200).json({
      success: true,
      data: updatedSpace,
      message: "Space updated successfully",
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
    if (error.message.includes("already exists")) {
      statusCode = 409; // Conflict
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};

// Delete a space
export const deleteSpace = async (req, res) => {
  try {
    const { mobile_number } = req.user;
    const { spaceId } = req.params;

    if (!spaceId) {
      return res.status(400).json({
        success: false,
        message: "Space ID is required",
      });
    }

    const result = await spaceService.deleteSpace(mobile_number, spaceId);

    return res.status(200).json({
      success: true,
      message: result.message,
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
    if (error.message.includes("Cannot delete the only space")) {
      statusCode = 400; // Bad Request
    }

    return res.status(statusCode).json({
      success: false,
      message: error.message,
    });
  }
};
