# JWT Authentication Server

A secure authentication server built with Express.js and JSON Web Tokens.

## Features

- User registration and authentication
- Access & refresh token mechanism
- Token-based authorization
- Security best practices

## Project Structure

```
project-root/
│
├── src/
│   ├── controllers/          # Handle API logic
│   │    ├── authController.js
│   │    └── userController.js
│   │
│   ├── models/               # Database models
│   │    └── userModel.js
│   │
│   ├── routes/               # Route definitions
│   │    ├── authRoutes.js
│   │    └── userRoutes.js
│   │
│   ├── middlewares/          # Custom middlewares
│   │    ├── authMiddleware.js
│   │    └── errorMiddleware.js
│   │
│   ├── services/             # Business logic
│   │    └── authService.js
│   │
│   ├── utils/                # Helper functions
│   │    └── jwtHelper.js
│   │
│   ├── config/               # Configuration files
│   │    ├── db.js
│   │    └── serverConfig.js
│   │
│   ├── app.js                # Express App
│   └── server.js             # Server Entry Point
│
├── .env                      # Environment variables
├── package.json              # Dependencies
├── .gitignore                # Git ignore file
└── README.md                 # Documentation
```

## Installation

1. Clone the repository
2. Install dependencies:
```bash
npm install
```

3. Create a `.env` file in the root directory (see `.env.example`)
4. Start the server:
```bash
npm start
```

For development with auto-restart:
```bash
npm run dev
```

## API Endpoints

### Auth Routes
- `POST /login` - Authenticate user & get tokens
- `POST /refresh-token` - Refresh access token
- `POST /logout` - Invalidate refresh token

### User Routes
- `POST /register` - Register a new user
- `GET /profile` - Get user profile (protected)

## Security Features

- Helmet for HTTP headers security
- Rate limiting to prevent brute force
- Token expiration
- Secure token storage
- Input validation

## Environment Variables

- `PORT` - Server port (default: 5000)
- `NODE_ENV` - Environment (development/production)
- `ACCESS_TOKEN_SECRET` - Secret for access tokens
- `REFRESH_TOKEN_SECRET` - Secret for refresh tokens
- `ACCESS_TOKEN_EXPIRY` - Access token expiry (default: 59m)
- `REFRESH_TOKEN_EXPIRY` - Refresh token expiry (default: 7d)


# API Documentation

## Authentication

All endpoints are protected and require authentication. Authentication can be provided in one of these ways:
1. Bearer token in Authorization header
2. Access token in cookies
3. Refresh token in cookies or request body

## Space Management APIs

### 1. Get All Spaces

Retrieves all spaces for the authenticated user.

- **URL**: `/spaces`
- **Method**: `GET`
- **Auth Required**: Yes
- **Permissions Required**: None

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "data": [
    {
      "_id": "60d21b4667d0d8992e610c85",
      "space_name": "Home",
      "address": "123 Main St, City",
      "devices": [],
      "created_at": "2023-05-01T12:00:00.000Z"
    },
    {
      "_id": "60d21b4667d0d8992e610c86",
      "space_name": "Office",
      "address": "456 Business Ave, City",
      "devices": [],
      "created_at": "2023-05-02T12:00:00.000Z"
    }
  ]
}
```

#### Error Response:

- **Code**: 500 Internal Server Error
- **Content**:

```json
{
  "success": false,
  "message": "Failed to retrieve spaces"
}
```

### 2. Get Space by ID

Retrieves a specific space by ID.

- **URL**: `/spaces/:spaceId`
- **Method**: `GET`
- **URL Parameters**: `spaceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "data": {
    "_id": "60d21b4667d0d8992e610c85",
    "space_name": "Home",
    "address": "123 Main St, City",
    "devices": [],
    "created_at": "2023-05-01T12:00:00.000Z"
  }
}
```

#### Error Response:

- **Code**: 404 Not Found
- **Content**:

```json
{
  "success": false,
  "message": "Space not found"
}
```

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Space ID is required"
}
```

### 3. Create a New Space

Creates a new space for the authenticated user.

- **URL**: `/spaces`
- **Method**: `POST`
- **Auth Required**: Yes
- **Permissions Required**: None
- **Request Body**:

```json
{
  "space_name": "Home",
  "address": "123 Main St, City"
}
```

#### Success Response:

- **Code**: 201 Created
- **Content Example**:

```json
{
  "success": true,
  "data": {
    "_id": "60d21b4667d0d8992e610c85",
    "space_name": "Home",
    "address": "123 Main St, City",
    "devices": [],
    "created_at": "2023-05-01T12:00:00.000Z"
  },
  "message": "Space created successfully"
}
```

#### Error Response:

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Space name is required"
}
```

- **Code**: 409 Conflict
- **Content**:

```json
{
  "success": false,
  "message": "Space with name 'Home' already exists for this user"
}
```

### 4. Update a Space

Updates an existing space.

- **URL**: `/spaces/:spaceId`
- **Method**: `PUT`
- **URL Parameters**: `spaceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None
- **Request Body**:

```json
{
  "space_name": "Updated Home",
  "address": "Updated Address"
}
```

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "data": {
    "_id": "60d21b4667d0d8992e610c85",
    "space_name": "Updated Home",
    "address": "Updated Address",
    "devices": [],
    "created_at": "2023-05-01T12:00:00.000Z"
  },
  "message": "Space updated successfully"
}
```

#### Error Response:

- **Code**: 404 Not Found
- **Content**:

```json
{
  "success": false,
  "message": "Space not found"
}
```

- **Code**: 409 Conflict
- **Content**:

```json
{
  "success": false,
  "message": "Space with name 'Updated Home' already exists for this user"
}
```

### 5. Delete a Space

Deletes a space by ID.

- **URL**: `/spaces/:spaceId`
- **Method**: `DELETE`
- **URL Parameters**: `spaceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "message": "Space deleted successfully"
}
```

#### Error Response:

- **Code**: 404 Not Found
- **Content**:

```json
{
  "success": false,
  "message": "Space not found"
}
```

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Cannot delete the only space. Users must have at least one space."
}
```

## Device Management APIs

### 1. Get All Devices in a Space

Retrieves all devices in a specific space.

- **URL**: `/spaces/:spaceId/devices`
- **Method**: `GET`
- **URL Parameters**: `spaceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "data": [
    {
      "_id": "60d21b4667d0d8992e610c87",
      "device_id": "device123",
      "device_type": "base",
      "device_name": "Living Room Base",
      "connection_type": "wifi",
      "ssid": "HomeWiFi",
      "password": "wifi-password"
    },
    {
      "_id": "60d21b4667d0d8992e610c88",
      "device_id": "device456",
      "device_type": "tank",
      "device_name": "Kitchen Tank",
      "connection_type": "without_wifi"
    }
  ]
}
```

#### Error Response:

- **Code**: 404 Not Found
- **Content**:

```json
{
  "success": false,
  "message": "Space not found"
}
```

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Space ID is required"
}
```

### 2. Get a Specific Device

Retrieves a specific device by ID within a space.

- **URL**: `/spaces/:spaceId/devices/:deviceId`
- **Method**: `GET`
- **URL Parameters**: 
  - `spaceId=[string]`
  - `deviceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "data": {
    "_id": "60d21b4667d0d8992e610c87",
    "device_id": "device123",
    "device_type": "base",
    "device_name": "Living Room Base",
    "connection_type": "wifi",
    "ssid": "HomeWiFi",
    "password": "wifi-password"
  }
}
```

#### Error Response:

- **Code**: 404 Not Found
- **Content**:

```json
{
  "success": false,
  "message": "Device not found"
}
```

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Space ID and Device ID are required"
}
```

### 3. Add a New Device to a Space

Adds a new device to a specific space.

- **URL**: `/spaces/:spaceId/devices`
- **Method**: `POST`
- **URL Parameters**: `spaceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None
- **Request Body**:

For WiFi device:
```json
{
  "device_id": "device123",
  "device_type": "base",
  "device_name": "Living Room Base",
  "connection_type": "wifi",
  "ssid": "HomeWiFi",
  "password": "wifi-password"
}
```

For non-WiFi device:
```json
{
  "device_id": "device456",
  "device_type": "tank",
  "device_name": "Kitchen Tank",
  "connection_type": "without_wifi"
}
```

#### Success Response:

- **Code**: 201 Created
- **Content Example**:

```json
{
  "success": true,
  "data": {
    "_id": "60d21b4667d0d8992e610c87",
    "device_id": "device123",
    "device_type": "base",
    "device_name": "Living Room Base",
    "connection_type": "wifi",
    "ssid": "HomeWiFi",
    "password": "wifi-password"
  },
  "message": "Device added successfully"
}
```

#### Error Response:

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Device ID is required"
}
```

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "SSID and password are required for WiFi devices"
}
```

- **Code**: 409 Conflict
- **Content**:

```json
{
  "success": false,
  "message": "Device with ID 'device123' already exists in this space"
}
```

### 4. Delete a Device

Deletes a device from a space.

- **URL**: `/spaces/:spaceId/devices/:deviceId`
- **Method**: `DELETE`
- **URL Parameters**: 
  - `spaceId=[string]`
  - `deviceId=[string]`
- **Auth Required**: Yes
- **Permissions Required**: None

#### Success Response:

- **Code**: 200 OK
- **Content Example**:

```json
{
  "success": true,
  "message": "Device deleted successfully"
}
```

#### Error Response:

- **Code**: 404 Not Found
- **Content**:

```json
{
  "success": false,
  "message": "Device not found"
}
```

- **Code**: 400 Bad Request
- **Content**:

```json
{
  "success": false,
  "message": "Space ID and Device ID are required"
}
```

## Common Error Responses

### Authentication Error

- **Code**: 401 Unauthorized
- **Content**:

```json
{
  "success": false,
  "message": "Authentication required",
  "code": "AUTH_REQUIRED"
}
```

### Server Error

- **Code**: 500 Internal Server Error
- **Content**:

```json
{
  "success": false,
  "message": "Internal server error",
  "code": "SERVER_ERROR"
}
```