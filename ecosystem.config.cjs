module.exports = {
  apps: [{
    name: 'iotiq-backend-dev',
    script: './src/app.js',  // Or your entry point
    out_file: "./logs/out.log",     // stdout logs
    error_file: "./logs/error.log", // error logs
    log_date_format: "YYYY-MM-DD HH:mm Z",
    instances: 1,
    exec_mode: 'fork',
    watch: true,  // Auto-restart on file changes
    watch_delay: 1000,
    ignore_watch: [
      'node_modules',
      'logs',
      '.git',
      '*.log'
    ],
    env: {
      NODE_ENV: 'development',
      PORT: 3000,
      MONGO_URI: 'mongodb+srv://justravi997:l36RtKaa0ZJpwd8J@admin.wrwjwgp.mongodb.net/',  // Use dev database
      AWS_IOT_ENDPOINT: 'a1r6z29mxc63px-ats.iot.ap-south-1.amazonaws.com',
      AWS_REGION: 'ap-south-1',
      LOG_LEVEL: 'debug'  // Verbose logging for testing
    },
    error_file: './logs/pm2-error.log',
    out_file: './logs/pm2-out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true,
    autorestart: true,
    max_restarts: 10,
    min_uptime: '10s'
  }]
};