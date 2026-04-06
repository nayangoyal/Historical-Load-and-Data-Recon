const mongoose = require('mongoose');

let isConnected = false;

async function connectDB() {
  if (isConnected) return;
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.warn('MONGODB_URI not set — history will use local JSON file fallback.');
    return;
  }
  try {
    await mongoose.connect(uri);
    isConnected = true;
    console.log('MongoDB connected.');
  } catch (err) {
    console.error('MongoDB connection failed:', err.message);
  }
}

function isMongoReady() {
  return isConnected && mongoose.connection.readyState === 1;
}

module.exports = { connectDB, isMongoReady };
