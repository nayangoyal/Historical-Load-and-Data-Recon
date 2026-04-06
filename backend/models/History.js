const mongoose = require('mongoose');

const historySchema = new mongoose.Schema({
  timestamp: { type: Date, default: Date.now },
  fileName:  { type: String, default: 'Unknown' },
  usecases:  [String],
  configs:   mongoose.Schema.Types.Mixed,
  logs:      mongoose.Schema.Types.Mixed,
});

// Always return newest first
historySchema.index({ timestamp: -1 });

module.exports = mongoose.model('History', historySchema);
