'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// event-alarm-model.js  —  schema + model for saved ADAS alarm rows
//
// One document per alarmId (unique). Re-processing the same alarmId updates
// the existing document instead of creating a duplicate.
// ─────────────────────────────────────────────────────────────────────────────

const { mongoose } = require('./mongo');

const eventAlarmSchema = new mongoose.Schema({
    // ── Fields as returned by the upstream recentlyAdasList API ────────────
    alarmId:      { type: String, required: true, unique: true },
    vehicleId:    Number,
    deviceId:     String,
    deviceName:   String,
    group:        String,
    alarmType:    Number,
    alarmName:    String,
    alarmTime:    String,   // kept as upstream's "YYYY-MM-DD HH:mm:ss" string
    mediaType:    mongoose.Schema.Types.Mixed,
    filePath:     mongoose.Schema.Types.Mixed,
    aviPath:      String,
    imagePath:    String,
    deviceType:   String,
    duration:     String,
    driverName:   String,
    speed:        String,
    lon:          String,
    lat:          String,
    detail:       String,
    driverImg:    String,
    maxSimilar:   Number,

    // ── Added by our pipeline ───────────────────────────────────────────────
    sourceUrl:      String,   // full URL the video was downloaded from
    blobUrl:        String,   // Azure Blob URL once uploaded
    blobPath:       String,   // path inside the container
    uploadStatus:   { type: String, enum: ['pending', 'uploaded', 'failed', 'skipped'], default: 'pending' },
    uploadError:    String,   // last error message, if any
    uploadAttempts: { type: Number, default: 0 },
    lastAttemptAt:  Date,

    // Which request produced/last touched this row (for auditing)
    queryContext: {
        ids:         [Number],
        startTime:   String,
        endTime:     String,
        queryType:   String,
        queryParams: [String],
    },
}, { timestamps: true });

// alarmId already has `unique: true` above, which creates the index — no
// need to declare it again with schema.index() (that would just duplicate it).

module.exports = mongoose.models.EventAlarm || mongoose.model('EventAlarm', eventAlarmSchema);