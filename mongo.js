'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// mongo.js  —  single shared MongoDB connection
//
// Everything that needs the DB does:
//   const { connectMongo } = require('./mongo');
//   await connectMongo();
//
// connectMongo() is safe to call from many places at once (e.g. several
// parallel requests) — it only ever opens ONE connection, no matter how many
// times or how concurrently it's called.
//
// .env:
//   MONGO_URI = mongodb://localhost:27017/pictor
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const mongoose = require('mongoose');

const MONGO_URI = process.env.MONGO_URI || null;

const log  = (...a) => console.log ('[MONGO]', ...a);
const warn = (...a) => console.warn('[MONGO]', ...a);
const err  = (...a) => console.error('[MONGO]', ...a);

mongoose.set('strictQuery', true);

// Single in-flight connection promise — this is what prevents the race where
// several parallel callers each try to open their own connection.
let connectingPromise = null;

/**
 * connectMongo()
 * Resolves once connected. Resolves to `null` (not an error) if MONGO_URI
 * isn't configured, so callers can treat "Mongo disabled" as a normal case.
 */
function connectMongo() {
    if (!MONGO_URI) {
        warn('MONGO_URI not set — MongoDB is disabled, nothing will be saved.');
        return Promise.resolve(null);
    }

    if (mongoose.connection.readyState === 1) {
        return Promise.resolve(mongoose.connection);
    }

    if (connectingPromise) {
        return connectingPromise;   // someone else is already connecting — wait on that
    }

    connectingPromise = mongoose.connect(MONGO_URI)
        .then((m) => {
            log('✅ MongoDB connected');
            return m.connection;
        })
        .catch((e) => {
            err('❌ MongoDB connection failed:', e.message);
            connectingPromise = null;   // let the next call try again
            throw e;
        });

    return connectingPromise;
}

mongoose.connection.on('disconnected', () => {
    warn('MongoDB disconnected');
    connectingPromise = null;   // allow a fresh connect() next time it's needed
});

module.exports = { mongoose, connectMongo };