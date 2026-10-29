
const Database = require('better-sqlite3');

const db = new Database('reliability.db');

db.exec(`
  CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
`);

const findEvent = db.prepare(
  'SELECT event_id FROM processed_events WHERE event_id = ?'
);

const saveEvent = db.prepare(
  'INSERT INTO processed_events (event_id) VALUES (?)'
);

function isProcessed(eventId) {
  return Boolean(findEvent.get(eventId));
}

function markProcessed(eventId) {
  saveEvent.run(eventId);
}

module.exports = { isProcessed, markProcessed };
