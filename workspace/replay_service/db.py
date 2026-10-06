"""SQLite storage layer: schema, per-thread connections, transactional helper."""
import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices(
  device_id   TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  secret      TEXT NOT NULL,           -- per-device HMAC key for audit reports
  params      TEXT NOT NULL DEFAULT '{}',
  created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upload_sessions(
  session_id     TEXT PRIMARY KEY,
  device_id      TEXT NOT NULL REFERENCES devices(device_id),
  batch_id       TEXT NOT NULL,
  total_chunks   INTEGER NOT NULL,
  package_sha256 TEXT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'open',   -- open | sealed
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL,
  UNIQUE(device_id, batch_id, package_sha256)
);

CREATE TABLE IF NOT EXISTS upload_chunks(
  session_id  TEXT NOT NULL REFERENCES upload_sessions(session_id),
  chunk_index INTEGER NOT NULL,
  sha256      TEXT NOT NULL,
  size        INTEGER NOT NULL,
  data        BLOB NOT NULL,
  PRIMARY KEY(session_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS batches(
  device_id      TEXT NOT NULL REFERENCES devices(device_id),
  batch_id       TEXT NOT NULL,
  package_sha256 TEXT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'sealed',  -- sealed | imported
  event_count    INTEGER NOT NULL DEFAULT 0,
  created_at     INTEGER NOT NULL,
  imported_at    INTEGER,
  PRIMARY KEY(device_id, batch_id)
);

CREATE TABLE IF NOT EXISTS events(
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  batch_id  TEXT NOT NULL,
  kind      TEXT NOT NULL,             -- temp | pressure | material
  seq       INTEGER NOT NULL,
  device_ts INTEGER NOT NULL,          -- device clock, ms
  server_ts INTEGER NOT NULL,          -- gateway receipt clock, ms
  value     REAL NOT NULL,
  raw       TEXT NOT NULL,
  UNIQUE(device_id, batch_id, kind, seq)
);

-- Clock-drift correction versions. corrected_ts = device_ts * (a_num/a_den) + (b_num/b_den).
-- Append-only: a version that is referenced must never be mutated or deleted.
CREATE TABLE IF NOT EXISTS correction_versions(
  version_id      TEXT PRIMARY KEY,
  device_id       TEXT NOT NULL REFERENCES devices(device_id),
  a_num INTEGER NOT NULL, a_den INTEGER NOT NULL,
  b_num INTEGER NOT NULL, b_den INTEGER NOT NULL,
  source_batch_id TEXT,
  status          TEXT NOT NULL DEFAULT 'active',  -- active | superseded
  referenced      INTEGER NOT NULL DEFAULT 0,
  note            TEXT,
  created_at      INTEGER NOT NULL,
  UNIQUE(device_id, a_num, a_den, b_num, b_den)    -- identical correction is idempotent
);

CREATE TABLE IF NOT EXISTS timelines(
  timeline_id   TEXT PRIMARY KEY,
  device_id     TEXT NOT NULL,
  version_id    TEXT NOT NULL REFERENCES correction_versions(version_id),
  algo_version  TEXT NOT NULL,
  generation    INTEGER NOT NULL DEFAULT 0,  -- 0 = normal build; recompute bumps it
  input_hash    TEXT NOT NULL,
  timeline_hash TEXT NOT NULL,          -- reproducibility fingerprint
  status        TEXT NOT NULL DEFAULT 'built',
  created_at    INTEGER NOT NULL,
  UNIQUE(version_id, input_hash, algo_version, generation)  -- same inputs+gen is idempotent
);

CREATE TABLE IF NOT EXISTS timeline_points(
  timeline_id  TEXT NOT NULL REFERENCES timelines(timeline_id),
  event_id     INTEGER NOT NULL REFERENCES events(id),
  corrected_ts INTEGER NOT NULL,
  kind         TEXT NOT NULL,
  value        REAL NOT NULL,
  PRIMARY KEY(timeline_id, event_id)
);

CREATE TABLE IF NOT EXISTS findings(
  finding_id TEXT PRIMARY KEY,          -- deterministic hash of content
  timeline_id TEXT NOT NULL REFERENCES timelines(timeline_id),
  device_id  TEXT NOT NULL,
  type       TEXT NOT NULL,             -- exceed | gap | meter_jump
  start_ts   INTEGER NOT NULL,
  end_ts     INTEGER NOT NULL,
  details    TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'open',  -- open | accepted | rejected | returned
  version    INTEGER NOT NULL DEFAULT 0,    -- optimistic-concurrency token
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews(
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  finding_id TEXT NOT NULL REFERENCES findings(finding_id),
  action     TEXT NOT NULL,             -- accept | reject | return | recompute_ok | recompute_failed
  reviewer   TEXT NOT NULL,
  note       TEXT,
  decided_at INTEGER NOT NULL
);
-- Defense in depth: at most one terminal decision (accept/reject) per finding.
CREATE UNIQUE INDEX IF NOT EXISTS one_terminal_decision
  ON reviews(finding_id) WHERE action IN ('accept','reject');

CREATE TABLE IF NOT EXISTS reports(
  report_id  TEXT PRIMARY KEY,
  device_id  TEXT NOT NULL,
  timeline_id TEXT NOT NULL,
  payload    TEXT NOT NULL,             -- canonical JSON that was signed
  signature  TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
"""


class Database:
    def __init__(self, path):
        self.path = path
        self._local = threading.local()
        with self.tx() as conn:
            conn.executescript(SCHEMA)

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    @contextmanager
    def tx(self):
        """Immediate-write transaction; any exception rolls everything back."""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

    def query(self, sql, args=()):
        return self._conn().execute(sql, args).fetchall()

    def one(self, sql, args=()):
        return self._conn().execute(sql, args).fetchone()
