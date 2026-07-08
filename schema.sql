PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL,

    -- Device timestamps
    unix_time INTEGER NOT NULL,
    millis INTEGER,

    -- Power
    battery_v REAL,
    battery_pct INTEGER,

    -- Water
    water_temp REAL,
    turbidity REAL,
    tds REAL,

    -- Environment
    air_temp REAL,
    humidity REAL,
    baro REAL,
    alt REAL,
    aqi REAL,
    voc REAL,
    co2 REAL,
    uv REAL,
    air_velocity REAL,
    air_velocity_peak REAL,
    ozone REAL,
    pm1_0 REAL,
    pm2_5 REAL,
    pm10 REAL,
    chamber_temp REAL,

    -- Server receive time
    created_at INTEGER NOT NULL
);

-- Speeds up "get samples for endpoint X ordered by time"
CREATE INDEX IF NOT EXISTS idx_samples_endpoint_time
    ON samples (endpoint, unix_time DESC);

CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL,
    cmd TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, sent, acked
    created_at INTEGER NOT NULL,
    sent_at INTEGER,
    ack_at INTEGER
);

-- Covers all command queries: by endpoint alone, by endpoint+status, and ordering by created_at
CREATE INDEX IF NOT EXISTS idx_commands_endpoint_status
    ON commands (endpoint, status, created_at DESC);

-- Latest sensor health report per device (only one row per endpoint, upserted on each init message)
CREATE TABLE IF NOT EXISTS sensor_health (
    endpoint TEXT PRIMARY KEY,
    unix_time INTEGER NOT NULL,
    temperature INTEGER NOT NULL DEFAULT 0, -- boolean
    turbidity INTEGER NOT NULL DEFAULT 0,
    tds INTEGER NOT NULL DEFAULT 0,
    environmental INTEGER NOT NULL DEFAULT 0,
    ozone INTEGER NOT NULL DEFAULT 0,
    air_velocity INTEGER NOT NULL DEFAULT 0,
    particulate_matter INTEGER NOT NULL DEFAULT 0,
    chamber_temp INTEGER NOT NULL DEFAULT 0,
    uv INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);

-- Static metadata for each device endpoint
CREATE TABLE IF NOT EXISTS device_info (
    endpoint TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    lat REAL,
    lng REAL,
    hidden INTEGER NOT NULL DEFAULT 0
);
