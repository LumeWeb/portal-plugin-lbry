-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS lbry_streams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_hash TEXT NOT NULL UNIQUE,
    sd_hash TEXT NOT NULL UNIQUE,
    stream_name TEXT,
    stream_type TEXT DEFAULT 'lbryfile',
    suggested_file_name TEXT,
    key_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS lbry_blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL UNIQUE,
    blob_size INTEGER NOT NULL,
    iv_data BLOB,
    terminating BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS lbry_stream_blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id INTEGER NOT NULL,
    blob_id INTEGER NOT NULL,
    blob_number INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    FOREIGN KEY (blob_id) REFERENCES lbry_blobs(id) ON DELETE CASCADE,
    UNIQUE(stream_id, blob_id)
);

CREATE TABLE IF NOT EXISTS lbry_stream_pins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    UNIQUE(user_id, stream_id)
);

CREATE TABLE IF NOT EXISTS lbry_devices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    ip_address TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_lbry_devices_user_id ON lbry_devices(user_id);

CREATE TABLE IF NOT EXISTS lbry_pending_blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    device_id INTEGER,
    stream_id INTEGER,
    blob_size INTEGER,
    blob_number INTEGER NOT NULL DEFAULT 0,
    received BOOLEAN NOT NULL DEFAULT FALSE,
    terminating BOOLEAN NOT NULL DEFAULT FALSE,
    iv_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL,
    
    UNIQUE(user_id, blob_hash),
    UNIQUE(user_id, stream_id, blob_number),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id),
    FOREIGN KEY (stream_id) REFERENCES lbry_pending_streams(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_lbry_pending_blobs_user_id ON lbry_pending_blobs(user_id);
CREATE INDEX IF NOT EXISTS idx_lbry_pending_blobs_device_id ON lbry_pending_blobs(device_id);
CREATE INDEX IF NOT EXISTS idx_lbry_pending_blobs_stream_id ON lbry_pending_blobs(stream_id);

CREATE TABLE IF NOT EXISTS lbry_pending_streams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_hash TEXT NOT NULL,
    sd_hash TEXT NOT NULL,
    stream_name TEXT,
    stream_type TEXT DEFAULT 'lbryfile',
    suggested_file_name TEXT,
    key_data BLOB,
    total_blobs INTEGER NOT NULL DEFAULT 0,
    user_id INTEGER NOT NULL,
    device_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id),
    UNIQUE(user_id, stream_hash),
    UNIQUE(user_id, sd_hash)
);

CREATE INDEX IF NOT EXISTS idx_lbry_pending_streams_user_id ON lbry_pending_streams(user_id);
CREATE INDEX IF NOT EXISTS idx_lbry_pending_streams_device_id ON lbry_pending_streams(device_id);
CREATE INDEX IF NOT EXISTS idx_lbry_pending_streams_sd_hash ON lbry_pending_streams(sd_hash);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS lbry_pending_blobs;
DROP TABLE IF EXISTS lbry_pending_streams;
DROP TABLE IF EXISTS lbry_stream_pins;
DROP TABLE IF EXISTS lbry_stream_blobs;
DROP TABLE IF EXISTS lbry_blobs;
DROP TABLE IF EXISTS lbry_streams;
DROP TABLE IF EXISTS lbry_devices;
-- +goose StatementEnd
