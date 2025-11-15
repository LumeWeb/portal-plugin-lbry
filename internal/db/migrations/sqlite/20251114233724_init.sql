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

CREATE INDEX IF NOT EXISTS idx_lbry_stream_hash ON lbry_streams(stream_hash);
CREATE INDEX IF NOT EXISTS idx_lbry_sd_hash ON lbry_streams(sd_hash);
CREATE INDEX IF NOT EXISTS idx_lbry_created_at ON lbry_streams(created_at);

CREATE TABLE IF NOT EXISTS lbry_blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL UNIQUE,
    blob_size INTEGER NOT NULL,
    iv_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_lbry_blob_hash ON lbry_blobs(blob_hash);
CREATE INDEX IF NOT EXISTS idx_lbry_created_at ON lbry_blobs(created_at);

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

CREATE INDEX IF NOT EXISTS idx_lbry_stream_blob_numbers ON lbry_stream_blobs(stream_id, blob_number);
CREATE INDEX IF NOT EXISTS idx_lbry_blob_order ON lbry_stream_blobs(blob_number);

CREATE TABLE IF NOT EXISTS lbry_stream_pins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    UNIQUE(user_id, stream_id)
);

CREATE INDEX IF NOT EXISTS idx_lbry_user_pinned_at ON lbry_stream_pins(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_lbry_stream_pinned_at ON lbry_stream_pins(stream_id, created_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS lbry_stream_pins;
DROP TABLE IF EXISTS lbry_stream_blobs;
DROP TABLE IF EXISTS lbry_blobs;
DROP TABLE IF EXISTS lbry_streams;
-- +goose StatementEnd
