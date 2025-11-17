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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS lbry_stream_pins;
DROP TABLE IF EXISTS lbry_stream_blobs;
DROP TABLE IF EXISTS lbry_blobs;
DROP TABLE IF EXISTS lbry_streams;
-- +goose StatementEnd
