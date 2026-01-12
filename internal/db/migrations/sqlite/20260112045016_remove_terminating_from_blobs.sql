-- +goose Up
-- Add terminating_blob_number to streams table
ALTER TABLE lbry_streams ADD COLUMN terminating_blob_number INTEGER DEFAULT NULL;

-- Add terminating_blob_number to pending_streams table  
ALTER TABLE lbry_pending_streams ADD COLUMN terminating_blob_number INTEGER DEFAULT NULL;

-- Recreate blobs table without terminating column
CREATE TABLE lbry_blobs_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL UNIQUE,
    blob_size INTEGER NOT NULL,
    iv_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

-- Copy data from old table to new table
INSERT INTO lbry_blobs_new (id, blob_hash, blob_size, iv_data, created_at, updated_at, deleted_at)
SELECT id, blob_hash, blob_size, iv_data, created_at, updated_at, deleted_at FROM lbry_blobs;

-- Drop old table and rename new table
DROP TABLE lbry_blobs;
ALTER TABLE lbry_blobs_new RENAME TO lbry_blobs;

-- Recreate pending_blobs table without terminating column
CREATE TABLE lbry_pending_blobs_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    device_id INTEGER,
    stream_id INTEGER,
    blob_size INTEGER,
    blob_number INTEGER NOT NULL DEFAULT 0,
    received BOOLEAN NOT NULL DEFAULT 0,
    iv_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL,
    
    UNIQUE (user_id, blob_hash),
    UNIQUE (user_id, stream_id, blob_number),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id),
    FOREIGN KEY (stream_id) REFERENCES lbry_pending_streams(id)
);

-- Copy data from old table to new table
INSERT INTO lbry_pending_blobs_new (id, blob_hash, user_id, device_id, stream_id, blob_size, blob_number, received, iv_data, created_at, updated_at, deleted_at)
SELECT id, blob_hash, user_id, device_id, stream_id, blob_size, blob_number, received, iv_data, created_at, updated_at, deleted_at FROM lbry_pending_blobs;

-- Drop old table and rename new table
DROP TABLE lbry_pending_blobs;
ALTER TABLE lbry_pending_blobs_new RENAME TO lbry_pending_blobs;

-- Recreate indexes for pending_blobs
CREATE INDEX idx_pending_blobs_user_id ON lbry_pending_blobs(user_id);
CREATE INDEX idx_pending_blobs_device_id ON lbry_pending_blobs(device_id);
CREATE INDEX idx_pending_blobs_stream_id ON lbry_pending_blobs(stream_id);

-- +goose Down
-- Re-add terminating column to streams table (not applicable, we're removing it)

-- Recreate blobs table with terminating column
CREATE TABLE lbry_blobs_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL UNIQUE,
    blob_size INTEGER NOT NULL,
    iv_data BLOB,
    terminating BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

-- Copy data from old table to new table
INSERT INTO lbry_blobs_new (id, blob_hash, blob_size, iv_data, created_at, updated_at, deleted_at)
SELECT id, blob_hash, blob_size, iv_data, created_at, updated_at, deleted_at FROM lbry_blobs;

-- Drop old table and rename new table
DROP TABLE lbry_blobs;
ALTER TABLE lbry_blobs_new RENAME TO lbry_blobs;

-- Recreate pending_streams table without terminating_blob_number
CREATE TABLE lbry_pending_streams_new (
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
    deleted_at TIMESTAMP NULL,
    
    UNIQUE (user_id, stream_hash),
    UNIQUE (user_id, sd_hash),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id)
);

-- Copy data from old table to new table
INSERT INTO lbry_pending_streams_new (id, stream_hash, sd_hash, stream_name, stream_type, suggested_file_name, key_data, total_blobs, user_id, device_id, created_at, updated_at, deleted_at)
SELECT id, stream_hash, sd_hash, stream_name, stream_type, suggested_file_name, key_data, total_blobs, user_id, device_id, created_at, updated_at, deleted_at FROM lbry_pending_streams;

-- Drop old table and rename new table
DROP TABLE lbry_pending_streams;
ALTER TABLE lbry_pending_streams_new RENAME TO lbry_pending_streams;

-- Recreate indexes for pending_streams
CREATE INDEX idx_pending_streams_user_id ON lbry_pending_streams(user_id);
CREATE INDEX idx_pending_streams_device_id ON lbry_pending_streams(device_id);
CREATE INDEX idx_pending_streams_sd_hash ON lbry_pending_streams(sd_hash);

-- Recreate pending_blobs table with terminating column
CREATE TABLE lbry_pending_blobs_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_hash TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    device_id INTEGER,
    stream_id INTEGER,
    blob_size INTEGER,
    blob_number INTEGER NOT NULL DEFAULT 0,
    received BOOLEAN NOT NULL DEFAULT 0,
    terminating BOOLEAN NOT NULL DEFAULT 0,
    iv_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL,
    
    UNIQUE (user_id, blob_hash),
    UNIQUE (user_id, stream_id, blob_number),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id),
    FOREIGN KEY (stream_id) REFERENCES lbry_pending_streams(id)
);

-- Copy data from old table to new table
INSERT INTO lbry_pending_blobs_new (id, blob_hash, user_id, device_id, stream_id, blob_size, blob_number, received, iv_data, created_at, updated_at, deleted_at)
SELECT id, blob_hash, user_id, device_id, stream_id, blob_size, blob_number, received, iv_data, created_at, updated_at, deleted_at FROM lbry_pending_blobs;

-- Drop old table and rename new table
DROP TABLE lbry_pending_blobs;
ALTER TABLE lbry_pending_blobs_new RENAME TO lbry_pending_blobs;

-- Recreate indexes for pending_blobs
CREATE INDEX idx_pending_blobs_user_id ON lbry_pending_blobs(user_id);
CREATE INDEX idx_pending_blobs_device_id ON lbry_pending_blobs(device_id);
CREATE INDEX idx_pending_blobs_stream_id ON lbry_pending_blobs(stream_id);

-- Recreate streams table without terminating_blob_number
CREATE TABLE lbry_streams_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_hash TEXT NOT NULL UNIQUE,
    sd_hash TEXT NOT NULL UNIQUE,
    stream_name TEXT,
    stream_type TEXT DEFAULT 'lbryfile',
    suggested_file_name TEXT,
    key_data BLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

-- Copy data from old table to new table
INSERT INTO lbry_streams_new (id, stream_hash, sd_hash, stream_name, stream_type, suggested_file_name, key_data, created_at, updated_at, deleted_at)
SELECT id, stream_hash, sd_hash, stream_name, stream_type, suggested_file_name, key_data, created_at, updated_at, deleted_at FROM lbry_streams;

-- Drop old table and rename new table
DROP TABLE lbry_streams;
ALTER TABLE lbry_streams_new RENAME TO lbry_streams;
