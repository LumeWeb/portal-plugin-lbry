-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS lbry_streams (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_hash CHAR(96) NOT NULL UNIQUE,
    sd_hash CHAR(96) NOT NULL UNIQUE,
    stream_name TEXT,
    stream_type VARCHAR(50) DEFAULT 'lbryfile',
    suggested_file_name TEXT,
    key_data LONGBLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS lbry_blobs (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    blob_hash CHAR(96) NOT NULL UNIQUE,
    blob_size INT NOT NULL,
    iv_data LONGBLOB,
    terminating BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS lbry_stream_blobs (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_id BIGINT UNSIGNED NOT NULL,
    blob_id BIGINT UNSIGNED NOT NULL,
    blob_number INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    FOREIGN KEY (blob_id) REFERENCES lbry_blobs(id) ON DELETE CASCADE,
    UNIQUE KEY unique_stream_blob (stream_id, blob_id)
);

CREATE TABLE IF NOT EXISTS lbry_stream_pins (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    stream_id BIGINT UNSIGNED NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_stream_pin (user_id, stream_id)
);

CREATE TABLE IF NOT EXISTS lbry_devices (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45) NOT NULL UNIQUE,
    is_sd_blob BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lbry_pending_streams (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_hash CHAR(96) NOT NULL,
    sd_hash CHAR(96) NOT NULL,
    stream_name TEXT,
    stream_type VARCHAR(50) DEFAULT 'lbryfile',
    suggested_file_name TEXT,
    key_data LONGBLOB,
    total_blobs INT NOT NULL DEFAULT 0,
    user_id BIGINT UNSIGNED NOT NULL,
    device_id BIGINT UNSIGNED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id),
    
    UNIQUE KEY unique_user_pending_stream_hash (user_id, stream_hash),
    UNIQUE KEY unique_user_pending_sd_hash (user_id, sd_hash),
    KEY idx_pending_streams_user_id (user_id),
    KEY idx_pending_streams_device_id (device_id),
    KEY idx_pending_streams_sd_hash (sd_hash)
);

CREATE TABLE IF NOT EXISTS lbry_pending_blobs (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    blob_hash CHAR(96) NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    device_id BIGINT UNSIGNED,
    stream_id BIGINT UNSIGNED,
    blob_size INT,
    blob_number INT NOT NULL DEFAULT 0,
    received BOOLEAN NOT NULL DEFAULT FALSE,
    terminating BOOLEAN NOT NULL DEFAULT FALSE,
    iv_data LONGBLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (device_id) REFERENCES lbry_devices(id),
    FOREIGN KEY (stream_id) REFERENCES lbry_pending_streams(id) ON DELETE CASCADE
);

CREATE INDEX idx_pending_blobs_user_id ON lbry_pending_blobs(user_id);
CREATE INDEX idx_pending_blobs_device_id ON lbry_pending_blobs(device_id);
CREATE INDEX idx_pending_blobs_stream_id ON lbry_pending_blobs(stream_id);
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
