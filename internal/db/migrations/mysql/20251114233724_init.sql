-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS lbry_streams (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    stream_hash CHAR(96) NOT NULL UNIQUE,
    sd_hash CHAR(96) NOT NULL UNIQUE,
    stream_name TEXT,
    stream_type VARCHAR(50) DEFAULT 'lbryfile',
    suggested_file_name TEXT,
    key_data LONGBLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    INDEX idx_stream_hash (stream_hash),
    INDEX idx_sd_hash (sd_hash),
    INDEX idx_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS lbry_blobs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    blob_hash CHAR(96) NOT NULL UNIQUE,
    blob_size INT NOT NULL,
    iv_data LONGBLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    INDEX idx_blob_hash (blob_hash),
    INDEX idx_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS lbry_stream_blobs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    stream_id BIGINT NOT NULL,
    blob_id BIGINT NOT NULL,
    blob_number INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    FOREIGN KEY (blob_id) REFERENCES lbry_blobs(id) ON DELETE CASCADE,
    UNIQUE KEY unique_stream_blob (stream_id, blob_id),
    INDEX idx_stream_blob_numbers (stream_id, blob_number),
    INDEX idx_blob_order (blob_number)
);

CREATE TABLE IF NOT EXISTS lbry_stream_pins (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    stream_id BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_stream_pin (user_id, stream_id),
    INDEX idx_user_pinned_at (user_id, created_at),
    INDEX idx_stream_pinned_at (stream_id, created_at)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS lbry_stream_pins;
DROP TABLE IF EXISTS lbry_stream_blobs;
DROP TABLE IF EXISTS lbry_blobs;
DROP TABLE IF EXISTS lbry_streams;
-- +goose StatementEnd
