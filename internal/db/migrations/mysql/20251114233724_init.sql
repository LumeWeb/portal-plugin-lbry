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
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES lbry_streams(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_stream_pin (user_id, stream_id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS lbry_stream_pins;
DROP TABLE IF EXISTS lbry_stream_blobs;
DROP TABLE IF EXISTS lbry_blobs;
DROP TABLE IF EXISTS lbry_streams;
-- +goose StatementEnd
