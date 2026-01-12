-- +goose Up
-- Add terminating_blob_number to streams table
ALTER TABLE lbry_streams ADD COLUMN terminating_blob_number INT DEFAULT NULL;

-- Add terminating_blob_number to pending_streams table  
ALTER TABLE lbry_pending_streams ADD COLUMN terminating_blob_number INT DEFAULT NULL;

-- Drop terminating column from blobs table
ALTER TABLE lbry_blobs DROP COLUMN terminating;

-- Drop terminating column from pending_blobs table
ALTER TABLE lbry_pending_blobs DROP COLUMN terminating;

-- Drop the terminating blob unique constraint from pending_blobs
DROP INDEX unique_user_terminating_blob ON lbry_pending_blobs;

-- +goose Down
-- Re-add terminating column to blobs table
ALTER TABLE lbry_blobs ADD COLUMN terminating BOOLEAN NOT NULL DEFAULT FALSE;

-- Re-add terminating column to pending_blobs table
ALTER TABLE lbry_pending_blobs ADD COLUMN terminating BOOLEAN NOT NULL DEFAULT FALSE;

-- Re-add the terminating blob unique constraint to pending_blobs
CREATE UNIQUE INDEX unique_user_terminating_blob ON lbry_pending_blobs(user_id, stream_id, blob_number);

-- Drop terminating_blob_number from streams table
ALTER TABLE lbry_streams DROP COLUMN terminating_blob_number;

-- Drop terminating_blob_number from pending_streams table
ALTER TABLE lbry_pending_streams DROP COLUMN terminating_blob_number;
