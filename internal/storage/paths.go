package storage

import (
	"fmt"
)

// GetTempBlobPath generates the storage path for a temporary blob
func GetTempBlobPath(userID uint, blobHash string) string {
	return fmt.Sprintf("temp/users/%d/blobs/%s", userID, blobHash)
}

// GetTempUserPath generates the base storage path for a user's temporary files
func GetTempUserPath(userID uint) string {
	return fmt.Sprintf("temp/users/%d", userID)
}

// GetTempBlobsPath generates the storage path for a user's temporary blobs directory
func GetTempBlobsPath(userID uint) string {
	return fmt.Sprintf("temp/users/%d/blobs", userID)
}
