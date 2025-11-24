// Package api provides API-related functionality for the LBRY protocol
package api

import (
	"encoding/json"
	"net/http"

	"go.lumeweb.com/portal/core"
)

// Namespace for LBRY errors
const Namespace = "lbry"

// LBRY-specific error constants
var (
	ErrKeyMetadataValidationFailed core.ErrorType = "metadata_validation_failed"
	ErrKeyMetadataMissing          core.ErrorType = "metadata_missing"
	ErrKeyMetadataJSONInvalid      core.ErrorType = "metadata_json_invalid"
	ErrKeyUploadFailed             core.ErrorType = "upload_failed"
	ErrKeyStreamProcessingFailed   core.ErrorType = "stream_processing_failed"
	ErrKeyEmptyFile                core.ErrorType = "empty_file"
	ErrKeyInvalidSDHash            core.ErrorType = "invalid_sd_hash"
	ErrKeyStreamNotFound           core.ErrorType = "stream_not_found"
	ErrKeyStreamDeleteFailed       core.ErrorType = "stream_delete_failed"
	ErrKeyStreamListFailed         core.ErrorType = "stream_list_failed"
	ErrKeyDeviceCreateFailed       core.ErrorType = "device_create_failed"
	ErrKeyDeviceUpdateFailed       core.ErrorType = "device_update_failed"
	ErrKeyDeviceGetFailed          core.ErrorType = "device_get_failed"
	ErrKeyDeviceDeleteFailed       core.ErrorType = "device_delete_failed"
	ErrKeyDeviceListFailed         core.ErrorType = "device_list_failed"
)

// ErrorDetails represents detailed error information
type ErrorDetails struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

// ErrorWrapper wraps an error with additional context
type ErrorWrapper struct {
	error
	details *ErrorDetails
}

// LBRYError represents a LBRY-specific error
type LBRYError struct {
	coreErr *core.Error
}

// MarshalJSON implements json.Marshaler for structured error responses
func (e *ErrorWrapper) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"error": e.details.Message,
		"code":  e.details.Code,
	})
}

// Error returns the error message
func (e *LBRYError) Error() string {
	return e.coreErr.Error()
}

// HttpStatus returns the HTTP status code for the error
func (e *LBRYError) HttpStatus() int {
	return e.coreErr.HttpStatus()
}

// Unwrap returns the underlying error
func (e *LBRYError) Unwrap() error {
	return e.coreErr.Unwrap()
}

// NewError creates a new LBRY error
func NewError(key core.ErrorType, err error, args ...any) *LBRYError {
	return &LBRYError{core.NewError(Namespace, key, err, args...)}
}

// init registers the LBRY namespace and error messages
func init() {
	core.MustRegisterNamespace(Namespace)

	// Register error messages using map-based approach
	core.MustRegisterDefaultErrorMessages(Namespace, map[core.ErrorType]core.ErrorDefinition{
		ErrKeyMetadataValidationFailed: {Key: ErrKeyMetadataValidationFailed, Message: "Stream metadata validation failed"},
		ErrKeyMetadataMissing:          {Key: ErrKeyMetadataMissing, Message: "Stream metadata is missing"},
		ErrKeyMetadataJSONInvalid:      {Key: ErrKeyMetadataJSONInvalid, Message: "Stream metadata JSON is invalid"},
		ErrKeyUploadFailed:             {Key: ErrKeyUploadFailed, Message: "Stream upload failed"},
		ErrKeyStreamProcessingFailed:   {Key: ErrKeyStreamProcessingFailed, Message: "Stream processing failed"},
		ErrKeyEmptyFile:                {Key: ErrKeyEmptyFile, Message: "Uploaded file is empty"},
		ErrKeyInvalidSDHash:            {Key: ErrKeyInvalidSDHash, Message: "Invalid SD hash format"},
		ErrKeyStreamNotFound:           {Key: ErrKeyStreamNotFound, Message: "Stream not found or access denied"},
		ErrKeyStreamDeleteFailed:       {Key: ErrKeyStreamDeleteFailed, Message: "Failed to delete stream"},
		ErrKeyStreamListFailed:         {Key: ErrKeyStreamListFailed, Message: "Failed to list streams"},
		ErrKeyDeviceCreateFailed:       {Key: ErrKeyDeviceCreateFailed, Message: "Failed to create device"},
		ErrKeyDeviceUpdateFailed:       {Key: ErrKeyDeviceUpdateFailed, Message: "Failed to update device"},
		ErrKeyDeviceGetFailed:          {Key: ErrKeyDeviceGetFailed, Message: "Failed to get device"},
		ErrKeyDeviceDeleteFailed:       {Key: ErrKeyDeviceDeleteFailed, Message: "Failed to delete device"},
		ErrKeyDeviceListFailed:         {Key: ErrKeyDeviceListFailed, Message: "Failed to list devices"},
	})

	// Register HTTP status codes using map-based approach
	core.MustRegisterErrorCodes(Namespace, map[core.ErrorType]int{
		ErrKeyMetadataValidationFailed: http.StatusUnprocessableEntity,
		ErrKeyMetadataMissing:          http.StatusBadRequest,
		ErrKeyMetadataJSONInvalid:      http.StatusBadRequest,
		ErrKeyUploadFailed:             http.StatusInternalServerError,
		ErrKeyStreamProcessingFailed:   http.StatusInternalServerError,
		ErrKeyEmptyFile:                http.StatusBadRequest,
		ErrKeyInvalidSDHash:            http.StatusBadRequest,
		ErrKeyStreamNotFound:           http.StatusNotFound,
		ErrKeyStreamDeleteFailed:       http.StatusInternalServerError,
		ErrKeyStreamListFailed:         http.StatusInternalServerError,
		ErrKeyDeviceCreateFailed:       http.StatusBadRequest,
		ErrKeyDeviceUpdateFailed:       http.StatusBadRequest,
		ErrKeyDeviceGetFailed:          http.StatusNotFound,
		ErrKeyDeviceDeleteFailed:       http.StatusBadRequest,
		ErrKeyDeviceListFailed:         http.StatusInternalServerError,
	})
}
