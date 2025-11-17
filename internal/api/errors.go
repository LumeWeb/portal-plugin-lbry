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
	})

	// Register HTTP status codes using map-based approach
	core.MustRegisterErrorCodes(Namespace, map[core.ErrorType]int{
		ErrKeyMetadataValidationFailed: http.StatusUnprocessableEntity,
		ErrKeyMetadataMissing:          http.StatusBadRequest,
		ErrKeyMetadataJSONInvalid:      http.StatusBadRequest,
		ErrKeyUploadFailed:             http.StatusInternalServerError,
		ErrKeyStreamProcessingFailed:   http.StatusInternalServerError,
		ErrKeyEmptyFile:                http.StatusBadRequest,
	})
}
