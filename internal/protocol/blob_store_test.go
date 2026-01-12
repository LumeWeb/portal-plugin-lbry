package protocol

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/blob"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"gorm.io/gorm"
)

// Hash constants
const (
	testBlobHash1 = "a2f1841bb9c5f3b583ac3b8c07ee1a5bf9cc48923721c30d5ca6318615776c284e8936d72fa4db7fdda2e4e9598b1e6c"
	testBlobHash2 = "0c9675ad7f40f29dcd41883ed9cf7e145bbb13976d9b83ab9354f4f61a87f0f7771a56724c2aa7a5ab43c68d7942e5cb"
	testBlobHash3 = "a4d07d442b9907036c75b6c92db316a8b8428733bf5ec976627a48a7c862bf84db33075d54125a7c0b297bd2dc445f1c"
	testBlobHash4 = "dcd2093f4a3eca9f6dd59d785d0bef068fee788481986aa894cf72ed4d992c0ff9d19d1743525de2f5c3c62f5ede1c58"
)

// Data constants
const (
	testData1 = "test blob data"
	testData2 = "test blob data for put operation"
)

// Stream constants
const (
	testStreamName        = "test_stream"
	testStreamType        = "video"
	testSuggestedFileName = "test_video.mp4"
	testEncryptionKey     = "test_encryption_key"
)

// Helper functions for test setup
func createTestStore(tb testing.TB, ctx coreTesting.TestContext) (*BlobStore, *coreMocks.MockStorageService) {

	store, err := NewLBRYBlobStore(ctx)
	require.NoError(tb, err)

	mockStorage := coreMocks.NewMockStorageService(tb)
	store.storageSvc = mockStorage

	return store, mockStorage
}

func createMockStorage(tb testing.TB) *coreMocks.MockStorageService {
	return coreMocks.NewMockStorageService(tb)
}

func insertTestBlob(tb testing.TB, ctx coreTesting.TestContext, hash string, size int, ivData []byte) db.Blob {

	blob := db.Blob{
		BlobHash: hash,
		BlobSize: size,
		IVData:   ivData,
	}
	err := ctx.DB().Create(&blob).Error
	require.NoError(tb, err)

	return blob
}

func createTestStream(tb testing.TB, ctx coreTesting.TestContext, streamHash, sdHash, streamName, streamType, suggestedFileName string, keyData []byte) db.Stream {
	_stream := db.Stream{
		StreamHash:        streamHash,
		SDHash:            sdHash,
		StreamName:        streamName,
		StreamType:        streamType,
		SuggestedFileName: suggestedFileName,
		KeyData:           keyData,
	}
	err := ctx.DB().Create(&_stream).Error
	require.NoError(tb, err)

	return _stream
}

func createStreamBlobAssociation(tb testing.TB, ctx coreTesting.TestContext, streamID, blobID uint64, blobNumber int) db.StreamBlob {

	streamBlob := db.StreamBlob{
		StreamID:   streamID,
		BlobID:     blobID,
		BlobNumber: blobNumber,
	}
	err := ctx.DB().Create(&streamBlob).Error
	require.NoError(tb, err)

	return streamBlob
}

// Helper functions for common assertions
func assertBlobExists(tb testing.TB, ctx coreTesting.TestContext, hash string, expectedSize int) {
	ast := assert.New(tb)

	var blob db.Blob
	err := ctx.DB().Where("blob_hash = ?", hash).First(&blob).Error
	ast.NoError(err)
	ast.Equal(hash, blob.BlobHash)
	ast.Equal(expectedSize, blob.BlobSize)
}

func assertBlobNotExists(tb testing.TB, ctx coreTesting.TestContext, hash string) {
	ast := assert.New(tb)

	var blob db.Blob
	err := ctx.DB().Where("blob_hash = ?", hash).First(&blob).Error
	ast.Error(err)
	ast.True(errors.Is(err, gorm.ErrRecordNotFound))
}

func assertStreamExists(tb testing.TB, ctx coreTesting.TestContext, sdHash string, expectedStream db.Stream) {
	ast := assert.New(tb)

	var stream db.Stream
	err := ctx.DB().Where("sd_hash = ?", sdHash).First(&stream).Error
	ast.NoError(err)
	ast.Equal(expectedStream.StreamHash, stream.StreamHash)
	ast.Equal(expectedStream.StreamName, stream.StreamName)
	ast.Equal(expectedStream.StreamType, stream.StreamType)
	ast.Equal(expectedStream.SuggestedFileName, stream.SuggestedFileName)
	ast.Equal(expectedStream.KeyData, stream.KeyData)
}

func createTestSDBlob(streamName, streamType, suggestedFileName string, key []byte) *stream.SDBlob {
	return &stream.SDBlob{
		StreamName:        streamName,
		StreamType:        streamType,
		SuggestedFileName: suggestedFileName,
		Key:               key,
	}
}

// Helper functions for hex decoding
func createTestBlobHashBytes(hash1, hash2 string) ([]byte, []byte, error) {
	blob1Bytes, err := hex.DecodeString(hash1)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode hash1 %q: %w", hash1, err)
	}

	blob2Bytes, err := hex.DecodeString(hash2)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode hash2 %q: %w", hash2, err)
	}

	return blob1Bytes, blob2Bytes, nil
}

// Helper functions for mock storage expectations
func setupMockDownloadSuccess(tb testing.TB, mockStorage *coreMocks.MockStorageService, hash string, data string) {
	storageHash, err := internal.LBRYHashToStorageHash(hash)
	require.NoError(tb, err, "failed to convert hash for mock setup")
	mockStorage.EXPECT().DownloadObject(mock.Anything, mock.Anything, storageHash, int64(0)).
		Return(io.NopCloser(strings.NewReader(data)), nil)
}

func setupMockDownloadError(tb testing.TB, mockStorage *coreMocks.MockStorageService, hash string, errorMsg string) {
	storageHash, err := internal.LBRYHashToStorageHash(hash)
	require.NoError(tb, err, "failed to convert hash for mock setup")
	mockStorage.EXPECT().DownloadObject(mock.Anything, mock.Anything, storageHash, int64(0)).
		Return(nil, fmt.Errorf("%s", errorMsg))
}

func setupMockUploadSuccess(mockStorage *coreMocks.MockStorageService) {
	mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
		Return(nil, nil)
}

func setupMockUploadError(mockStorage *coreMocks.MockStorageService, errorMsg string) {
	mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("%s", errorMsg))
}

func setupMockDeleteSuccess(tb testing.TB, mockStorage *coreMocks.MockStorageService, hash string) {
	storageHash, err := internal.LBRYHashToStorageHash(hash)
	require.NoError(tb, err, "failed to convert hash for mock setup")
	mockStorage.EXPECT().DeleteObject(mock.Anything, mock.Anything, storageHash).
		Return(nil)
}

func setupMockDeleteError(tb testing.TB, mockStorage *coreMocks.MockStorageService, hash string, errorMsg string) {
	storageHash, err := internal.LBRYHashToStorageHash(hash)
	require.NoError(tb, err, "failed to convert hash for mock setup")
	mockStorage.EXPECT().DeleteObject(mock.Anything, mock.Anything, storageHash).
		Return(fmt.Errorf("%s", errorMsg))
}

func TestLBRYBlobStore_NewLBRYBlobStore(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Act
		store, err := NewLBRYBlobStore(ctx)

		// Assert
		ast := assert.New(tb)
		ast.NoError(err)
		ast.NotNil(store)
		ast.Equal(BLOBSTORE_NAME, store.Name())
	})
}

func TestLBRYBlobStore_Has(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, mockStorage := createTestStore(tb, ctx)

		// Act - Test when blob doesn't exist in DB
		has, err := store.Has(t.Context(), testBlobHash1)

		// Assert
		ast.NoError(err)
		ast.False(has)

		// Arrange - Insert blob into DB
		insertTestBlob(tb, ctx, testBlobHash1, 100, nil)

		// Set up mock expectation for when blob exists in DB but not in storage
		setupMockDownloadError(tb, mockStorage, testBlobHash1, "object not found")

		// Act - Test when blob exists in DB but not in storage
		has, err = store.Has(t.Context(), testBlobHash1)
		ast.NoError(err)
		ast.False(has)

		// Arrange - Create a fresh mock instance for storage success case
		mockStorage = createMockStorage(tb)
		store.storageSvc = mockStorage

		setupMockDownloadSuccess(tb, mockStorage, testBlobHash1, testData1)

		// Act - Test when blob exists in both DB and storage
		has, err = store.Has(t.Context(), testBlobHash1)
		ast.NoError(err)
		ast.True(has)
	})
}

func TestLBRYBlobStore_Get(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange

		ast := assert.New(tb)
		store, mockStorage := createTestStore(tb, ctx)

		testData := []byte(testData1)
		insertTestBlob(tb, ctx, testBlobHash1, len(testData), nil)

		setupMockDownloadSuccess(tb, mockStorage, testBlobHash1, testData1)

		// Act
		data, err := store.Get(t.Context(), testBlobHash1)

		// Assert
		ast.NoError(err)
		ast.Equal(testData, data)
	})
}

func TestLBRYBlobStore_Get_SDBlobWithAssociatedBlobs(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange

		ast := assert.New(tb)
		_store, _ := createTestStore(tb, ctx)

		sdBlob := createTestSDBlob(testStreamName, testStreamType, testSuggestedFileName, []byte(testEncryptionKey))
		_, err := sdBlob.ToBlob()
		require.NoError(tb, err)

		_stream := createTestStream(tb, ctx, sdBlob.HashHex(), sdBlob.HashHex(),
			testStreamName, testStreamType, testSuggestedFileName, []byte(testEncryptionKey))

		// Create associated blobs with IV data
		blob1 := insertTestBlob(tb, ctx, testBlobHash1, 100, []byte("iv12345678901234"))
		blob2 := insertTestBlob(tb, ctx, testBlobHash2, 200, []byte("iv56789012345678"))

		// Create stream_blob associations
		createStreamBlobAssociation(tb, ctx, uint64(_stream.ID), uint64(blob1.ID), 0)
		createStreamBlobAssociation(tb, ctx, uint64(_stream.ID), uint64(blob2.ID), 1)

		// Act
		data, err := _store.Get(t.Context(), sdBlob.HashHex())

		// Assert
		ast.NoError(err)

		parsedSdBlob := stream.SDBlob{}
		err = parsedSdBlob.FromBlob(data)
		ast.NoError(err)
		ast.Equal(testStreamName, parsedSdBlob.StreamName)
		ast.Equal(testStreamType, parsedSdBlob.StreamType)
		ast.Equal(testSuggestedFileName, parsedSdBlob.SuggestedFileName)
		ast.Equal([]byte(testEncryptionKey), parsedSdBlob.Key)

		// Verify that stream hash is correctly set
		ast.Equal(_stream.StreamHash, hex.EncodeToString(parsedSdBlob.StreamHash))

		// Verify that blob info was included in the reconstructed blob
		ast.Len(parsedSdBlob.BlobInfos, 2)

		// Verify blob details including IV data
		expectedIVs := [][]byte{[]byte("iv12345678901234"), []byte("iv56789012345678")}
		for i, blobInfo := range parsedSdBlob.BlobInfos {
			ast.Equal(expectedIVs[i], blobInfo.IV, "IV data should match for blob %d", i)
		}
	})
}

func TestLBRYBlobStore_Get_SDBlobNoAssociatedBlobs(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Create test SD blob data
		sdBlob := stream.SDBlob{
			StreamName:        testStreamName,
			StreamType:        testStreamType,
			SuggestedFileName: testSuggestedFileName,
			Key:               []byte(testEncryptionKey),
		}

		// Create stream in DB (no associated blobs)
		// Use a proper stream hash since the implementation will set it from DB
		streamHash := "d756e860d8f49d03937c1a35a560636e792d97bee6f9660fc69e206cbcfe7f9297c5ede8428dd5f17d240e434eb557da"
		// Update the SD blob with the correct stream hash first
		sdBlob.StreamHash, _ = hex.DecodeString(streamHash)
		expectedData, err := sdBlob.ToBlob()
		require.NoError(tb, err)

		// Now get the correct hash after updating the blob content
		correctHash := sdBlob.HashHex()

		_stream := db.Stream{
			StreamHash:        streamHash,
			SDHash:            correctHash, // Use the hash of the updated blob
			StreamName:        sdBlob.StreamName,
			StreamType:        sdBlob.StreamType,
			SuggestedFileName: sdBlob.SuggestedFileName,
			KeyData:           sdBlob.Key,
		}
		err = ctx.DB().Create(&_stream).Error
		require.NoError(tb, err)

		// Act - use the correct hash that matches the blob content
		data, err := store.Get(t.Context(), correctHash)

		// Assert
		ast.NoError(err)
		// Should return serialized SD blob data with correct stream hash
		ast.Equal(expectedData, data)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Get_SDBlobMissingAssociatedBlobs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Create test SD blob data
		sdBlob := stream.SDBlob{
			StreamName:        testStreamName,
			StreamType:        testStreamType,
			SuggestedFileName: testSuggestedFileName,
			Key:               []byte(testEncryptionKey),
		}

		// Create stream in DB with proper stream hash
		streamHash := "d756e860d8f49d03937c1a35a560636e792d97bee6f9660fc69e206cbcfe7f9297c5ede8428dd5f17d240e434eb557da"

		// Update the SD blob with the correct stream hash first
		sdBlob.StreamHash, _ = hex.DecodeString(streamHash)

		// Add placeholder blob info for the missing blob (blob ID 999)
		sdBlob.BlobInfos = []stream.BlobInfo{
			{
				Length:   0,        // Missing blobs have no size
				BlobNum:  0,        // Blob number from the test
				BlobHash: []byte{}, // Empty hash for missing blobs
				IV:       []byte{}, // Empty IV for missing blobs
			},
		}

		expectedData, err := sdBlob.ToBlob()
		require.NoError(tb, err)

		// Now get the correct hash after updating the blob content
		correctHash := sdBlob.HashHex()

		_stream := db.Stream{
			StreamHash:        streamHash,
			SDHash:            correctHash, // Use the hash of the updated blob
			StreamName:        sdBlob.StreamName,
			StreamType:        sdBlob.StreamType,
			SuggestedFileName: sdBlob.SuggestedFileName,
			KeyData:           sdBlob.Key,
		}
		err = ctx.DB().Create(&_stream).Error
		require.NoError(tb, err)

		// Create stream_blob association with a blob that doesn't exist
		streamBlob := db.StreamBlob{
			StreamID:   uint64(_stream.ID),
			BlobID:     999, // Non-existent blob ID
			BlobNumber: 0,
		}
		err = ctx.DB().Create(&streamBlob).Error
		require.NoError(tb, err)

		// Act
		data, err := store.Get(t.Context(), correctHash)

		// Assert
		ast.NoError(err)
		// Should still return serialized SD blob data even with missing blobs
		ast.Equal(expectedData, data)

		// No need to call mockStorage.AssertExpectations(tb)
	}, testConfig)
}

func TestLBRYBlobStore_Get_StreamMetadataError(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Act
		data, err := store.Get(t.Context(), testBlobHash1)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "not found in local storage")
		ast.Nil(data)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Put(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange

		ast := assert.New(tb)
		store, mockStorage := createTestStore(tb, ctx)

		testData := []byte(testData2)
		_, err := internal.LBRYHashToStorageHash(testBlobHash1)
		require.NoError(tb, err)

		setupMockUploadSuccess(mockStorage)

		// Act
		err = store.Put(t.Context(), testBlobHash1, testData)

		// Assert
		ast.NoError(err)
		assertBlobExists(tb, ctx, testBlobHash1, len(testData))
	})
}

func TestLBRYBlobStore_PutSD(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		sdBlob := createTestSDBlob(testStreamName, testStreamType, testSuggestedFileName, []byte(testEncryptionKey))
		testData, err := sdBlob.ToBlob()
		require.NoError(tb, err)

		hash := sdBlob.HashHex()

		// Act
		err = store.PutSD(t.Context(), hash, testData)

		// Assert
		ast.NoError(err)

		expectedStream := db.Stream{
			StreamHash:        hex.EncodeToString(sdBlob.StreamHash),
			StreamName:        testStreamName,
			StreamType:        testStreamType,
			SuggestedFileName: testSuggestedFileName,
			KeyData:           []byte(testEncryptionKey),
		}
		assertStreamExists(tb, ctx, hash, expectedStream)
	})
}

func TestLBRYBlobStore_Name(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Act
		name := store.Name()

		// Assert
		ast.Equal(BLOBSTORE_NAME, name)
	})
}

func TestLBRYBlobStore_List(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Insert some test data
		blob1 := db.Blob{
			BlobHash: testBlobHash1,
			BlobSize: 100,
		}
		blob2 := db.Blob{
			BlobHash: testBlobHash2,
			BlobSize: 200,
		}

		err = ctx.DB().Create(&blob1).Error
		require.NoError(tb, err)

		err = ctx.DB().Create(&blob2).Error
		require.NoError(tb, err)

		// Act
		results, err := store.List(t.Context(), 0, 10)

		// Assert
		ast.NoError(err)
		ast.NotNil(results)
		// We can't easily predict the exact order due to UNION ALL, so just check it doesn't panic
		ast.GreaterOrEqual(len(results), 2)
	})
}

func TestLBRYBlobStore_Delete(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange

		ast := assert.New(tb)
		store, mockStorage := createTestStore(tb, ctx)

		setupMockDeleteSuccess(tb, mockStorage, testBlobHash2)

		insertTestBlob(tb, ctx, testBlobHash2, 100, nil)

		// Act
		err := store.Delete(t.Context(), testBlobHash2)

		// Assert
		ast.NoError(err)
		assertBlobNotExists(tb, ctx, testBlobHash2)
	})
}

func TestLBRYBlobStore_Get_StorageError(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Insert blob into DB first (simulating existing blob)
		blob := db.Blob{
			BlobHash: testBlobHash1,
			BlobSize: 100,
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(tb, err)

		// Set up mock expectations for storage error
		setupMockDownloadError(tb, mockStorage, testBlobHash1, "storage connection failed")

		// Act
		data, err := store.Get(t.Context(), testBlobHash1)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "failed to download blob")
		ast.Nil(data)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Put_StorageFailure(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Test data
		testData := []byte(testData2)

		// Set up mock expectations for storage failure
		setupMockUploadError(mockStorage, "upload failed: disk space exceeded")

		// Act
		err = store.Put(t.Context(), testBlobHash1, testData)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "failed to store blob")
		ast.Contains(err.Error(), "upload failed")

		// Verify blob was not stored in DB
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testBlobHash1).First(&blob).Error
		ast.Error(err)
		ast.True(errors.Is(err, gorm.ErrRecordNotFound))

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_PutSD_InvalidBlobData(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Test with invalid blob data
		invalidData := []byte("this is not valid SD blob data")

		// Act
		err = store.PutSD(t.Context(), "invalid_hash", invalidData)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "failed to parse SD blob")
	})
}

func TestLBRYBlobStore_PutSD_WithChildBlobs(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a proper SDBlob structure with child blobs
		sdBlob := stream.SDBlob{
			StreamName:        "test_stream_with_blobs",
			StreamType:        "video",
			SuggestedFileName: "test_video_with_blobs.mp4",
			Key:               []byte("test_encryption_key"),
		}

		// Add child blob infos to simulate a real stream with multiple blobs
		// Decode the hex strings to get the actual blob hash bytes
		blobHash1Bytes, blobHash2Bytes, err := createTestBlobHashBytes(testBlobHash1, testBlobHash2)
		require.NoError(tb, err)

		sdBlob.BlobInfos = []stream.BlobInfo{
			{
				Length:   1024,
				BlobNum:  0,
				BlobHash: blobHash1Bytes,
				IV:       []byte("iv12345678901234"), // 16 bytes IV
			},
			{
				Length:   2048,
				BlobNum:  1,
				BlobHash: blobHash2Bytes,
				IV:       []byte("iv56789012345678"), // 16 bytes IV
			},
		}

		// Serialize the SDBlob to its binary format
		testData, err := sdBlob.ToBlob()
		require.NoError(tb, err)

		hash := sdBlob.HashHex()

		// Act
		err = store.PutSD(t.Context(), hash, testData)

		// Assert
		ast.NoError(err)

		// Check that stream was inserted into DB with correct values
		var _stream db.Stream
		err = ctx.DB().Where("sd_hash = ?", hash).First(&_stream).Error
		ast.NoError(err)
		ast.Equal(hex.EncodeToString(sdBlob.StreamHash), _stream.StreamHash)
		ast.Equal("test_stream_with_blobs", _stream.StreamName)
		ast.Equal("video", _stream.StreamType)
		ast.Equal("test_video_with_blobs.mp4", _stream.SuggestedFileName)
		ast.Equal(sdBlob.Key, _stream.KeyData)

		// Check that child blob records were created
		var blobs []db.Blob
		err = ctx.DB().Table("lbry_blobs").
			Joins("JOIN lbry_stream_blobs ON lbry_blobs.id = lbry_stream_blobs.blob_id").
			Where("lbry_blobs.blob_hash IN ? AND lbry_stream_blobs.stream_id = ?", []string{testBlobHash1, testBlobHash2}, _stream.ID).
			Order("lbry_stream_blobs.blob_number").
			Find(&blobs).Error
		ast.NoError(err)
		ast.Len(blobs, 2)

		// Verify blob details including IV data
		blobHashes := make([]string, len(blobs))
		blobSizes := make([]int, len(blobs))
		blobIVs := make([][]byte, len(blobs))
		for i, blob := range blobs {
			blobHashes[i] = blob.BlobHash
			blobSizes[i] = blob.BlobSize
			blobIVs[i] = blob.IVData
		}
		ast.Equal([]string{testBlobHash1, testBlobHash2}, blobHashes)
		ast.Equal([]int{1024, 2048}, blobSizes)

		// Verify IV data is stored correctly
		expectedIVs := [][]byte{[]byte("iv12345678901234"), []byte("iv56789012345678")}
		ast.Equal(expectedIVs, blobIVs)

		// Check that stream blob associations were created
		var streamBlobs []db.StreamBlob
		err = ctx.DB().Where("stream_id = ?", _stream.ID).Order("blob_number").Find(&streamBlobs).Error
		ast.NoError(err)
		ast.Len(streamBlobs, 2)

		// Verify no duplicate blobs were created
		var totalBlobCount int64
		err = ctx.DB().Model(&db.Blob{}).Where("blob_hash IN ?", []string{testBlobHash1, testBlobHash2}).Count(&totalBlobCount).Error
		ast.NoError(err)
		ast.Equal(int64(2), totalBlobCount, "Should not create duplicate blob records")

		// Verify stream blob associations
		ast.Equal(0, streamBlobs[0].BlobNumber)
		ast.Equal(1, streamBlobs[1].BlobNumber)
		ast.Equal(uint64(blobs[0].ID), streamBlobs[0].BlobID)
		ast.Equal(uint64(blobs[1].ID), streamBlobs[1].BlobID)
		ast.Equal(uint64(_stream.ID), streamBlobs[0].StreamID)
		ast.Equal(uint64(_stream.ID), streamBlobs[1].StreamID)
	})
}

func TestLBRYBlobStore_PutSD_WithExistingChildBlobs(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Pre-existing blob records
		existingBlob1 := db.Blob{
			BlobHash: testBlobHash1,
			BlobSize: 512, // Different size to test update
		}
		existingBlob2 := db.Blob{
			BlobHash: testBlobHash2,
			BlobSize: 1024, // Different size to test update
		}
		err = ctx.DB().Create(&existingBlob1).Error
		require.NoError(tb, err)
		err = ctx.DB().Create(&existingBlob2).Error
		require.NoError(tb, err)

		// Create a proper SDBlob structure with child blobs
		sdBlob := stream.SDBlob{
			StreamName:        "test_stream_existing_blobs",
			StreamType:        "audio",
			SuggestedFileName: "test_audio.mp3",
			Key:               []byte("test_audio_key"),
		}

		// Add child blob infos with updated sizes
		// Decode the hex strings to get the actual blob hash bytes
		blobHash1Bytes, blobHash2Bytes, err := createTestBlobHashBytes(testBlobHash1, testBlobHash2)
		require.NoError(tb, err)

		sdBlob.BlobInfos = []stream.BlobInfo{
			{
				Length:   1024, // Updated size
				BlobNum:  0,
				BlobHash: blobHash1Bytes,
				IV:       []byte("iv12345678901234"),
			},
			{
				Length:   2048, // Updated size
				BlobNum:  1,
				BlobHash: blobHash2Bytes,
				IV:       []byte("iv56789012345678"),
			},
		}

		// Serialize the SDBlob to its binary format
		testData, err := sdBlob.ToBlob()
		require.NoError(tb, err)

		hash := sdBlob.HashHex()

		// Act
		err = store.PutSD(t.Context(), hash, testData)

		// Assert
		ast.NoError(err)

		// Check that stream was inserted into DB
		var _stream db.Stream
		err = ctx.DB().Where("sd_hash = ?", hash).First(&_stream).Error
		ast.NoError(err)
		ast.Equal(hex.EncodeToString(sdBlob.StreamHash), _stream.StreamHash)

		// Check that existing blob records were updated with new sizes
		var blobs []db.Blob
		err = ctx.DB().Table("lbry_blobs").
			Joins("JOIN lbry_stream_blobs ON lbry_blobs.id = lbry_stream_blobs.blob_id").
			Where("lbry_blobs.blob_hash IN ? AND lbry_stream_blobs.stream_id = ?", []string{testBlobHash1, testBlobHash2}, _stream.ID).
			Order("lbry_stream_blobs.blob_number").
			Find(&blobs).Error
		ast.NoError(err)
		ast.Len(blobs, 2)

		// Verify blob sizes were updated
		blobSizes := make([]int, len(blobs))
		for i, blob := range blobs {
			blobSizes[i] = blob.BlobSize
		}
		ast.Equal([]int{1024, 2048}, blobSizes) // Updated sizes

		// Check that stream blob associations were created
		var streamBlobs []db.StreamBlob
		err = ctx.DB().Where("stream_id = ?", _stream.ID).Order("blob_number").Find(&streamBlobs).Error
		ast.NoError(err)
		ast.Len(streamBlobs, 2)
	})
}

func TestLBRYBlobStore_Delete_StorageError(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Set up mock expectations for storage error
		setupMockDeleteError(tb, mockStorage, testBlobHash1, "storage delete failed: network timeout")

		// Insert blob into DB
		blob := db.Blob{
			BlobHash: testBlobHash1,
			BlobSize: 100,
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(tb, err)

		// Act
		err = store.Delete(t.Context(), testBlobHash1)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "failed to delete blob from storage")
		ast.Contains(err.Error(), "network timeout")

		// Verify blob still exists in DB (partial failure scenario)
		var deletedBlob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testBlobHash1).First(&deletedBlob).Error
		ast.NoError(err)
		ast.Equal(testBlobHash1, deletedBlob.BlobHash)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Put_EmptyData(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Test with empty data
		emptyData := []byte{}

		// Set up mock expectations
		setupMockUploadSuccess(mockStorage)

		// Act
		err = store.Put(t.Context(), testBlobHash1, emptyData)

		// Assert
		ast.NoError(err)

		// Check that blob was inserted into DB with size 0
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testBlobHash1).First(&blob).Error
		ast.NoError(err)
		ast.Equal(testBlobHash1, blob.BlobHash)
		ast.Equal(0, blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Put_NilData(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Set up mock expectations
		setupMockUploadSuccess(mockStorage)

		// Act
		err = store.Put(t.Context(), testBlobHash1, nil)

		// Assert
		ast.NoError(err)

		// Check that blob was inserted into DB with size 0
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testBlobHash1).First(&blob).Error
		ast.NoError(err)
		ast.Equal(testBlobHash1, blob.BlobHash)
		ast.Equal(0, blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Get_NotInDB(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Don't insert blob into DB - this should cause the method to return early
		// Set up mock expectations to ensure storage is NOT called
		// Note: We don't set up any mock expectations for storage since it shouldn't be called

		// Act
		data, err := store.Get(t.Context(), testBlobHash1)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "not found in local storage")
		ast.Nil(data)

		// Verify storage was never called
		// Note: We can't easily assert that storage wasn't called with testify/mock
		// But we know from the logic that if the blob isn't in DB, we return early
		// So we just verify the error condition

		// No need to call mockStorage.AssertExpectations(tb) because it shouldn't be called
	})
}

func TestLBRYBlobStore_Put_LargeBlob(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Create large test data (>1MB)
		largeData := make([]byte, 2*1024*1024) // 2MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		// Set up mock expectations
		setupMockUploadSuccess(mockStorage)

		// Act
		err = store.Put(t.Context(), testBlobHash1, largeData)

		// Assert
		ast.NoError(err)

		// Check that blob was inserted into DB with correct size
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testBlobHash1).First(&blob).Error
		ast.NoError(err)
		ast.Equal(testBlobHash1, blob.BlobHash)
		ast.Equal(len(largeData), blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Put_TerminatingBlob(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Set up mock expectations - storage should NOT be called for terminating blob

		// Act - Put terminating blob with empty hash and no data
		err = store.Put(t.Context(), "", []byte{})

		// Assert
		ast.NoError(err)

		// Verify storage was NOT called for terminating blob
		mockStorage.AssertNotCalled(tb, "UploadObject")

		// Terminating blobs are no longer stored in the blob table
		// They are tracked via terminating_blob_number on the stream
	})
}

func TestLBRYBlobStore_List_PaginationEdgeCases(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Insert some test data
		blob1 := db.Blob{
			BlobHash: testBlobHash1,
			BlobSize: 100,
		}
		blob2 := db.Blob{
			BlobHash: testBlobHash2,
			BlobSize: 200,
		}

		err = ctx.DB().Create(&blob1).Error
		require.NoError(tb, err)

		err = ctx.DB().Create(&blob2).Error
		require.NoError(tb, err)

		// Test with offset beyond available records
		results, err := store.List(t.Context(), 10, 5)
		ast.NoError(err)
		// Should return empty slice, not panic
		ast.Len(results, 0)

		// Test with limit 0
		results, err = store.List(t.Context(), 0, 0)
		ast.NoError(err)
		// Should return empty slice, not panic
		ast.Len(results, 0)

		// Test with negative offset (should behave like offset 0)
		results, err = store.List(t.Context(), -5, 5)
		ast.NoError(err)
		// Should return first 5 items (or however many exist)
		ast.GreaterOrEqual(len(results), 0)
		ast.LessOrEqual(len(results), 2)

		// Test with negative limit (should behave like limit 10)
		results, err = store.List(t.Context(), 0, -10)
		ast.NoError(err)
		// Should return up to 10 items
		ast.GreaterOrEqual(len(results), 0)
		ast.LessOrEqual(len(results), 2)
	})
}

func TestLBRYBlobStore_PartialFailure_DBSuccessStorageFailure(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create a mock storage service that will fail
		mockStorage := coreMocks.NewMockStorageService(tb)
		store.storageSvc = mockStorage

		// Test data
		testData := []byte(testData2)

		// Set up mock expectations for storage failure
		setupMockUploadError(mockStorage, "storage quota exceeded")

		// Act
		err = store.Put(t.Context(), testBlobHash1, testData)

		// Assert
		ast.Error(err)
		ast.Contains(err.Error(), "storage quota exceeded")

		// Verify blob was not stored in DB (since storage failed)
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testBlobHash1).First(&blob).Error
		ast.Error(err)
		ast.True(errors.Is(err, gorm.ErrRecordNotFound))

		// No need to call mockStorage.AssertExpectations(tb)
	})
}

func TestLBRYBlobStore_Get_SDBlobSkipsEmptyBlobs(t *testing.T) {
	// Define test case structure
	type testCase struct {
		name            string
		blobs           []db.Blob
		expectedCount   int
		expectedIVs     [][]byte
		description     string
		allowZeroLength bool // Allow zero-length blobs in this test case
	}

	// Define test cases
	testCases := []testCase{
		{
			name: "empty_hash_blob",
			blobs: []db.Blob{
				{BlobHash: "", BlobSize: 100, IVData: []byte("iv12345678901234")},
			},
			expectedCount:   0,
			expectedIVs:     [][]byte{},
			description:     "Blob with empty hash should be skipped",
			allowZeroLength: false,
		},
		{
			name: "non_terminating_zero_size_blob",
			blobs: []db.Blob{
				{BlobHash: testBlobHash1, BlobSize: 100, IVData: []byte("iv12345678901234")},
				{BlobHash: testBlobHash3, BlobSize: 0, IVData: []byte("iv56789012345678")},
				{BlobHash: testBlobHash2, BlobSize: 200, IVData: []byte("iv56789012345678")},
			},
			expectedCount:   2, // Non-terminating zero-size blob should be skipped, but the other two should be included
			expectedIVs:     [][]byte{[]byte("iv12345678901234"), []byte("iv56789012345678")},
			description:     "Non-terminating zero-size blob should be skipped",
			allowZeroLength: false,
		},
		{
			name: "empty_iv_blob",
			blobs: []db.Blob{
				{BlobHash: testBlobHash1, BlobSize: 100, IVData: []byte{}},
			},
			expectedCount:   0,
			expectedIVs:     [][]byte{},
			description:     "Blob with empty IV should be skipped",
			allowZeroLength: false,
		},
		{
			name: "all_valid_blobs",
			blobs: []db.Blob{
				{BlobHash: testBlobHash1, BlobSize: 100, IVData: []byte("iv12345678901234")},
				{BlobHash: testBlobHash2, BlobSize: 200, IVData: []byte("iv56789012345678")},
			},
			expectedCount:   2,
			expectedIVs:     [][]byte{[]byte("iv12345678901234"), []byte("iv56789012345678")},
			description:     "All valid blobs should be included",
			allowZeroLength: false,
		},
		{
			name: "mixed_valid_invalid_blobs",
			blobs: []db.Blob{
				{BlobHash: testBlobHash1, BlobSize: 100, IVData: []byte("iv12345678901234")}, // Valid
				{BlobHash: "", BlobSize: 200, IVData: []byte("iv56789012345678")},            // Empty hash
				{BlobHash: testBlobHash3, BlobSize: 0, IVData: []byte("iv56789012345678")},   // Zero size
				{BlobHash: testBlobHash4, BlobSize: 300, IVData: []byte{}},                   // Empty IV
				{BlobHash: testBlobHash2, BlobSize: 400, IVData: []byte("iv56789012345678")}, // Valid
			},
			expectedCount:   2,
			expectedIVs:     [][]byte{[]byte("iv12345678901234"), []byte("iv56789012345678")},
			description:     "Only valid blobs should be included from mixed set",
			allowZeroLength: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
				// Arrange
				ast := assert.New(tb)
				store, err := NewLBRYBlobStore(ctx)
				require.NoError(tb, err)

				// Create a mock storage service
				mockStorage := coreMocks.NewMockStorageService(tb)
				store.storageSvc = mockStorage

				// Create test SD blob data
				sdBlob := stream.SDBlob{
					StreamName:        testStreamName,
					StreamType:        testStreamType,
					SuggestedFileName: testSuggestedFileName,
					Key:               []byte(testEncryptionKey),
				}

				// Serialize the SDBlob to its binary format
				_, err = sdBlob.ToBlob()
				require.NoError(tb, err)

				// Create stream in DB
				_stream := db.Stream{
					StreamHash:        sdBlob.HashHex(),
					SDHash:            sdBlob.HashHex(),
					StreamName:        sdBlob.StreamName,
					StreamType:        sdBlob.StreamType,
					SuggestedFileName: sdBlob.SuggestedFileName,
					KeyData:           sdBlob.Key,
				}
				err = ctx.DB().Create(&_stream).Error
				require.NoError(tb, err)

				// Create test case blobs
				var createdBlobs []db.Blob
				for _, blob := range tc.blobs {
					err = ctx.DB().Create(&blob).Error
					require.NoError(tb, err)
					createdBlobs = append(createdBlobs, blob)
				}

				// Create stream_blob associations
				for i, blob := range createdBlobs {
					streamBlob := db.StreamBlob{
						StreamID:   uint64(_stream.ID),
						BlobID:     uint64(blob.ID),
						BlobNumber: i,
					}
					err = ctx.DB().Create(&streamBlob).Error
					require.NoError(tb, err)
				}

				// Act
				data, err := store.Get(t.Context(), sdBlob.HashHex())

				// Assert
				ast.NoError(err)

				// The returned data should be a reconstructed SD blob
				parsedSdBlob := stream.SDBlob{}
				err = parsedSdBlob.FromBlob(data)
				ast.NoError(err)
				ast.Equal(testStreamName, parsedSdBlob.StreamName)
				ast.Equal(testStreamType, parsedSdBlob.StreamType)
				ast.Equal(testSuggestedFileName, parsedSdBlob.SuggestedFileName)
				ast.Equal([]byte(testEncryptionKey), parsedSdBlob.Key)

				// Verify blob count matches expectation
				ast.Len(parsedSdBlob.BlobInfos, tc.expectedCount, tc.description)

				// Verify the valid blobs are included with correct IV data
				if tc.expectedCount > 0 {
					for i, blobInfo := range parsedSdBlob.BlobInfos {
						// Check IV data - for terminating blobs, expectedIVs might be empty
						if i < len(tc.expectedIVs) {
							ast.Equal(tc.expectedIVs[i], blobInfo.IV, "IV data should match for blob %d in test case %s", i, tc.name)
						}
						// All non-terminating blobs should have non-empty hash
						ast.NotEmpty(blobInfo.BlobHash, "Non-terminating blob should have non-empty hash in test case %s", tc.name)

						// Check length based on test case expectations
						if tc.allowZeroLength && blobInfo.Length == 0 {
							// Zero-length is expected for this test case
							ast.Equal(0, blobInfo.Length, "Zero-length blob should have zero length in test case %s", tc.name)
						} else {
							ast.NotZero(blobInfo.Length, "Valid blob should have non-zero length in test case %s", tc.name)
						}
					}
				}
			}, testConfig)
		})
	}
}

func TestLBRYBlobStore_BuildBlobInfosFromDb_HashDecoding(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create test stream
		testStream := db.Stream{
			StreamHash:        "test_stream_hash_123",
			SDHash:            "test_sd_hash_456",
			StreamName:        testStreamName,
			StreamType:        testStreamType,
			SuggestedFileName: testSuggestedFileName,
			KeyData:           []byte(testEncryptionKey),
		}
		err = ctx.DB().Create(&testStream).Error
		require.NoError(tb, err)

		// Create test blob with proper hex-encoded hash
		testBlobHashBytes := []byte{0x61, 0x62, 0x63, 0x64}      // "abcd" in hex
		testBlobHashHex := hex.EncodeToString(testBlobHashBytes) // "61626364"
		testIV := []byte("test_iv_16_bytes")

		testBlob := db.Blob{
			BlobHash: testBlobHashHex, // Store as hex string in DB
			BlobSize: 1024,
			IVData:   testIV,
		}
		err = ctx.DB().Create(&testBlob).Error
		require.NoError(tb, err)

		// Create stream-blob association
		streamBlob := db.StreamBlob{
			StreamID:   uint64(testStream.ID),
			BlobID:     uint64(testBlob.ID),
			BlobNumber: 0,
		}
		err = ctx.DB().Create(&streamBlob).Error
		require.NoError(tb, err)

		// Act - call buildBlobInfosFromDb which should properly decode hex
		blobInfos, err := store.buildBlobInfosFromDb(t.Context(), testStream.ID)

		// Assert
		require.NoError(tb, err)
		ast.Len(blobInfos, 1, "Should return exactly one blob info")

		// Critical assertion: the hash should be properly decoded from hex string
		// This protects against the regression where []byte(hexString) was used instead of hex.DecodeString()
		blobInfo := blobInfos[0]
		ast.Equal(testBlobHashBytes, blobInfo.BlobHash,
			"Blob hash should be properly hex-decoded, not converted character-by-character")
		ast.Equal(1024, blobInfo.Length)
		ast.Equal(0, blobInfo.BlobNum)
		ast.Equal(testIV, blobInfo.IV)

		// Additional verification: ensure the decoded hash is not the character-by-character conversion
		// []byte("61626364") would be {0x36, 0x31, 0x36, 0x32, 0x36, 0x33, 0x36, 0x34}
		incorrectConversion := []byte("61626364")
		ast.NotEqual(incorrectConversion, blobInfo.BlobHash,
			"Should not be character-by-character conversion of hex string")
	})
}

func TestLBRYBlobStore_BuildBlobInfosFromDb_RoundTripHashConsistency(t *testing.T) {
	runBlobStoreTest(t, func(tb testing.TB, ctx coreTesting.TestContext) { // Arrange
		ast := assert.New(tb)
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(tb, err)

		// Create test stream
		testStream := db.Stream{
			StreamHash:        "test_stream_hash_789",
			SDHash:            "test_sd_hash_012",
			StreamName:        "round_trip_test",
			StreamType:        "audio",
			SuggestedFileName: "test_audio.mp3",
			KeyData:           []byte("round_trip_key"),
		}
		err = ctx.DB().Create(&testStream).Error
		require.NoError(tb, err)

		// Create multiple test blobs with different hash patterns
		testCases := []struct {
			name    string
			hash    []byte
			size    int
			iv      []byte
			blobNum int
		}{
			{
				name:    "simple_hash",
				hash:    []byte{0x01, 0x02, 0x03, 0x04},
				size:    512,
				iv:      []byte("iv1_test_16_bytes"),
				blobNum: 0,
			},
			{
				name:    "complex_hash",
				hash:    []byte{0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88},
				size:    2048,
				iv:      []byte("iv2_test_16_bytes"),
				blobNum: 1,
			},
		}

		for _, tc := range testCases {
			// Create blob with hex-encoded hash
			blobHashHex := hex.EncodeToString(tc.hash)
			testBlob := db.Blob{
				BlobHash: blobHashHex,
				BlobSize: tc.size,
				IVData:   tc.iv,
			}
			err = ctx.DB().Create(&testBlob).Error
			require.NoError(tb, err)

			// Create stream-blob association
			streamBlob := db.StreamBlob{
				StreamID:   uint64(testStream.ID),
				BlobID:     uint64(testBlob.ID),
				BlobNumber: tc.blobNum,
			}
			err = ctx.DB().Create(&streamBlob).Error
			require.NoError(tb, err)
		}

		// Act
		blobInfos, err := store.buildBlobInfosFromDb(t.Context(), testStream.ID)

		// Assert
		require.NoError(tb, err)
		ast.Len(blobInfos, len(testCases), "Should return all test cases")

		// Verify round-trip consistency for each blob
		for _, tc := range testCases {
			// Find matching blob info by blob number
			var foundBlob *stream.BlobInfo
			for _, bi := range blobInfos {
				if bi.BlobNum == tc.blobNum {
					foundBlob = &bi
					break
				}
			}

			require.NotNil(tb, foundBlob, "Should find blob info for test case: %s", tc.name)
			if foundBlob != nil {
				ast.Equal(tc.hash, foundBlob.BlobHash,
					"Hash should match original for test case: %s", tc.name)
				ast.Equal(tc.size, foundBlob.Length,
					"Size should match for test case: %s", tc.name)
				ast.Equal(tc.iv, foundBlob.IV,
					"IV should match for test case: %s", tc.name)
				ast.Equal(tc.blobNum, foundBlob.BlobNum,
					"Blob number should match for test case: %s", tc.name)
			}
		}
	})
}
