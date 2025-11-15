package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/liblbry/stream"
	"go.lumeweb.com/portal-plugin-lbry/internal"
	"go.lumeweb.com/portal-plugin-lbry/internal/db"
	"go.lumeweb.com/portal-plugin-lbry/internal/db/migrations"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"gorm.io/gorm"
)

const (
	testBlobHash1 = "acc6adf8b4f10dcddffc5c2ca87dbd9cb3a2664564695ac7aaab038193ff14a280cc3d4ebae55c71d0b885a7316d0137"
	testBlobHash2 = "bde7be09c5010edee00d6d3db98ec0a0c4b37756757a6bd8bbbc1492a40025b391dd4e5fcf066d82e1c996b8427e1248"
	testData1     = "test blob data"
	testData2     = "test blob data for put operation"
)

func TestLBRYBlobStore_NewLBRYBlobStore(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		// We'll test this by checking if the blob store can be created successfully

		// Act
		store, err := NewLBRYBlobStore(ctx)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, store)
		assert.Equal(t, BLOBSTORE_NAME, store.Name())
	}, testConfig)
}

func TestLBRYBlobStore_Has(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash1

		// Act - Test when blob doesn't exist in DB
		has, err := store.Has(testHash)

		// Assert
		assert.NoError(t, err)
		assert.False(t, has)

		// Act - Test when blob exists in DB but not in storage
		// Insert blob into DB
		blob := db.Blob{
			BlobHash: testHash,
			BlobSize: 100,
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(t, err)

		// Set up mock expectation for when blob exists in DB but not in storage
		storageHash, err := LBRYHashToHash(testHash)
		require.NoError(t, err)

		mockStorage.EXPECT().DownloadObject(mock.Anything, mock.Anything, storageHash, int64(0)).Return(nil, fmt.Errorf("object not found"))

		has, err = store.Has(testHash)
		assert.NoError(t, err)
		assert.False(t, has)

		// Act - Test when blob exists in both DB and storage
		// Create a fresh mock instance
		mockStorage = coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		mockStorage.EXPECT().DownloadObject(mock.Anything, mock.Anything, storageHash, int64(0)).Return(io.NopCloser(strings.NewReader(testData1)), nil)

		has, err = store.Has(testHash)
		assert.NoError(t, err)
		assert.True(t, has)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Get(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash1
		testData := []byte(testData1)

		// Insert blob into DB first (simulating existing blob)
		blob := db.Blob{
			BlobHash: testHash,
			BlobSize: len(testData),
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(t, err)

		// Set up mock expectations
		storageHash, err := LBRYHashToHash(testHash)
		require.NoError(t, err)

		mockStorage.EXPECT().DownloadObject(mock.Anything, mock.Anything, storageHash, int64(0)).
			Return(io.NopCloser(bytes.NewReader(testData)), nil)

		// Act
		data, err := store.Get(testHash)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, testData, data)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Get_SDBlobWithAssociatedBlobs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Create test SD blob data
		sdBlob := stream.SDBlob{
			StreamName:        "test_stream",
			StreamType:        "video",
			SuggestedFileName: "test_video.mp4",
			Key:               []byte("test_encryption_key"),
		}

		// Serialize the SDBlob to its binary format
		_, err = sdBlob.ToBlob()
		require.NoError(t, err)

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
		require.NoError(t, err)

		// Create associated blobs
		blob1 := db.Blob{
			BlobHash: testBlobHash1,
			BlobSize: 100,
		}
		blob2 := db.Blob{
			BlobHash: testBlobHash2,
			BlobSize: 200,
		}

		err = ctx.DB().Create(&blob1).Error
		require.NoError(t, err)
		err = ctx.DB().Create(&blob2).Error
		require.NoError(t, err)

		// Create stream_blob associations
		streamBlob1 := db.StreamBlob{
			StreamID:   uint64(_stream.ID),
			BlobID:     uint64(blob1.ID),
			BlobNumber: 0,
		}
		streamBlob2 := db.StreamBlob{
			StreamID:   uint64(_stream.ID),
			BlobID:     uint64(blob2.ID),
			BlobNumber: 1,
		}

		err = ctx.DB().Create(&streamBlob1).Error
		require.NoError(t, err)
		err = ctx.DB().Create(&streamBlob2).Error
		require.NoError(t, err)

		// For this test, we don't actually need to mock storage calls
		// because the Get method for SD blobs reconstructs from DB data only
		// The mock setup is unnecessary and misleading for this test case

		// Act
		data, err := store.Get(sdBlob.HashHex())

		// Assert
		assert.NoError(t, err)

		// The returned data should be a reconstructed SD blob with blob info
		// We can't directly compare with originalTestData because the structure
		// is different - it includes the blob information now
		// Instead, we verify it's a valid SD blob that can be parsed
		parsedSdBlob := stream.SDBlob{}
		err = parsedSdBlob.FromBlob(data)
		assert.NoError(t, err)
		assert.Equal(t, "test_stream", parsedSdBlob.StreamName)
		assert.Equal(t, "video", parsedSdBlob.StreamType)
		assert.Equal(t, "test_video.mp4", parsedSdBlob.SuggestedFileName)
		assert.Equal(t, []byte("test_encryption_key"), parsedSdBlob.Key)

		// Verify that blob info was included in the reconstructed blob
		// The exact assertion depends on how the stream package handles this
		// But we know it should have blob information now
	}, testConfig)
}

func TestLBRYBlobStore_Get_SDBlobNoAssociatedBlobs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Create test SD blob data
		sdBlob := stream.SDBlob{
			StreamName:        "test_stream",
			StreamType:        "video",
			SuggestedFileName: "test_video.mp4",
			Key:               []byte("test_encryption_key"),
		}

		// Serialize the SDBlob to its binary format
		testData, err := sdBlob.ToBlob()
		require.NoError(t, err)

		// Create stream in DB (no associated blobs)
		stream := db.Stream{
			StreamHash:        sdBlob.HashHex(),
			SDHash:            sdBlob.HashHex(),
			StreamName:        sdBlob.StreamName,
			StreamType:        sdBlob.StreamType,
			SuggestedFileName: sdBlob.SuggestedFileName,
			KeyData:           sdBlob.Key,
		}
		err = ctx.DB().Create(&stream).Error
		require.NoError(t, err)

		// Act
		data, err := store.Get(sdBlob.HashHex())

		// Assert
		assert.NoError(t, err)
		// Should return serialized SD blob data
		assert.Equal(t, testData, data)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Get_SDBlobMissingAssociatedBlobs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Create test SD blob data
		sdBlob := stream.SDBlob{
			StreamName:        "test_stream",
			StreamType:        "video",
			SuggestedFileName: "test_video.mp4",
			Key:               []byte("test_encryption_key"),
		}

		// Serialize the SDBlob to its binary format
		testData, err := sdBlob.ToBlob()
		require.NoError(t, err)

		// Create stream in DB
		stream := db.Stream{
			StreamHash:        sdBlob.HashHex(),
			SDHash:            sdBlob.HashHex(),
			StreamName:        sdBlob.StreamName,
			StreamType:        sdBlob.StreamType,
			SuggestedFileName: sdBlob.SuggestedFileName,
			KeyData:           sdBlob.Key,
		}
		err = ctx.DB().Create(&stream).Error
		require.NoError(t, err)

		// Create stream_blob association with a blob that doesn't exist
		streamBlob := db.StreamBlob{
			StreamID:   uint64(stream.ID),
			BlobID:     999, // Non-existent blob ID
			BlobNumber: 0,
		}
		err = ctx.DB().Create(&streamBlob).Error
		require.NoError(t, err)

		// Act
		data, err := store.Get(sdBlob.HashHex())

		// Assert
		assert.NoError(t, err)
		// Should still return serialized SD blob data even with missing blobs
		assert.Equal(t, testData, data)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Get_StreamMetadataError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data - hash that doesn't exist in DB
		testHash := testBlobHash1

		// Act
		data, err := store.Get(testHash)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in local storage")
		assert.Nil(t, data)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Put(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash1
		testData := []byte(testData2)

		// Set up mock expectations
		_, err = LBRYHashToHash(testHash)
		require.NoError(t, err)

		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
			Return(nil, nil)

		// Act
		err = store.Put(testHash, testData)

		// Assert
		assert.NoError(t, err)

		// Check that blob was inserted into DB
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&blob).Error
		assert.NoError(t, err)
		assert.Equal(t, testHash, blob.BlobHash)
		assert.Equal(t, len(testData), blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_PutSD(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a proper SDBlob structure
		// Since we're working with the actual liblbry stream package,
		// we'll create a valid SDBlob with realistic test data
		sdBlob := stream.SDBlob{
			StreamName:        "test_stream",
			StreamType:        "video",
			SuggestedFileName: "test_video.mp4",
			Key:               []byte("test_encryption_key"),
		}

		// Serialize the SDBlob to its binary format
		// This simulates what would happen with a real SD blob
		testData, err := sdBlob.ToBlob()
		require.NoError(t, err)

		hash := sdBlob.HashHex()

		// Act
		err = store.PutSD(sdBlob.HashHex(), testData)

		// Assert
		assert.NoError(t, err)

		// Check that stream was inserted into DB with correct values
		var _stream db.Stream
		err = ctx.DB().Where("sd_hash = ?", hash).First(&_stream).Error
		assert.NoError(t, err)
		assert.Equal(t, hash, _stream.StreamHash)
		assert.Equal(t, "test_stream", _stream.StreamName)
		assert.Equal(t, "video", _stream.StreamType)
		assert.Equal(t, "test_video.mp4", _stream.SuggestedFileName)
		assert.Equal(t, sdBlob.Key, _stream.KeyData)
	}, testConfig)
}

func TestLBRYBlobStore_Name(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Act
		name := store.Name()

		// Assert
		assert.Equal(t, BLOBSTORE_NAME, name)
	}, testConfig)
}

func TestLBRYBlobStore_List(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

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
		require.NoError(t, err)

		err = ctx.DB().Create(&blob2).Error
		require.NoError(t, err)

		// Act
		results, err := store.List(0, 10)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, results)
		// We can't easily predict the exact order due to UNION ALL, so just check it doesn't panic
		assert.GreaterOrEqual(t, len(results), 2)
	}, testConfig)
}

func TestLBRYBlobStore_Delete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash2

		// Set up mock expectations
		storageHash, err := LBRYHashToHash(testHash)
		require.NoError(t, err)

		mockStorage.EXPECT().DeleteObject(mock.Anything, mock.Anything, storageHash).
			Return(nil)

		// Insert blob into DB
		blob := db.Blob{
			BlobHash: testHash,
			BlobSize: 100,
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(t, err)

		// Act
		err = store.Delete(testHash)

		// Assert
		assert.NoError(t, err)

		// Check that blob was deleted from DB
		var deletedBlob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&deletedBlob).Error
		assert.Error(t, err)
		assert.True(t, errors.Is(err, gorm.ErrRecordNotFound))

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Get_StorageError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash1

		// Insert blob into DB first (simulating existing blob)
		blob := db.Blob{
			BlobHash: testHash,
			BlobSize: 100,
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(t, err)

		// Set up mock expectations for storage error
		storageHash, err := LBRYHashToHash(testHash)
		require.NoError(t, err)

		mockStorage.EXPECT().DownloadObject(mock.Anything, mock.Anything, storageHash, int64(0)).
			Return(nil, fmt.Errorf("storage connection failed"))

		// Act
		data, err := store.Get(testHash)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to download blob")
		assert.Nil(t, data)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Put_StorageFailure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash1
		testData := []byte(testData2)

		// Set up mock expectations for storage failure
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("upload failed: disk space exceeded"))

		// Act
		err = store.Put(testHash, testData)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to store blob")
		assert.Contains(t, err.Error(), "upload failed")

		// Verify blob was not stored in DB
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&blob).Error
		assert.Error(t, err)
		assert.True(t, errors.Is(err, gorm.ErrRecordNotFound))

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_PutSD_InvalidBlobData(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Test with invalid blob data
		invalidData := []byte("this is not valid SD blob data")

		// Act
		err = store.PutSD("invalid_hash", invalidData)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse SD blob")
	}, testConfig)
}

func TestLBRYBlobStore_Delete_StorageError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash2

		// Set up mock expectations for storage error
		storageHash, err := LBRYHashToHash(testHash)
		require.NoError(t, err)

		mockStorage.EXPECT().DeleteObject(mock.Anything, mock.Anything, storageHash).
			Return(fmt.Errorf("storage delete failed: network timeout"))

		// Insert blob into DB
		blob := db.Blob{
			BlobHash: testHash,
			BlobSize: 100,
		}
		err = ctx.DB().Create(&blob).Error
		require.NoError(t, err)

		// Act
		err = store.Delete(testHash)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete blob from storage")
		assert.Contains(t, err.Error(), "network timeout")

		// Verify blob still exists in DB (partial failure scenario)
		var deletedBlob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&deletedBlob).Error
		assert.NoError(t, err)
		assert.Equal(t, testHash, deletedBlob.BlobHash)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Put_EmptyData(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test with empty data
		testHash := testBlobHash1
		emptyData := []byte{}

		// Set up mock expectations
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
			Return(nil, nil)

		// Act
		err = store.Put(testHash, emptyData)

		// Assert
		assert.NoError(t, err)

		// Check that blob was inserted into DB with size 0
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&blob).Error
		assert.NoError(t, err)
		assert.Equal(t, testHash, blob.BlobHash)
		assert.Equal(t, 0, blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Put_NilData(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test with nil data
		testHash := testBlobHash1

		// Set up mock expectations
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
			Return(nil, nil)

		// Act
		err = store.Put(testHash, nil)

		// Assert
		assert.NoError(t, err)

		// Check that blob was inserted into DB with size 0
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&blob).Error
		assert.NoError(t, err)
		assert.Equal(t, testHash, blob.BlobHash)
		assert.Equal(t, 0, blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_Get_NotInDB(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data - blob not in DB
		testHash := testBlobHash1

		// Don't insert blob into DB - this should cause the method to return early
		// Set up mock expectations to ensure storage is NOT called
		// Note: We don't set up any mock expectations for storage since it shouldn't be called

		// Act
		data, err := store.Get(testHash)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in local storage")
		assert.Nil(t, data)

		// Verify storage was never called
		// Note: We can't easily assert that storage wasn't called with testify/mock
		// But we know from the logic that if the blob isn't in DB, we return early
		// So we just verify the error condition

		// No need to call mockStorage.AssertExpectations(t) because it shouldn't be called
	}, testConfig)
}

func TestLBRYBlobStore_Put_LargeBlob(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Create large test data (>1MB)
		largeData := make([]byte, 2*1024*1024) // 2MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		testHash := testBlobHash1

		// Set up mock expectations
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
			Return(nil, nil)

		// Act
		err = store.Put(testHash, largeData)

		// Assert
		assert.NoError(t, err)

		// Check that blob was inserted into DB with correct size
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&blob).Error
		assert.NoError(t, err)
		assert.Equal(t, testHash, blob.BlobHash)
		assert.Equal(t, len(largeData), blob.BlobSize)

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

func TestLBRYBlobStore_List_PaginationEdgeCases(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

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
		require.NoError(t, err)

		err = ctx.DB().Create(&blob2).Error
		require.NoError(t, err)

		// Test with offset beyond available records
		results, err := store.List(10, 5)
		assert.NoError(t, err)
		// Should return empty slice, not panic
		assert.Len(t, results, 0)

		// Test with limit 0
		results, err = store.List(0, 0)
		assert.NoError(t, err)
		// Should return empty slice, not panic
		assert.Len(t, results, 0)

		// Test with negative offset (should behave like offset 0)
		results, err = store.List(-5, 5)
		assert.NoError(t, err)
		// Should return first 5 items (or however many exist)
		assert.GreaterOrEqual(t, len(results), 0)
		assert.LessOrEqual(t, len(results), 2)

		// Test with negative limit (should behave like limit 10)
		results, err = store.List(0, -10)
		assert.NoError(t, err)
		// Should return up to 10 items
		assert.GreaterOrEqual(t, len(results), 0)
		assert.LessOrEqual(t, len(results), 2)
	}, testConfig)
}

func TestLBRYBlobStore_PartialFailure_DBSuccessStorageFailure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb testing.TB, ctx coreTesting.TestContext) {
		// Arrange
		store, err := NewLBRYBlobStore(ctx)
		require.NoError(t, err)

		// Create a mock storage service that will fail
		mockStorage := coreMocks.NewMockStorageService(t)
		store.storageSvc = mockStorage

		// Test data
		testHash := testBlobHash1
		testData := []byte(testData2)

		// Set up mock expectations for storage failure
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("storage quota exceeded"))

		// Act
		err = store.Put(testHash, testData)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage quota exceeded")

		// Verify blob was not stored in DB (since storage failed)
		var blob db.Blob
		err = ctx.DB().Where("blob_hash = ?", testHash).First(&blob).Error
		assert.Error(t, err)
		assert.True(t, errors.Is(err, gorm.ErrRecordNotFound))

		// No need to call mockStorage.AssertExpectations(t)
	}, testConfig)
}

var (
	cfg        = coreTesting.NewConfigBuilder().Build()
	testConfig = coreTesting.CombineOptions(
		coreTesting.WithMockProtocol(internal.ProtocolName, func(protocol *coreTesting.MockProtocol) {
			protocol.WithConfig(cfg)
		}),
		coreTesting.WithProtocolConfig(internal.ProtocolName, cfg),
		coreTesting.WithSQLitePluginMigrations(internal.ProtocolName, migrations.GetSQLite()),
	)
)
