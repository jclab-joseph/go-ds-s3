package s3ds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/ipfs/go-ds-s3/pkg/filecache"
)

func TestSuiteLocalS3(t *testing.T) {
	// Only run tests when LOCAL_S3 is set, since the tests are only set up for a local S3 endpoint.
	// To run tests locally, run `docker-compose up` in this repo in order to get a local S3 running
	// on port 9000. Then run `LOCAL_S3=true go test -v ./...` to execute tests.
	if _, localS3 := os.LookupEnv("LOCAL_S3"); !localS3 {
		t.Skipf("skipping test suite; LOCAL_S3 is not set.")
	}

	localBucketName, localBucketNameSet := os.LookupEnv("LOCAL_BUCKET_NAME")
	if !localBucketNameSet {
		localBucketName = fmt.Sprintf("localbucketname%d", time.Now().UnixNano())
	}

	config := Config{
		RegionEndpoint: "http://localhost:9000",
		Bucket:         localBucketName,
		Region:         "local",
		AccessKey:      "test",
		SecretKey:      "testdslocal",
		KeyTransform:   "default",
	}

	s3ds, err := NewS3Datastore(config)
	if err != nil {
		t.Fatal(err)
	}

	bucketClient, ok := s3ds.S3.(testBucketClient)
	if !ok {
		t.Fatalf("unexpected S3 client type %T", s3ds.S3)
	}
	if err = devMakeBucket(bucketClient, localBucketName); err != nil {
		t.Fatal(err)
	}

	t.Run("basic operations", func(t *testing.T) {
		dstest.SubtestBasicPutGet(t, s3ds)
	})
	t.Run("not found operations", func(t *testing.T) {
		dstest.SubtestNotFounds(t, s3ds)
	})
	t.Run("many puts and gets, query", func(t *testing.T) {
		dstest.SubtestManyKeysAndQuery(t, s3ds)
	})
	t.Run("return sizes", func(t *testing.T) {
		dstest.SubtestReturnSizes(t, s3ds)
	})
}

type testBucketClient interface {
	DeleteBucket(context.Context, *s3.DeleteBucketInput, ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
}

func devMakeBucket(s3obj testBucketClient, bucketName string) error {
	_, _ = s3obj.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	_, err := s3obj.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	return err
}

type mockS3Client struct {
	sync.RWMutex
	objects      map[string][]byte
	listPageSize int
	failGet      error
}

func (m *mockS3Client) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	buf, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = buf
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.failGet != nil {
		return nil, m.failGet
	}
	m.RLock()
	defer m.RUnlock()
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "not found"}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))),
	}, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.RLock()
	defer m.RUnlock()
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(data))),
	}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	delete(m.objects, *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.RLock()
	defer m.RUnlock()

	var allMatchingKeys []string
	for k := range m.objects {
		if strings.HasPrefix(k, *input.Prefix) {
			allMatchingKeys = append(allMatchingKeys, k)
		}
	}
	sort.Strings(allMatchingKeys)

	start := 0
	if input.ContinuationToken != nil {
		token := *input.ContinuationToken
		// In our mock, token is the key to start from
		i := sort.SearchStrings(allMatchingKeys, token)
		if i < len(allMatchingKeys) && allMatchingKeys[i] == token {
			start = i
		}
	}

	limit := int(*input.MaxKeys)
	if m.listPageSize > 0 && limit > m.listPageSize {
		limit = m.listPageSize
	}

	end := start + limit
	isTruncated := false
	var nextToken *string

	if end < len(allMatchingKeys) {
		isTruncated = true
		nextToken = &allMatchingKeys[end]
	} else {
		end = len(allMatchingKeys)
	}

	var contents []s3types.Object
	for _, k := range allMatchingKeys[start:end] {
		contents = append(contents, s3types.Object{
			Key:  aws.String(k),
			Size: aws.Int64(int64(len(m.objects[k]))),
		})
	}

	return &s3.ListObjectsV2Output{
		Contents:              contents,
		IsTruncated:           aws.Bool(isTruncated),
		NextContinuationToken: nextToken,
	}, nil
}

func (m *mockS3Client) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	m.Lock()
	defer m.Unlock()
	var deleted []s3types.DeletedObject
	var errors []s3types.Error
	for _, obj := range input.Delete.Objects {
		if _, ok := m.objects[*obj.Key]; ok {
			delete(m.objects, *obj.Key)
			deleted = append(deleted, s3types.DeletedObject{Key: obj.Key})
		} else {
			errors = append(errors, s3types.Error{
				Key:     obj.Key,
				Code:    aws.String("NoSuchKey"),
				Message: aws.String("The specified key does not exist."),
			})
		}
	}
	output := &s3.DeleteObjectsOutput{
		Deleted: deleted,
		Errors:  errors,
	}
	return output, nil
}

func newMockS3Datastore(t *testing.T) (*S3Bucket, *mockS3Client) {
	mockS3 := &mockS3Client{
		objects: make(map[string][]byte),
	}
	s3ds := &S3Bucket{
		S3: mockS3,
		Config: Config{
			Bucket:       "test-bucket",
			Workers:      10,
			KeyTransform: "default",
		},
		Cache: filecache.NewNoop(),
	}
	return s3ds, mockS3
}

func TestBatching(t *testing.T) {
	s3ds, _ := newMockS3Datastore(t)

	b, err := s3ds.Batch(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Test a mix of puts and deletes
	err = b.Put(context.Background(), ds.NewKey("key1"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Put(context.Background(), ds.NewKey("key2"), []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Delete(context.Background(), ds.NewKey("key3")) // was never there
	if err != nil {
		t.Fatal(err)
	}
	err = b.Put(context.Background(), ds.NewKey("key4"), []byte("value4"))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Delete(context.Background(), ds.NewKey("key4")) // put then delete
	if err != nil {
		t.Fatal(err)
	}

	err = b.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Check state
	val, err := s3ds.Get(context.Background(), ds.NewKey("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value1" {
		t.Errorf("unexpected value for key1: got %s, want value1", string(val))
	}

	val, err = s3ds.Get(context.Background(), ds.NewKey("key2"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value2" {
		t.Errorf("unexpected value for key2: got %s, want value2", string(val))
	}

	exists, err := s3ds.Has(context.Background(), ds.NewKey("key3"))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("key3 should not exist")
	}

	exists, err = s3ds.Has(context.Background(), ds.NewKey("key4"))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("key4 should not exist")
	}
}

func TestQueryPagination(t *testing.T) {
	s3ds, mockS3 := newMockS3Datastore(t)
	mockS3.listPageSize = 5

	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("key/%02d", i)
		err := s3ds.Put(context.Background(), ds.NewKey(key), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	q := dsq.Query{Prefix: "/key/"}
	results, err := s3ds.Query(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 12 {
		t.Fatalf("expected 12 entries, got %d", len(entries))
	}

	for i, entry := range entries {
		expectedKey := fmt.Sprintf("/key/%02d", i)
		if entry.Key != expectedKey {
			t.Errorf("unexpected key at index %d: got %s, want %s", i, entry.Key, expectedKey)
		}
		expectedValue := fmt.Sprintf("value-%d", i)
		if string(entry.Value) != expectedValue {
			t.Errorf("unexpected value for key %s: got %s, want %s", entry.Key, string(entry.Value), expectedValue)
		}
	}
}

func TestQueryWithOffset(t *testing.T) {
	s3ds, mockS3 := newMockS3Datastore(t)
	mockS3.listPageSize = 5

	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key/%02d", i)
		err := s3ds.Put(context.Background(), ds.NewKey(key), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	q := dsq.Query{Prefix: "/key/", Offset: 7, Limit: 5}
	results, err := s3ds.Query(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}

	for i, entry := range entries {
		expectedIdx := i + 7
		expectedKey := fmt.Sprintf("/key/%02d", expectedIdx)
		if entry.Key != expectedKey {
			t.Errorf("unexpected key at index %d: got %s, want %s", i, entry.Key, expectedKey)
		}
		expectedValue := fmt.Sprintf("value-%d", expectedIdx)
		if string(entry.Value) != expectedValue {
			t.Errorf("unexpected value for key %s: got %s, want %s", entry.Key, string(entry.Value), expectedValue)
		}
	}
}

func TestGetError(t *testing.T) {
	s3ds, mockS3 := newMockS3Datastore(t)
	mockS3.failGet = &smithy.GenericAPIError{Code: "InternalError", Message: "something broke"}

	_, err := s3ds.Get(context.Background(), ds.NewKey("anykey"))
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	var awsErr smithy.APIError
	ok := errors.As(err, &awsErr)
	if !ok {
		t.Fatalf("expected smithy.APIError, got %T", err)
	}
	if awsErr.ErrorCode() != "InternalError" {
		t.Errorf("unexpected error code: got %s, want InternalError", awsErr.ErrorCode())
	}
}

func TestDeleteIdempotent(t *testing.T) {
	s3ds, _ := newMockS3Datastore(t)
	err := s3ds.Delete(context.Background(), ds.NewKey("non-existent"))
	if err != nil {
		t.Fatalf("expected no error on deleting non-existent key, got %v", err)
	}
}
