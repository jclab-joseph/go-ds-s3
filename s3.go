package s3ds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-ds-s3/pkg/filecache"
	disk "github.com/ipfs/go-ds-s3/pkg/minio-disk"
	logging "github.com/ipfs/go-log/v2"
)

const (
	// listMax is the largest amount of objects you can request from S3 in a list
	// call.
	listMax = 1000

	// deleteMax is the largest amount of objects you can delete from S3 in a
	// delete objects call.
	deleteMax = 1000

	defaultWorkers = 100

	// credsRefreshWindow, subtracted from the endpointcred's expiration time, is the
	// earliest time the endpoint creds can be refreshed.
	credsRefreshWindow = 2 * time.Minute
)

var (
	_   ds.Datastore = (*S3Bucket)(nil)
	log              = logging.Logger("godss3")
)

type S3Bucket struct {
	Config
	S3    s3API
	Cache filecache.FileCache
}

type s3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type Config struct {
	AccessKey           string
	SecretKey           string
	SessionToken        string
	Bucket              string
	Region              string
	RegionEndpoint      string
	RootDirectory       string
	Workers             int
	CredentialsEndpoint string
	KeyTransform        string
	CacheDirectory      string
	CacheCapacity       int64
}

var KeyTransforms = map[string]func(ds.Key) string{
	"default": func(k ds.Key) string {
		return k.String()
	},
	"suffix": func(k ds.Key) string {
		return k.String() + "/data"
	},
	"next-to-last/2": func(k ds.Key) string {
		s := k.String()
		s, _ = strings.CutPrefix(s, "/")
		offset := 1
		start := len(s) - 2 - offset
		return s[start:start+2] + "/" + s
	},
}

func NewS3Datastore(conf Config) (*S3Bucket, error) {
	logConfig := conf
	if logConfig.AccessKey != "" {
		logConfig.AccessKey = ""
	}
	if logConfig.SecretKey != "" {
		logConfig.SecretKey = ""
	}
	if logConfig.SessionToken != "" {
		logConfig.SessionToken = ""
	}
	log.Infof("creating new S3 datastore with config: %+v", logConfig)

	if conf.Workers == 0 {
		conf.Workers = defaultWorkers
	}

	loadOptions := []func(*config.LoadOptions) error{}
	if conf.Region != "" {
		loadOptions = append(loadOptions, config.WithRegion(conf.Region))
	}

	awsConfig, err := config.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %s", err)
	}
	awsConfig.Credentials = newCredentialProvider(conf, awsConfig)

	s3obj := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		if conf.RegionEndpoint != "" {
			o.BaseEndpoint = aws.String(conf.RegionEndpoint)
			o.UsePathStyle = true
		}
	})

	var cache filecache.FileCache
	if conf.CacheDirectory != "" {
		cacheImpl := filecache.NewDefaultCache(conf.CacheDirectory, nil, filecache.DefaultCacheComparer)
		cacheImpl.MaxItems = 262144
		cacheImpl.MaxSize = conf.CacheCapacity
		if cacheImpl.MaxSize <= 0 {
			info, err := disk.GetInfo(conf.CacheDirectory, false)
			if err == nil {
				cacheImpl.MaxSize = int64(float64(info.Total) * 0.8)
				log.Infof("[go-ds-s3] cache capacity is automatically set to %.2f GB", float64(cacheImpl.MaxSize)/filecache.Gigabyte)
			} else {
				cacheImpl.MaxSize = filecache.Gigabyte
				log.Infof("[go-ds-s3] could not get disk info for cache directory(%s): %+v", conf.CacheDirectory, err)
			}
		}
		cache = cacheImpl
	} else {
		cache = filecache.NewNoop()
	}
	if err = cache.Start(); err != nil {
		log.Infof("[go-ds-s3] cache(%s) failed to start: %+v", conf.CacheDirectory, err)
		cache = filecache.NewNoop()
	}

	return &S3Bucket{
		S3:     s3obj,
		Config: conf,
		Cache:  cache,
	}, nil
}

func (s *S3Bucket) Put(ctx context.Context, k ds.Key, value []byte) error {
	log.Debugf("put: %s", k)

	key := prepareKey(s.Config, k)

	_, err := s.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.s3Path(key)),
		Body:   bytes.NewReader(value),
	})
	if err == nil {
		s.writeToCache(key, value)
	}
	if err != nil {
		log.Errorf("put error on key %s: %v", k, err)
	}
	return err
}

func (s *S3Bucket) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

func (s *S3Bucket) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	var err error
	var resp *s3.GetObjectOutput
	keys := prepareKeyWithFallback(s.Config, k)

	log.Debugf("get: %s", k)

	cachedFile, cerr := s.Cache.Open(keys[0])
	if cerr == nil {
		body, cerr := io.ReadAll(cachedFile)
		if cerr == nil {
			return body, nil
		}
	}

	var body []byte
	for index, key := range keys {
		resp, err = s.S3.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.s3Path(key)),
		})

		if err == nil {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		if index == 1 && err == nil {
			_, _ = s.S3.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(s.Bucket),
				Key:    aws.String(s.s3Path(prepareKey(s.Config, k))),
				Body:   bytes.NewReader(body),
			})
		}

		if err == nil || !isNotFound(err) {
			break
		}
	}
	if err != nil {
		if isNotFound(err) {
			log.Debugf("get: %s not found", k)
			return nil, ds.ErrNotFound
		}
		log.Errorf("get error on key %s: %v", k, err)
		return nil, err
	}

	s.writeToCache(keys[0], body)

	return body, nil
}

func (s *S3Bucket) Has(ctx context.Context, k ds.Key) (exists bool, err error) {
	log.Debugf("has: %s", k)
	_, err = s.GetSize(ctx, k)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *S3Bucket) GetSize(ctx context.Context, k ds.Key) (size int, err error) {
	log.Debugf("get size: %s", k)

	var resp *s3.HeadObjectOutput
	keys := prepareKeyWithFallback(s.Config, k)

	for _, key := range keys {
		resp, err = s.S3.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.s3Path(key)),
		})
		if err == nil || !isNotFound(err) {
			break
		}
	}
	if err != nil {
		if isNotFound(err) {
			return -1, ds.ErrNotFound
		}
		log.Errorf("get size error on key %s: %v", k, err)
		return -1, err
	}
	return int(aws.ToInt64(resp.ContentLength)), nil
}

func (s *S3Bucket) Delete(ctx context.Context, k ds.Key) error {
	log.Debugf("delete: %s", k)

	var err error
	keys := prepareKeyWithFallback(s.Config, k)

	s.Cache.Remove(keys[0])

	for _, key := range keys {
		_, err = s.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.s3Path(key)),
		})
		if err == nil || !isNotFound(err) {
			break
		}
	}
	if isNotFound(err) {
		// delete is idempotent
		log.Debugf("delete: %s not found, idempotent", k)
		err = nil
	} else if err != nil {
		log.Errorf("delete error on key %s: %v", k, err)
	}
	return err
}

func (s *S3Bucket) writeToCache(k string, data []byte) {
	writer, err := s.Cache.Create(k)
	if err != nil {
		log.Infof("cache create failed: %+v", err)
		return
	}
	_, err = writer.Write(data)
	if err != nil {
		log.Infof("cache write failed: %+v", err)
		writer.Cancel()
	} else {
		writer.Close()
	}
}

func prepareKey(cfg Config, k ds.Key) string {
	return KeyTransforms[cfg.KeyTransform](k)
}

func prepareKeyWithFallback(cfg Config, k ds.Key) []string {
	keys := []string{KeyTransforms[cfg.KeyTransform](k)}
	if cfg.KeyTransform != "default" {
		keys = append(keys, KeyTransforms["default"](k))
	}
	return keys
}

func (s *S3Bucket) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	log.Debugf("query: %+v", q)
	if q.Orders != nil || q.Filters != nil {
		err := fmt.Errorf("s3ds: filters or orders are not supported")
		log.Error(err)
		return nil, err
	}

	// S3 store a "/foo" key as "foo" so we need to trim the leading "/"
	prefix := strings.TrimPrefix(q.Prefix, "/")

	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Bucket),
		Prefix:  aws.String(s.s3Path(prefix)),
		MaxKeys: aws.Int32(listMax),
	}

	// The iterator needs to be stateful across Next() calls.
	// The closure will capture these state variables.
	var (
		resp    *s3.ListObjectsV2Output
		err     error
		index   = 0
		started = false
		skipped = 0
		yielded = 0
	)

	nextValue := func() (dsq.Result, bool) {
		// Initial fetch on first call
		if !started {
			log.Debugf("query: initial list call for prefix %s", q.Prefix)
			resp, err = s.S3.ListObjectsV2(ctx, listInput)
			if err != nil {
				log.Errorf("query: list objects error: %v", err)
				return dsq.Result{Error: err}, false
			}
			started = true
		}

		for {
			// Have we yielded enough results according to limit?
			if q.Limit > 0 && yielded >= q.Limit {
				return dsq.Result{}, false
			}

			// Do we need to fetch the next page of results?
			for index >= len(resp.Contents) {
				if !aws.ToBool(resp.IsTruncated) {
					log.Debug("query: end of results")
					return dsq.Result{}, false
				}

				index = 0
				listInput.ContinuationToken = resp.NextContinuationToken
				log.Debugf("query: fetching next page with token %s", *resp.NextContinuationToken)
				resp, err = s.S3.ListObjectsV2(ctx, listInput)
				if err != nil {
					log.Errorf("query: list objects error on next page: %v", err)
					return dsq.Result{Error: err}, false
				}
			}

			// Have we skipped enough results according to offset?
			if skipped < q.Offset {
				skipped++
				index++
				continue
			}

			// If we are here, we have an entry to return.
			keyFromS3 := aws.ToString(resp.Contents[index].Key)
			dsKeyPath := strings.TrimPrefix(keyFromS3, s.RootDirectory)
			dsKeyPath = strings.TrimPrefix(dsKeyPath, "/")
			dsKeyTokens := strings.Split(dsKeyPath, "/")
			dsKeyPath = dsKeyTokens[len(dsKeyTokens)-1]

			entry := dsq.Entry{
				Key:  ds.NewKey(dsKeyPath).String(),
				Size: int(aws.ToInt64(resp.Contents[index].Size)),
			}
			if !q.KeysOnly {
				value, getErr := s.Get(ctx, ds.NewKey(entry.Key))
				if getErr != nil {
					return dsq.Result{Error: getErr}, false
				}
				entry.Value = value
			}

			index++
			yielded++
			return dsq.Result{Entry: entry}, true
		}
	}

	// We are handling offset and limit in our iterator.
	// We create a new query object for ResultsFromIterator to avoid it also
	// trying to apply them.
	cleanQuery := q
	cleanQuery.Offset = 0
	cleanQuery.Limit = 0

	return dsq.ResultsFromIterator(cleanQuery, dsq.Iterator{
		Close: func() error { return nil },
		Next:  nextValue,
	}), nil
}

func (s *S3Bucket) Batch(_ context.Context) (ds.Batch, error) {
	log.Debug("starting batch")
	return &s3Batch{
		s:          s,
		ops:        make(map[string]batchOp),
		numWorkers: s.Workers,
	}, nil
}

func (s *S3Bucket) Close() error {
	return nil
}

func (s *S3Bucket) s3Path(p string) string {
	return path.Join(s.RootDirectory, strings.TrimPrefix(p, "/"))
}

func isNotFound(err error) bool {
	var apiErr smithy.APIError
	ok := errors.As(err, &apiErr)
	return ok && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound")
}

type s3Batch struct {
	s          *S3Bucket
	ops        map[string]batchOp
	numWorkers int
}

type batchOp struct {
	val    []byte
	delete bool
}

func (b *s3Batch) Put(ctx context.Context, k ds.Key, val []byte) error {
	log.Debugf("batch put: %s", k)
	b.ops[k.String()] = batchOp{
		val:    val,
		delete: false,
	}
	return nil
}

func (b *s3Batch) Delete(ctx context.Context, k ds.Key) error {
	log.Debugf("batch delete: %s", k)
	b.ops[k.String()] = batchOp{
		val:    nil,
		delete: true,
	}
	return nil
}

func (b *s3Batch) Commit(ctx context.Context) error {
	log.Debugf("committing batch with %d operations", len(b.ops))
	var (
		deleteObjs []s3types.ObjectIdentifier
		putKeys    []ds.Key
	)
	for k, op := range b.ops {
		if op.delete {
			deleteObjs = append(deleteObjs, s3types.ObjectIdentifier{
				Key: aws.String(b.s.s3Path(k)),
			})
		} else {
			putKeys = append(putKeys, ds.NewKey(k))
		}
	}

	log.Debugf("batch commit: %d puts, %d deletes", len(putKeys), len(deleteObjs))

	numJobs := len(putKeys) + (len(deleteObjs) / deleteMax)
	if len(deleteObjs)%deleteMax > 0 {
		numJobs++
	}
	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	numWorkers := b.numWorkers
	if numJobs < numWorkers {
		numWorkers = numJobs
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	defer wg.Wait()

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			worker(jobs, results)
		}()
	}

	for _, k := range putKeys {
		jobs <- b.newPutJob(ctx, k, b.ops[k.String()].val)
	}

	if len(deleteObjs) > 0 {
		for i := 0; i < len(deleteObjs); i += deleteMax {
			limit := deleteMax
			if len(deleteObjs[i:]) < limit {
				limit = len(deleteObjs[i:])
			}

			jobs <- b.newDeleteJob(ctx, deleteObjs[i:i+limit])
		}
	}
	close(jobs)

	var errs []string
	for i := 0; i < numJobs; i++ {
		err := <-results
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err := fmt.Errorf("s3ds: failed batch operation:\n%s", strings.Join(errs, "\n"))
		log.Error(err)
		return err
	}

	log.Debug("batch commit successful")
	return nil
}

func (b *s3Batch) newPutJob(ctx context.Context, k ds.Key, value []byte) func() error {
	return func() error {
		return b.s.Put(ctx, k, value)
	}
}

func (b *s3Batch) newDeleteJob(ctx context.Context, objs []s3types.ObjectIdentifier) func() error {
	return func() error {
		log.Debugf("batch worker: deleting %d objects", len(objs))
		resp, err := b.s.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.s.Bucket),
			Delete: &s3types.Delete{
				Objects: objs,
			},
		})
		if err != nil && !isNotFound(err) {
			log.Errorf("batch worker: error deleting objects: %v", err)
			return err
		}

		var errs []string
		for _, err := range resp.Errors {
			if err.Code != nil && *err.Code == "NoSuchKey" {
				// idempotent
				continue
			}
			errs = append(errs, fmt.Sprintf("%s: %s", aws.ToString(err.Code), aws.ToString(err.Message)))
		}

		if len(errs) > 0 {
			err := fmt.Errorf("failed to delete objects: %s", errs)
			log.Error(err)
			return err
		}

		return nil
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}

func newCredentialProvider(conf Config, cfg aws.Config) aws.CredentialsProvider {
	var providers []aws.CredentialsProvider

	if roleARN, tokenFile := os.Getenv("AWS_ROLE_ARN"), os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"); roleARN != "" && tokenFile != "" {
		stsClient := sts.NewFromConfig(cfg)
		providers = append(providers, aws.NewCredentialsCache(
			stscreds.NewWebIdentityRoleProvider(stsClient, roleARN, stscreds.IdentityTokenFile(tokenFile)),
			func(o *aws.CredentialsCacheOptions) {
				o.ExpiryWindow = credsRefreshWindow
			},
		))
	}

	if conf.AccessKey != "" || conf.SecretKey != "" || conf.SessionToken != "" {
		providers = append(providers, credentials.NewStaticCredentialsProvider(conf.AccessKey, conf.SecretKey, conf.SessionToken))
	}

	if cfg.Credentials != nil {
		providers = append(providers, cfg.Credentials)
	}

	if conf.CredentialsEndpoint != "" {
		providers = append(providers, aws.NewCredentialsCache(
			endpointcreds.New(conf.CredentialsEndpoint),
			func(o *aws.CredentialsCacheOptions) {
				o.ExpiryWindow = credsRefreshWindow
			},
		))
	}

	if len(providers) == 1 {
		return providers[0]
	}

	return aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		var errs []string
		for _, provider := range providers {
			creds, err := provider.Retrieve(ctx)
			if err == nil && creds.HasKeys() {
				return creds, nil
			}
			if err != nil {
				errs = append(errs, err.Error())
			}
		}

		if len(errs) == 0 {
			return aws.Credentials{}, errors.New("failed to retrieve AWS credentials")
		}
		return aws.Credentials{}, fmt.Errorf("failed to retrieve AWS credentials: %s", strings.Join(errs, "; "))
	})
}

var _ ds.Batching = (*S3Bucket)(nil)
