package durex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// S3Config configures the S3 storage backend.
type S3Config struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Config is the AWS SDK configuration.
	Config aws.Config

	// UsePathStyle enables path-style addressing, required for MinIO and LocalStack.
	UsePathStyle bool

	// MaxObjectSize is the maximum size of an S3 object that will be read.
	// Prevents OOM from maliciously large objects in the bucket.
	// Default: 10 MB
	MaxObjectSize int64
}

const defaultMaxObjectSize = 10 << 20 // 10 MB

// S3Storage implements [ObjectStorage] using an S3-compatible backend.
type S3Storage struct {
	client        *s3.Client
	bucket        string
	maxObjectSize int64
}

// NewS3Storage creates a new S3-backed storage.
func NewS3Storage(cfg S3Config) *S3Storage {
	client := s3.NewFromConfig(cfg.Config, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
	})
	maxSize := cfg.MaxObjectSize
	if maxSize == 0 {
		maxSize = defaultMaxObjectSize
	}
	return &S3Storage{client: client, bucket: cfg.Bucket, maxObjectSize: maxSize}
}

func (s *S3Storage) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *S3Storage) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	noneMatch := aws.String("*")
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        bytes.NewReader(data),
		IfNoneMatch: noneMatch,
	})
	if err != nil {
		// S3 returns either 409 ConditionalRequestConflict or
		// 412 PreconditionFailed when If-None-Match fails because
		// the object already exists.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "ConditionalRequestConflict", "PreconditionFailed":
				return ErrAlreadyExists
			}
		}
		return err
	}
	return nil
}

func (s *S3Storage) GetWithETag(ctx context.Context, key string) ([]byte, string, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	defer out.Body.Close()
	limited := io.LimitReader(out.Body, s.maxObjectSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) > s.maxObjectSize {
		return nil, "", fmt.Errorf("object %q exceeds maximum size (%d bytes)", key, s.maxObjectSize)
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return data, etag, nil
}

func (s *S3Storage) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  &s.bucket,
		Key:     &key,
		Body:    bytes.NewReader(data),
		IfMatch: &etag,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "ConditionalRequestConflict", "PreconditionFailed":
				return ErrPreconditionFailed
			}
		}
		return err
	}
	return nil
}

func (s *S3Storage) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer out.Body.Close()
	limited := io.LimitReader(out.Body, s.maxObjectSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > s.maxObjectSize {
		return nil, fmt.Errorf("object %q exceeds maximum size (%d bytes)", key, s.maxObjectSize)
	}
	return data, nil
}

// isS3NotFound checks all the ways S3 can signal "object does not exist":
// typed errors (NoSuchKey, NotFound) and string error codes.
func isS3NotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return false
}

func (s *S3Storage) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

func (s *S3Storage) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	return err
}
