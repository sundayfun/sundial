// Package s3 provides a Sundial Provider backed by one standard S3 object.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/sundayfun/sundial"
)

// Config identifies the S3 object that stores one configuration document.
type Config struct {
	// Region overrides the region resolved by the AWS SDK when non-empty.
	Region string
	// Bucket contains the configuration object.
	Bucket string
	// PathPrefix optionally namespaces Key within Bucket.
	PathPrefix string
	// Key identifies the configuration object in Bucket.
	Key string
	// Endpoint optionally overrides the standard AWS S3 endpoint.
	Endpoint string
	// UsePathStyle forces bucket names into request paths instead of hostnames.
	UsePathStyle bool
	// WatchInterval controls how often Watch checks the object ETag.
	// The default is 30 seconds.
	WatchInterval time.Duration
}

type s3Client interface {
	HeadObject(
		ctx context.Context,
		params *awss3.HeadObjectInput,
		optFns ...func(*awss3.Options),
	) (*awss3.HeadObjectOutput, error)
	GetObject(
		ctx context.Context,
		params *awss3.GetObjectInput,
		optFns ...func(*awss3.Options),
	) (*awss3.GetObjectOutput, error)
	PutObject(
		ctx context.Context,
		params *awss3.PutObjectInput,
		optFns ...func(*awss3.Options),
	) (*awss3.PutObjectOutput, error)
}

// Provider stores one complete configuration document in an S3 object.
type Provider struct {
	client        s3Client
	bucket        string
	key           string
	watchInterval time.Duration
}

var (
	_ sundial.Provider = (*Provider)(nil)
	_ sundial.Watcher  = (*Provider)(nil)
	_ s3Client         = (*awss3.Client)(nil)
)

func New[T any](
	ctx context.Context,
	cfg *Config,
	opts ...sundial.Option[T],
) (*sundial.Client[T], error) {
	provider, err := NewProvider(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return sundial.New[T](ctx, provider, opts...)
}

// NewProvider creates an S3 Provider using the AWS SDK default configuration chain.
func NewProvider(ctx context.Context, cfg *Config) (*Provider, error) {
	if cfg == nil {
		return nil, ErrConfigRequired
	}
	if cfg.Bucket == "" {
		return nil, ErrBucketRequired
	}
	if cfg.Key == "" {
		return nil, ErrKeyRequired
	}
	if cfg.WatchInterval < 0 {
		return nil, ErrWatchIntervalInvalid
	}
	normalized := *cfg
	if normalized.WatchInterval == 0 {
		normalized.WatchInterval = defaultWatchInterval
	}

	loadOptions := make([]func(*awsconfig.LoadOptions) error, 0, 1)
	if cfg.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.Region))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("s3: load AWS configuration: %w", err)
	}

	client := awss3.NewFromConfig(awsConfig, func(options *awss3.Options) {
		if cfg.Endpoint != "" {
			options.BaseEndpoint = &cfg.Endpoint
		}
		options.UsePathStyle = cfg.UsePathStyle
	})
	return newProvider(client, &normalized), nil
}

func newProvider(client s3Client, cfg *Config) *Provider {
	return &Provider{
		client:        client,
		bucket:        cfg.Bucket,
		key:           prefixedKey(cfg.PathPrefix, cfg.Key),
		watchInterval: cfg.WatchInterval,
	}
}

func prefixedKey(pathPrefix, key string) string {
	if pathPrefix == "" {
		return key
	}
	return strings.TrimRight(pathPrefix, "/") + "/" + strings.TrimLeft(key, "/")
}

// Get reads the current object and uses its ETag as the revision.
func (p *Provider) Get(ctx context.Context) ([]byte, sundial.Metadata, error) {
	output, err := p.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &p.bucket,
		Key:    &p.key,
	})
	if err != nil {
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
			switch apiErr.ErrorCode() {
			case errorCodeNoSuchKey, errorCodeNotFound:
				return nil, sundial.Metadata{},
					fmt.Errorf("s3: get object: %w: %w", sundial.ErrNotFound, err)
			}
		}
		return nil, sundial.Metadata{}, fmt.Errorf("s3: get object: %w", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, sundial.Metadata{}, fmt.Errorf("s3: read object: %w", err)
	}
	if output.ETag == nil || *output.ETag == "" {
		return nil, sundial.Metadata{}, ErrEmptyETag
	}
	return data, sundial.Metadata{Revision: *output.ETag}, nil
}

// Put writes the object without checking its current ETag.
func (p *Provider) Put(ctx context.Context, data []byte) (sundial.Metadata, error) {
	output, err := p.client.PutObject(ctx, &awss3.PutObjectInput{
		Body:   bytes.NewReader(data),
		Bucket: &p.bucket,
		Key:    &p.key,
	})
	if err != nil {
		return sundial.Metadata{}, fmt.Errorf("s3: put object: %w", err)
	}
	if output.ETag == nil || *output.ETag == "" {
		return sundial.Metadata{}, ErrEmptyETag
	}
	return sundial.Metadata{Revision: *output.ETag}, nil
}

// PutIfRevision replaces an existing object only when its ETag matches the
// expected revision.
func (p *Provider) PutIfRevision(
	ctx context.Context,
	data []byte,
	expectedMetadata sundial.Metadata,
) (sundial.Metadata, error) {
	if expectedMetadata.Revision == "" {
		return sundial.Metadata{}, fmt.Errorf("s3: put object: %w", sundial.ErrConflict)
	}

	input := &awss3.PutObjectInput{
		Body:    bytes.NewReader(data),
		Bucket:  &p.bucket,
		Key:     &p.key,
		IfMatch: &expectedMetadata.Revision,
	}

	output, err := p.client.PutObject(ctx, input)
	if err != nil {
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
			switch apiErr.ErrorCode() {
			case errorCodePreconditionFailed, errorCodeConditionalRequestConflict:
				return sundial.Metadata{},
					fmt.Errorf("s3: put object: %w: %w", sundial.ErrConflict, err)
			case errorCodeNoSuchKey, errorCodeNotFound:
				return sundial.Metadata{},
					fmt.Errorf("s3: put object: %w: %w", sundial.ErrConflict, err)
			}
		}
		return sundial.Metadata{}, fmt.Errorf("s3: put object: %w", err)
	}
	if output.ETag == nil || *output.ETag == "" {
		return sundial.Metadata{}, ErrEmptyETag
	}
	return sundial.Metadata{Revision: *output.ETag}, nil
}
