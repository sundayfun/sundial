package s3_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sundayfun/sundial"
	s3provider "github.com/sundayfun/sundial/provider/s3"
)

const integrationWatchInterval = 20 * time.Millisecond

var integrationBucketSequence atomic.Uint64

type integrationConfig struct {
	Port int `json:"port"`
}

// TestIntegrationS3Provider verifies the public Provider API against MinIO.
// Set SUNDIAL_S3_ENDPOINT and AWS credentials to run it.
func TestIntegrationS3Provider(t *testing.T) {
	t.Parallel()

	factory := newMinIOTestFactory(t, t.Context())

	t.Run("new creates Client", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			initialData: []byte(`{"port":8080}`),
		})
		store, err := s3provider.New[integrationConfig](ctx, fixture.config)
		require.NoError(t, err)

		entry, err := store.Get()
		require.NoError(t, err)
		assert.Equal(t, 8080, entry.Value.Port)
		assert.NotEmpty(t, entry.Metadata.Revision)
	})

	t.Run("get missing object", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{})
		_, _, err := fixture.provider.Get(ctx)
		require.ErrorIs(t, err, sundial.ErrNotFound)
		requireAPIErrorCode(t, err, "NoSuchKey")
	})

	t.Run("put creates object", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{})
		created, err := fixture.provider.Put(ctx, []byte(`{"port":8080}`))
		require.NoError(t, err)
		assert.NotEmpty(t, created.Revision)
		loaded := requireStoredObject(t, ctx, fixture.provider, `{"port":8080}`)
		assert.Equal(t, created.Revision, loaded.Revision)
	})

	t.Run("put overwrites object", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			initialData: []byte(`{"port":8080}`),
		})
		_, loaded, err := fixture.provider.Get(ctx)
		require.NoError(t, err)

		updated, err := fixture.provider.Put(ctx, []byte(`{"port":9090}`))
		require.NoError(t, err)
		assert.NotEmpty(t, updated.Revision)
		assert.NotEqual(t, loaded.Revision, updated.Revision)
		reloaded := requireStoredObject(t, ctx, fixture.provider, `{"port":9090}`)
		assert.Equal(t, updated.Revision, reloaded.Revision)
	})

	t.Run("put with matching revision", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			initialData: []byte(`{"port":8080}`),
		})
		_, loaded, err := fixture.provider.Get(ctx)
		require.NoError(t, err)

		updated, err := fixture.provider.PutIfRevision(ctx, []byte(`{"port":9090}`), loaded)
		require.NoError(t, err)
		assert.NotEmpty(t, updated.Revision)
		assert.NotEqual(t, loaded.Revision, updated.Revision)
		reloaded := requireStoredObject(t, ctx, fixture.provider, `{"port":9090}`)
		assert.Equal(t, updated.Revision, reloaded.Revision)
	})

	t.Run("reject stale update from another provider", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			initialData: []byte(`{"port":8080}`),
		})
		providerB := fixture.newProvider(t, ctx)
		_, loadedA, err := fixture.provider.Get(ctx)
		require.NoError(t, err)
		_, loadedB, err := providerB.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, loadedA.Revision, loadedB.Revision)

		_, err = fixture.provider.PutIfRevision(ctx, []byte(`{"writer":"A"}`), loadedA)
		require.NoError(t, err)
		_, err = providerB.PutIfRevision(ctx, []byte(`{"writer":"B"}`), loadedB)
		require.ErrorIs(t, err, sundial.ErrConflict)
		requireStoredObject(t, ctx, fixture.provider, `{"writer":"A"}`)
	})

	t.Run("only one concurrent writer succeeds", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			initialData: []byte(`{"port":8080}`),
		})
		runConcurrentWriters(t, ctx, fixture)
	})

	t.Run("reject stale update after delete", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			initialData: []byte(`{"port":8080}`),
		})
		_, loaded, err := fixture.provider.Get(ctx)
		require.NoError(t, err)
		fixture.deleteObject(t, ctx)

		_, err = fixture.provider.PutIfRevision(ctx, []byte(`{"port":9090}`), loaded)
		require.ErrorIs(t, err, sundial.ErrConflict)
		requireAPIErrorCode(t, err, "NoSuchKey", "NotFound", "PreconditionFailed")
	})

	t.Run("preserve missing bucket errors", func(t *testing.T) {
		t.Parallel()

		ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
			withoutBucket: true,
		})
		_, _, getErr := fixture.provider.Get(ctx)
		require.Error(t, getErr)
		require.NotErrorIs(t, getErr, sundial.ErrNotFound)
		requireAPIErrorCode(t, getErr, "NoSuchBucket")

		_, putErr := fixture.provider.Put(ctx, []byte(`{"port":8080}`))
		require.Error(t, putErr)
		requireAPIErrorCode(t, putErr, "NoSuchBucket")

		_, conditionalErr := fixture.provider.PutIfRevision(
			ctx,
			[]byte(`{"port":9090}`),
			sundial.Metadata{Revision: `"missing"`},
		)
		require.Error(t, conditionalErr)
		require.NotErrorIs(t, conditionalErr, sundial.ErrConflict)
		requireAPIErrorCode(t, conditionalErr, "NoSuchBucket")
	})
}

func TestIntegrationS3Watch(t *testing.T) {
	t.Parallel()

	factory := newMinIOTestFactory(t, t.Context())
	ctx, fixture := newIntegrationFixture(t, factory, minIOFixtureConfig{
		watchInterval: integrationWatchInterval,
	})

	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	notifications := make(chan int, 8)
	notifyCalls := 0
	go func() {
		done <- fixture.provider.Watch(watchCtx, func() error {
			notifyCalls++
			notifications <- notifyCalls
			return nil
		})
	}()

	waitForIntegrationNotification(t, notifications, 1)
	assertNoIntegrationNotification(t, notifications)

	_, err := fixture.provider.Put(ctx, []byte(`{"state":"created"}`))
	require.NoError(t, err)
	waitForIntegrationNotification(t, notifications, 2)
	assertNoIntegrationNotification(t, notifications)

	_, err = fixture.provider.Put(ctx, []byte(`{"state":"updated"}`))
	require.NoError(t, err)
	waitForIntegrationNotification(t, notifications, 3)
	assertNoIntegrationNotification(t, notifications)

	fixture.deleteObject(t, ctx)
	waitForIntegrationNotification(t, notifications, 4)
	assertNoIntegrationNotification(t, notifications)

	cancel()
	require.ErrorIs(t, waitForIntegrationWatch(t, done), context.Canceled)
}

type writeResult struct {
	name string
	err  error
}

func runConcurrentWriters(t *testing.T, ctx context.Context, fixture *minIOFixture) {
	t.Helper()

	providerB := fixture.newProvider(t, ctx)
	_, loaded, err := fixture.provider.Get(ctx)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan writeResult, 2)
	var writers sync.WaitGroup
	for _, writer := range []struct {
		name     string
		provider *s3provider.Provider
	}{
		{name: "A", provider: fixture.provider},
		{name: "B", provider: providerB},
	} {
		writers.Go(func() {
			<-start
			_, putErr := writer.provider.PutIfRevision(
				ctx,
				[]byte(fmt.Sprintf(`{"writer":%q}`, writer.name)),
				loaded,
			)
			results <- writeResult{name: writer.name, err: putErr}
		})
	}
	close(start)
	writers.Wait()
	close(results)

	var winner string
	conflicts := 0
	for result := range results {
		if result.err == nil {
			winner = result.name
			continue
		}
		require.ErrorIs(t, result.err, sundial.ErrConflict)
		conflicts++
	}
	require.NotEmpty(t, winner)
	assert.Equal(t, 1, conflicts)

	data, _, err := fixture.provider.Get(ctx)
	require.NoError(t, err)
	assert.JSONEq(t, fmt.Sprintf(`{"writer":%q}`, winner), string(data))
}

func requireStoredObject(
	t *testing.T,
	ctx context.Context,
	provider *s3provider.Provider,
	wantData string,
) sundial.Metadata {
	t.Helper()

	data, metadata, err := provider.Get(ctx)
	require.NoError(t, err)
	assert.JSONEq(t, wantData, string(data))
	return metadata
}

func waitForIntegrationNotification(t *testing.T, notifications <-chan int, want int) {
	t.Helper()

	select {
	case got := <-notifications:
		assert.Equal(t, want, got)
	case <-time.After(3 * time.Second):
		t.Fatalf("Watch() did not make notification call %d", want)
	}
}

func assertNoIntegrationNotification(t *testing.T, notifications <-chan int) {
	t.Helper()

	select {
	case got := <-notifications:
		t.Fatalf("Watch() made unexpected notification call %d", got)
	case <-time.After(4 * integrationWatchInterval):
	}
}

func waitForIntegrationWatch(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Watch() did not stop")
		return nil
	}
}

type minIOTestFactory struct {
	admin    *awss3.Client
	endpoint string
	region   string
}

type minIOFixtureConfig struct {
	initialData   []byte
	watchInterval time.Duration
	withoutBucket bool
}

type minIOFixture struct {
	provider *s3provider.Provider
	config   *s3provider.Config
	admin    *awss3.Client
	bucket   string
	key      string
}

func newMinIOTestFactory(t *testing.T, ctx context.Context) *minIOTestFactory {
	t.Helper()

	endpoint := os.Getenv("SUNDIAL_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SUNDIAL_S3_ENDPOINT is not set")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	require.NoError(t, err)
	admin := awss3.NewFromConfig(awsConfig, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	return &minIOTestFactory{
		admin:    admin,
		endpoint: endpoint,
		region:   region,
	}
}

func newIntegrationFixture(
	t *testing.T,
	factory *minIOTestFactory,
	config minIOFixtureConfig,
) (context.Context, *minIOFixture) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx, factory.newFixture(t, ctx, config)
}

func (f *minIOTestFactory) newFixture(
	t *testing.T,
	ctx context.Context,
	fixtureConfig minIOFixtureConfig,
) *minIOFixture {
	t.Helper()

	bucket := fmt.Sprintf(
		"sundial-minio-%d-%d",
		time.Now().UnixNano(),
		integrationBucketSequence.Add(1),
	)
	key := "config/app.json"
	if !fixtureConfig.withoutBucket {
		_, err := f.admin.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()

			_, deleteObjectErr := f.admin.DeleteObject(cleanupCtx, &awss3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			assert.NoError(t, deleteObjectErr)
			_, deleteBucketErr := f.admin.DeleteBucket(cleanupCtx, &awss3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
			assert.NoError(t, deleteBucketErr)
		})
	}
	if fixtureConfig.initialData != nil {
		_, err := f.admin.PutObject(ctx, &awss3.PutObjectInput{
			Body:   bytes.NewReader(fixtureConfig.initialData),
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
	}

	config := &s3provider.Config{
		Region:        f.region,
		Bucket:        bucket,
		Key:           key,
		Endpoint:      f.endpoint,
		UsePathStyle:  true,
		WatchInterval: fixtureConfig.watchInterval,
	}
	provider, err := s3provider.NewProvider(ctx, config)
	require.NoError(t, err)
	return &minIOFixture{
		provider: provider,
		config:   config,
		admin:    f.admin,
		bucket:   bucket,
		key:      key,
	}
}

func (f *minIOFixture) newProvider(t *testing.T, ctx context.Context) *s3provider.Provider {
	t.Helper()

	provider, err := s3provider.NewProvider(ctx, f.config)
	require.NoError(t, err)
	return provider
}

func (f *minIOFixture) deleteObject(t *testing.T, ctx context.Context) {
	t.Helper()

	_, err := f.admin.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})
	require.NoError(t, err)
}

func requireAPIErrorCode(t *testing.T, err error, want ...string) {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, want, apiErr.ErrorCode())
}
