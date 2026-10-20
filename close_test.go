package sundial_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sundayfun/sundial"
	staticprovider "github.com/sundayfun/sundial/provider/static"
	providertesting "github.com/sundayfun/sundial/provider/testing"
)

// Failure cases: Close returns before Watch exits; canceling one client cancels
// its parent; concurrent/repeated Close panics or blocks; polling continues after
// Close; parent cancellation prevents Close from returning; failed New starts Watch;
// reads panic or lose the last snapshot after Close; provider operations succeed after Close; a queued write bypasses closure;
// an already returned snapshot is invalidated by Close; slow Close is silent or
// returns early; repeated Close duplicates lifecycle logs.
type delayedCloseWatcher struct {
	*providertesting.Provider

	canceled chan struct{}
	release  chan struct{}
}

func (p *delayedCloseWatcher) Watch(ctx context.Context, _ func() error) error {
	<-ctx.Done()
	close(p.canceled)
	<-p.release
	return ctx.Err()
}

func TestCloseWaitsForWatcherAndPreservesParent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var output bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&output, nil))
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := &delayedCloseWatcher{
			Provider: providertesting.New([]byte(`{"counter":7}`)),
			canceled: make(chan struct{}), release: make(chan struct{}),
		}
		client, err := sundial.New[testConfig](parent, provider, sundial.WithLogger[testConfig](logger))
		require.NoError(t, err)
		entry := client.GetEntry()
		closed := make(chan struct{})
		go func() { client.Close(); close(closed) }()
		<-provider.canceled
		time.Sleep(6 * time.Second)
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("Close returned before Watch exited")
		default:
		}
		require.NoError(t, parent.Err())
		close(provider.release)
		<-closed
		assert.Equal(t, 7, entry.Value.Counter)
		assert.Equal(t, entry.Value, client.Get())
		assert.Equal(t, entry, client.GetEntry())
		client.Close()
		logs := output.String()
		assert.Equal(t, 1, strings.Count(logs, `level=INFO msg="closing sundial client"`))
		assert.Equal(t, 1, strings.Count(logs, `level=INFO msg="closed sundial client"`))
		assert.Equal(t, 1, strings.Count(logs, `level=WARN msg="waiting for automatic reload to stop"`))
		assert.Equal(t, 1, strings.Count(logs, `level=WARN msg="read configuration after close"`))
		assert.Equal(t, 1, strings.Count(logs, `level=WARN msg="read configuration entry after close"`))
		assert.NotContains(t, logs, "level=ERROR")
		assert.Contains(t, logs, sundial.ErrClosed.Error())
	})
}

func TestCloseStopsPolling(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		provider := providertesting.New([]byte(`{"counter":1}`))
		client, err := sundial.New[testConfig](t.Context(), provider)
		require.NoError(t, err)
		synctest.Wait()
		client.Close()
		reads := provider.GetCount()
		provider.SetData([]byte(`{"counter":2}`))
		time.Sleep(time.Minute)
		assert.Equal(t, reads, provider.GetCount())
		require.ErrorIs(t, client.Reload(t.Context()), sundial.ErrClosed)
		_, err = client.Update(t.Context(), func(*testConfig) error {
			t.Error("modify called after Close")
			return nil
		})
		require.ErrorIs(t, err, sundial.ErrClosed)
		assert.Equal(t, reads, provider.GetCount())
		assert.Zero(t, provider.PutIfRevisionCount())
	})
}

func TestCloseConcurrentStaticClient(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client, err := sundial.New[testConfig](t.Context(), staticprovider.NewProvider([]byte(`{"counter":1}`)))
		require.NoError(t, err)
		var group sync.WaitGroup
		for range 8 {
			group.Go(client.Close)
		}
		group.Wait()
		client.Close()
	})
}

func TestCloseAfterParentCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := &cancellationWatcher{Provider: providertesting.New([]byte(`{}`)), stopped: make(chan struct{})}
		client, err := sundial.New[testConfig](ctx, provider)
		require.NoError(t, err)
		synctest.Wait()
		cancel()
		<-provider.stopped
		client.Close()
	})
}

func TestFailedNewDoesNotStartWatcher(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		provider := &delayedCloseWatcher{Provider: providertesting.New(nil), canceled: make(chan struct{}), release: make(chan struct{})}
		client, err := sundial.New[testConfig](t.Context(), provider)
		require.ErrorIs(t, err, sundial.ErrNotFound)
		assert.Nil(t, client)
		// Any Watch goroutine here would remain blocked and fail the synctest bubble.
	})
}

func TestCloseRejectsHistoryOperations(t *testing.T) {
	t.Parallel()
	client, err := sundial.New[testConfig](t.Context(), staticprovider.NewProvider([]byte(`{"counter":1}`)))
	require.NoError(t, err)
	client.Close()
	_, _, err = client.GetRevision(t.Context(), "revision")
	require.ErrorIs(t, err, sundial.ErrClosed)
	_, err = client.ListRevisions(t.Context(), sundial.ListRevisionsOptions{})
	require.ErrorIs(t, err, sundial.ErrClosed)
	_, err = client.RestoreRevision(t.Context(), "revision", "")
	require.ErrorIs(t, err, sundial.ErrClosed)
}

func TestCloseRejectsConcurrentUpdate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		provider := providertesting.New([]byte(`{"counter":1}`))
		client, err := sundial.New[testConfig](t.Context(), provider)
		require.NoError(t, err)
		started := make(chan struct{})
		release := make(chan struct{})
		first := make(chan error, 1)
		go func() {
			_, updateErr := client.Update(t.Context(), func(cfg *testConfig) error {
				close(started)
				<-release
				cfg.Counter = 2
				return nil
			})
			first <- updateErr
		}()
		<-started
		second := make(chan error, 1)
		attempting := make(chan struct{})
		go func() {
			close(attempting)
			_, updateErr := client.Update(t.Context(), func(*testConfig) error {
				t.Error("queued modify called after Close")
				return nil
			})
			second <- updateErr
		}()
		<-attempting
		client.Close()
		close(release)
		require.NoError(t, <-first)
		require.ErrorIs(t, <-second, sundial.ErrClosed)
		assert.Equal(t, 1, provider.PutIfRevisionCount())
	})
}
