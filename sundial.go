package sundial

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/sundayfun/sundial/codec"
)

// Client manages one typed configuration document and its in-memory state.
type Client[T any] struct {
	provider Provider
	codec    codec.Codec
	logger   *slog.Logger

	writeMu  sync.Mutex
	snapshot atomic.Pointer[snapshot]
}

// Entry pairs a detached configuration value with the Provider metadata from
// the same in-memory snapshot.
type Entry[T any] struct {
	Value    T
	Metadata Metadata
}

// New loads the configuration and reloads it until ctx is canceled.
func New[T any](ctx context.Context, provider Provider, opts ...Option[T]) (*Client[T], error) {
	normalized := normalizeOptions(opts)
	s := &Client[T]{
		provider: provider,
		codec:    normalized.Codec,
		logger:   normalized.Logger,
		writeMu:  sync.Mutex{},
		snapshot: atomic.Pointer[snapshot]{},
	}

	loaded, _, err := s.loadSnapshot(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "load configuration", "error", err)
		return nil, err
	}
	s.snapshot.Store(loaded)
	s.logger.DebugContext(ctx, "loaded configuration", "revision", loaded.metadata.Revision)

	go s.watch(ctx, normalized.Reload)

	return s, nil
}

// Get returns the current detached Entry from memory.
func (s *Client[T]) Get() (Entry[T], error) {
	current := s.snapshot.Load()
	config, err := decodeConfig[T](s.codec, current.data)
	if err != nil {
		return Entry[T]{Value: config, Metadata: Metadata{Revision: ""}},
			fmt.Errorf("sundial: decode configuration: %w", err)
	}
	return Entry[T]{Value: config, Metadata: current.metadata}, nil
}

// Put saves entry when its metadata revision is current, then updates memory.
// It returns the codec-decoded saved Entry with its new metadata. A stale
// revision returns ErrConflict.
func (s *Client[T]) Put(ctx context.Context, entry Entry[T]) (Entry[T], error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	data, err := s.codec.Encode(entry.Value)
	if err != nil {
		putErr := fmt.Errorf("sundial: encode configuration: %w", err)
		s.logger.ErrorContext(ctx, "put configuration", "error", putErr)
		return Entry[T]{}, putErr
	}
	next, savedValue, err := decodeSnapshot[T](s.codec, data, Metadata{Revision: ""})
	if err != nil {
		s.logger.ErrorContext(ctx, "put configuration", "error", err)
		return Entry[T]{}, err
	}
	metadata, err := s.provider.PutIfRevision(ctx, data, entry.Metadata)
	if err != nil {
		putErr := fmt.Errorf("sundial: put configuration: %w", err)
		s.logger.ErrorContext(ctx, "put configuration", "error", putErr)
		return Entry[T]{}, putErr
	}

	next.metadata = metadata
	s.snapshot.Store(next)
	s.logger.DebugContext(ctx, "put configuration", "revision", metadata.Revision)
	return Entry[T]{Value: savedValue, Metadata: metadata}, nil
}

func (s *Client[T]) loadSnapshot(ctx context.Context) (*snapshot, Entry[T], error) {
	data, metadata, err := s.provider.Get(ctx)
	if err != nil {
		return nil, Entry[T]{}, fmt.Errorf("sundial: get configuration: %w", err)
	}

	next, config, err := decodeSnapshot[T](s.codec, data, metadata)
	if err != nil {
		return nil, Entry[T]{}, err
	}
	return next, Entry[T]{Value: config, Metadata: metadata}, nil
}
