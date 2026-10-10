package sundial

import (
	"context"
	"fmt"
)

// GetRevision returns a detached historical configuration value and its revision.
func (s *Client[T]) GetRevision(ctx context.Context, revisionID string) (T, Revision, error) {
	var zero T
	provider, ok := s.provider.(RevisionManager)
	if !ok {
		return zero, Revision{}, ErrUnsupported
	}
	data, info, err := provider.GetRevision(ctx, revisionID)
	if err != nil {
		return zero, Revision{}, fmt.Errorf("sundial: get revision: %w", err)
	}
	value, err := decodeConfig[T](s.codec, data)
	if err != nil {
		return zero, Revision{}, fmt.Errorf("sundial: decode revision: %w", err)
	}
	return value, info, nil
}

// ListRevisions returns immutable revisions in newest-first order.
func (s *Client[T]) ListRevisions(
	ctx context.Context,
	opts ListRevisionsOptions,
) ([]Revision, error) {
	provider, ok := s.provider.(RevisionManager)
	if !ok {
		return nil, ErrUnsupported
	}
	if opts.Limit <= 0 {
		opts.Limit = DefaultListRevisionsLimit
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	revisions, err := provider.ListRevisions(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("sundial: list revisions: %w", err)
	}
	return revisions, nil
}

// RestoreRevision publishes targetRevisionID as a new revision if currentRevisionID
// still matches storage. currentRevisionID is the revision observed by the caller;
// an empty or stale ID returns ErrConflict.
// It decodes the historical content before writing and preserves its original bytes.
// The returned Entry is shared and read-only.
func (s *Client[T]) RestoreRevision(
	ctx context.Context,
	targetRevisionID string,
	currentRevisionID string,
) (Entry[T], error) {
	provider, ok := s.provider.(RevisionManager)
	if !ok {
		return Entry[T]{}, ErrUnsupported
	}

	if currentRevisionID == "" {
		return Entry[T]{}, fmt.Errorf("sundial: restore revision: %w", ErrConflict)
	}

	data, _, err := provider.GetRevision(ctx, targetRevisionID)
	if err != nil {
		return Entry[T]{}, fmt.Errorf("sundial: restore revision: %w", err)
	}
	var zeroRevision Revision
	next, err := s.decodeSnapshot(data, zeroRevision)
	if err != nil {
		return Entry[T]{}, err
	}
	// Serialize publication and the snapshot update; history reads need no lock.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	revision, err := s.provider.PutIfRevision(ctx, data, currentRevisionID)
	if err != nil {
		return Entry[T]{}, fmt.Errorf("sundial: restore revision: %w", err)
	}
	next.revision = revision
	s.snapshot.Store(next)
	return s.entry(next), nil
}
