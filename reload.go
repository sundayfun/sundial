package sundial

import (
	"context"
	"errors"
)

// Reload replaces the in-memory state when the Provider content changed.
func (s *Client[T]) Reload(ctx context.Context) error {
	_, changed, err := s.reload(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.logger.ErrorContext(ctx, "reload configuration", "error", err)
		}
		return err
	}
	if changed {
		current := s.snapshot.Load()
		s.logger.DebugContext(ctx, "reloaded configuration", "revision", current.metadata.Revision)
	}
	return nil
}

func (s *Client[T]) reload(ctx context.Context) (Entry[T], bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	next, entry, err := s.loadSnapshot(ctx)
	if err != nil {
		return Entry[T]{}, false, err
	}
	current := s.snapshot.Load()
	if next.hash == current.hash {
		if next.metadata.Revision != current.metadata.Revision {
			s.snapshot.Store(next)
		}
		return entry, false, nil
	}

	s.snapshot.Store(next)
	return entry, true, nil
}
