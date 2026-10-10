package sundial

import (
	"context"
	"errors"
)

// Reload updates the in-memory content and revision from the Provider.
// On error, the previous snapshot is preserved.
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
		s.logger.DebugContext(ctx, "reloaded configuration", "revision_id", current.revision.ID)
	}
	return nil
}

func (s *Client[T]) reload(ctx context.Context) (*snapshot[T], bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	next, err := s.loadSnapshot(ctx)
	if err != nil {
		return nil, false, err
	}
	current := s.snapshot.Load()
	if next.hash == current.hash {
		if next.revision.ID != current.revision.ID {
			s.snapshot.Store(next)
		}
		return next, false, nil
	}

	s.snapshot.Store(next)
	return next, true, nil
}
