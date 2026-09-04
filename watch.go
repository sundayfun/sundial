package sundial

import (
	"context"
	"errors"
	"time"
)

const (
	// defaultPollingInterval is the fallback interval for Providers without Watch support.
	defaultPollingInterval = 30 * time.Second
	// watcherRetryInterval is the retry delay for any Watcher that exits.
	watcherRetryInterval = 30 * time.Second
)

func (s *Client[T]) watch(ctx context.Context, opts reloadOptions[T]) {
	watcher, native := s.provider.(Watcher)
	if !native {
		s.poll(ctx, opts)
		return
	}

	for {
		err := s.runWatcher(ctx, watcher, opts)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}

		timer := time.NewTimer(watcherRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Client[T]) runWatcher(ctx context.Context, watcher Watcher, opts reloadOptions[T]) error {
	var reloadErr error
	err := watcher.Watch(ctx, func() error {
		reloadErr = s.autoReload(ctx, opts)
		return reloadErr
	})
	if err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, reloadErr) {
		s.logger.ErrorContext(
			ctx,
			"automatic reload failed",
			"operation",
			"watch provider",
			"error",
			err,
		)
		if opts.OnError != nil {
			opts.OnError(err)
		}
	}
	return err
}

func (s *Client[T]) poll(ctx context.Context, opts reloadOptions[T]) {
	ticker := time.NewTicker(defaultPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.autoReload(ctx, opts); errors.Is(err, context.Canceled) {
				return
			}
		}
	}
}

func (s *Client[T]) autoReload(ctx context.Context, opts reloadOptions[T]) error {
	entry, changed, err := s.reload(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		s.logger.ErrorContext(
			ctx,
			"automatic reload failed",
			"operation",
			"reload configuration",
			"error",
			err,
		)
		if opts.OnError != nil {
			opts.OnError(err)
		}
		return err
	}
	if changed && opts.OnChange != nil {
		opts.OnChange(entry)
	}
	if changed {
		current := s.snapshot.Load()
		s.logger.DebugContext(ctx, "reloaded configuration", "revision", current.metadata.Revision)
	}
	return nil
}
