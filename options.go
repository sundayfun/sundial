package sundial

import (
	"log/slog"

	"github.com/sundayfun/sundial/codec"
	jsoncodec "github.com/sundayfun/sundial/codec/json"
)

type options[T any] struct {
	Codec  codec.Codec
	Logger *slog.Logger
	Reload reloadOptions[T]
}

type reloadOptions[T any] struct {
	OnChange func(Entry[T])
	OnError  func(error)
}

// Option configures a Client.
type Option[T any] func(*options[T])

// WithCodec configures the document codec. JSON is used by default.
func WithCodec[T any](value codec.Codec) Option[T] {
	return func(opts *options[T]) {
		if value != nil {
			opts.Codec = value
		}
	}
}

// WithLogger configures structured debug and automatic reload error logging.
func WithLogger[T any](logger *slog.Logger) Option[T] {
	return func(opts *options[T]) {
		if logger != nil {
			opts.Logger = logger
		}
	}
}

// WithOnChange sets the callback run after a changed configuration is published.
func WithOnChange[T any](callback func(Entry[T])) Option[T] {
	return func(opts *options[T]) {
		opts.Reload.OnChange = callback
	}
}

// WithOnError sets the automatic reload error callback.
func WithOnError[T any](callback func(error)) Option[T] {
	return func(opts *options[T]) {
		opts.Reload.OnError = callback
	}
}

func normalizeOptions[T any](optionFunctions []Option[T]) options[T] {
	normalized := options[T]{
		Codec:  jsoncodec.New(),
		Logger: slog.New(slog.DiscardHandler),
		Reload: reloadOptions[T]{
			OnChange: nil,
			OnError:  nil,
		},
	}
	for _, option := range optionFunctions {
		if option != nil {
			option(&normalized)
		}
	}

	return normalized
}
