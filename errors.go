package sundial

import "errors"

var (
	// ErrClosed reports an operation on a closed Client.
	ErrClosed = errors.New("sundial: client closed")
	// ErrNotFound reports a missing configuration document.
	ErrNotFound = errors.New("sundial: not found")
	// ErrConflict reports a failed write condition, including an empty or stale revision ID.
	ErrConflict = errors.New("sundial: conflict")
	// ErrEmptyDocument reports an empty configuration document.
	ErrEmptyDocument = errors.New("sundial: empty configuration document")
	// ErrReadOnly reports a write rejected by a read-only Provider.
	ErrReadOnly = errors.New("sundial: read-only provider")
	// ErrUnsupported reports that a Provider does not implement an optional capability.
	ErrUnsupported = errors.New("sundial: unsupported operation")
	// ErrInvalidRevision reports corrupt immutable revision data.
	ErrInvalidRevision = errors.New("sundial: invalid revision")
)

// IsNotFound reports whether err indicates a missing configuration document.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsConflict reports whether err indicates a failed write condition.
func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}

// IsUnsupported reports whether err indicates an unsupported Provider capability.
func IsUnsupported(err error) bool {
	return errors.Is(err, ErrUnsupported)
}

// IsInvalidRevision reports whether err indicates corrupt immutable revision data.
func IsInvalidRevision(err error) bool {
	return errors.Is(err, ErrInvalidRevision)
}
