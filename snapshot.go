package sundial

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/sundayfun/sundial/codec"
)

// snapshot is one immutable encoded configuration state published for
// concurrent reads. The hash tracks content and metadata tracks the Provider
// state paired with the document.
type snapshot struct {
	data     []byte
	hash     [sha256.Size]byte
	metadata Metadata
}

func decodeSnapshot[T any](
	documentCodec codec.Codec,
	data []byte,
	metadata Metadata,
) (*snapshot, T, error) {
	config, err := decodeConfig[T](documentCodec, data)
	if err != nil {
		return nil, config, fmt.Errorf("sundial: decode configuration: %w", err)
	}

	return &snapshot{
		data:     cloneBytes(data),
		hash:     sha256.Sum256(data),
		metadata: metadata,
	}, config, nil
}

func decodeConfig[T any](documentCodec codec.Codec, data []byte) (T, error) {
	var config T
	if len(bytes.TrimSpace(data)) == 0 {
		return config, ErrEmptyDocument
	}
	if err := documentCodec.Decode(data, &config); err != nil {
		return config, err
	}
	return config, nil
}

func cloneBytes(data []byte) []byte {
	return append([]byte(nil), data...)
}
