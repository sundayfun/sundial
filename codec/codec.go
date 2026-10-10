// Package codec defines configuration document encoding and decoding.
package codec

// Codec converts between configuration documents and Go values.
type Codec interface {
	// Encode returns the document encoding of v without modifying v.
	Encode(v any) ([]byte, error)
	// Decode parses a document into v. It must create independent mutable data,
	// without retaining or modifying data or reusing mutable values from earlier calls.
	Decode(data []byte, v any) error
}
