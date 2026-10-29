package storage

import (
	"bytes"
	"encoding/gob"

	"blob-cache/common"
)

// Codec serializes cache keys and values for the disk cache. It is the
// counterpart of foyer's `Code` trait bound on storage keys/values.
type Codec[K comparable, V any] struct {
	EncodeKey   func(K) ([]byte, error)
	DecodeKey   func([]byte) (K, error)
	EncodeValue func(V) ([]byte, error)
	DecodeValue func([]byte) (V, error)
}

// GobCodec returns a codec encoding keys and values with encoding/gob. It
// works for most concrete types (strings, integers, structs, []byte...).
func GobCodec[K comparable, V any]() *Codec[K, V] {
	return &Codec[K, V]{
		EncodeKey: func(k K) ([]byte, error) {
			var b bytes.Buffer
			if err := gob.NewEncoder(&b).Encode(&k); err != nil {
				return nil, common.NewError(common.ErrorKindParse, "encode key failed").WithSource(err)
			}
			return b.Bytes(), nil
		},
		DecodeKey: func(b []byte) (K, error) {
			var k K
			if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&k); err != nil {
				return k, common.NewError(common.ErrorKindParse, "decode key failed").WithSource(err)
			}
			return k, nil
		},
		EncodeValue: func(v V) ([]byte, error) {
			var b bytes.Buffer
			if err := gob.NewEncoder(&b).Encode(&v); err != nil {
				return nil, common.NewError(common.ErrorKindParse, "encode value failed").WithSource(err)
			}
			return b.Bytes(), nil
		},
		DecodeValue: func(b []byte) (V, error) {
			var v V
			if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v); err != nil {
				return v, common.NewError(common.ErrorKindParse, "decode value failed").WithSource(err)
			}
			return v, nil
		},
	}
}

// StringKeyCodec returns a codec with string keys, encoding keys as their
// raw bytes.
func StringKeyCodec[V any](encodeValue func(V) ([]byte, error), decodeValue func([]byte) (V, error)) *Codec[string, V] {
	return &Codec[string, V]{
		EncodeKey:   func(k string) ([]byte, error) { return []byte(k), nil },
		DecodeKey:   func(b []byte) (string, error) { return string(b), nil },
		EncodeValue: encodeValue,
		DecodeValue: decodeValue,
	}
}

// BytesCodec is the codec for string keys and []byte values, both stored
// as raw bytes.
func BytesCodec() *Codec[string, []byte] {
	return StringKeyCodec(
		func(v []byte) ([]byte, error) { return v, nil },
		func(b []byte) ([]byte, error) { return b, nil },
	)
}

// estimatedEntrySize estimates the serialized size of an entry by encoding
// it. It is called on the enqueue path only.
func estimatedEntrySize[K comparable, V any](codec *Codec[K, V], key K, value V) (keyLen, valueLen int, err error) {
	kb, err := codec.EncodeKey(key)
	if err != nil {
		return 0, 0, err
	}
	vb, err := codec.EncodeValue(value)
	if err != nil {
		return 0, 0, err
	}
	return len(kb), len(vb), nil
}
