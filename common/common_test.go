package common

import (
	"errors"
	"fmt"
	"testing"
)

func TestXXH64KnownVectors(t *testing.T) {
	// Official test vectors from the xxHash sanity test suite.
	cases := []struct {
		input string
		seed  uint64
		want  uint64
	}{
		{"", 0, 0xEF46DB3751D8E999},
		{"abc", 0, 0x44BC2CF5AD770999},
	}
	for _, c := range cases {
		if got := Sum64XXH64([]byte(c.input), c.seed); got != c.want {
			t.Errorf("Sum64XXH64(%q, %d) = %#x, want %#x", c.input, c.seed, got, c.want)
		}
	}
}

func TestXXH64Incremental(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i * 7)
	}
	want := Sum64XXH64(data, 42)

	// Feed the same data with irregular chunk sizes; results must match.
	for _, chunk := range []int{1, 3, 7, 16, 31, 32, 33, 64, 100, 1024} {
		h := NewXXH64(42)
		for i := 0; i < len(data); i += chunk {
			end := i + chunk
			if end > len(data) {
				end = len(data)
			}
			if _, err := h.Write(data[i:end]); err != nil {
				t.Fatal(err)
			}
		}
		if got := h.Sum64(); got != want {
			t.Errorf("chunk %d: incremental sum = %#x, want %#x", chunk, got, want)
		}
	}
}

func TestError(t *testing.T) {
	src := fmt.Errorf("underlying io issue")
	err := NewError(ErrorKindIo, "read device failed").
		WithContext("offset", 1024).
		WithContext("path", "/tmp/cache").
		WithSource(src)

	if err.Kind() != ErrorKindIo {
		t.Errorf("kind = %v, want Io", err.Kind())
	}
	if !errors.Is(err, src) {
		t.Error("errors.Is should find the source error")
	}
	want := "I/O error: read device failed, context: { offset: 1024, path: /tmp/cache }, source: underlying io issue"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestProperties(t *testing.T) {
	p := Properties{}
	if p.Hint != HintNormal || p.Location != LocationDefault || p.Age != AgeFresh {
		t.Fatalf("unexpected zero properties: %+v", p)
	}
	p = p.WithHint(HintLow).WithLocation(LocationOnDisk).WithAge(AgeYoung)
	if p.Hint != HintLow || p.Location != LocationOnDisk || p.Age != AgeYoung {
		t.Fatalf("unexpected updated properties: %+v", p)
	}
}
