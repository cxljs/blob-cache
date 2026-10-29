package common

import (
	"fmt"
	"strings"
)

// ErrorKind classifies the errors returned by blob-cache, mirroring
// foyer's `ErrorKind`.
type ErrorKind int

const (
	// ErrorKindIo indicates an I/O error.
	ErrorKindIo ErrorKind = iota
	// ErrorKindExternal indicates an error from external code (e.g. a
	// user-provided fetch function).
	ErrorKindExternal
	// ErrorKindConfig indicates an invalid configuration.
	ErrorKindConfig
	// ErrorKindChannelClosed indicates an internal channel was closed.
	ErrorKindChannelClosed
	// ErrorKindTaskCancelled indicates a task was cancelled.
	ErrorKindTaskCancelled
	// ErrorKindJoin indicates a task join failure.
	ErrorKindJoin
	// ErrorKindParse indicates a parse error.
	ErrorKindParse
	// ErrorKindBufferSizeLimit indicates the buffer size limit was exceeded;
	// the caller may allocate a larger buffer and retry.
	ErrorKindBufferSizeLimit
	// ErrorKindChecksumMismatch indicates a checksum mismatch.
	ErrorKindChecksumMismatch
	// ErrorKindMagicMismatch indicates a magic number mismatch.
	ErrorKindMagicMismatch
	// ErrorKindOutOfRange indicates an out-of-range access.
	ErrorKindOutOfRange
	// ErrorKindNoSpace indicates there is no space left.
	ErrorKindNoSpace
	// ErrorKindClosed indicates the cache is closed.
	ErrorKindClosed
	// ErrorKindRecover indicates a recovery error.
	ErrorKindRecover
	// ErrorKindUnsupported indicates an unsupported operation.
	ErrorKindUnsupported
)

func (k ErrorKind) String() string {
	switch k {
	case ErrorKindIo:
		return "I/O error"
	case ErrorKindExternal:
		return "External error"
	case ErrorKindConfig:
		return "Config error"
	case ErrorKindChannelClosed:
		return "Channel closed"
	case ErrorKindTaskCancelled:
		return "Task cancelled"
	case ErrorKindJoin:
		return "Join error"
	case ErrorKindParse:
		return "Parse error"
	case ErrorKindBufferSizeLimit:
		return "Buffer size limit exceeded"
	case ErrorKindChecksumMismatch:
		return "Checksum mismatch"
	case ErrorKindMagicMismatch:
		return "Magic mismatch"
	case ErrorKindOutOfRange:
		return "Out of range"
	case ErrorKindNoSpace:
		return "No space"
	case ErrorKindClosed:
		return "Closed"
	case ErrorKindRecover:
		return "Recover error"
	case ErrorKindUnsupported:
		return "Unsupported operation"
	default:
		return "Unknown error"
	}
}

// Error is the error type returned by all blob-cache functions, mirroring
// foyer's `Error` with kinds, ordered key-value contexts and a source error.
type Error struct {
	kind    ErrorKind
	message string
	context []contextPair
	source  error
}

type contextPair struct {
	key   string
	value any
}

// NewError creates an Error with the given kind and message.
func NewError(kind ErrorKind, message string) *Error {
	return &Error{kind: kind, message: message}
}

// Kind returns the error kind.
func (e *Error) Kind() ErrorKind { return e.kind }

// WithContext attaches a key-value context pair and returns the same error.
func (e *Error) WithContext(key string, value any) *Error {
	e.context = append(e.context, contextPair{key, value})
	return e
}

// WithSource wraps an underlying error and returns the same error.
func (e *Error) WithSource(err error) *Error {
	e.source = err
	return e
}

// Error implements the error interface with a single-line representation.
func (e *Error) Error() string {
	var sb strings.Builder
	sb.WriteString(e.kind.String())
	if e.message != "" {
		sb.WriteString(": ")
		sb.WriteString(e.message)
	}
	if len(e.context) > 0 {
		parts := make([]string, 0, len(e.context))
		for _, c := range e.context {
			parts = append(parts, fmt.Sprintf("%s: %v", c.key, c.value))
		}
		sb.WriteString(", context: { ")
		sb.WriteString(strings.Join(parts, ", "))
		sb.WriteString(" }")
	}
	if e.source != nil {
		sb.WriteString(", source: ")
		sb.WriteString(e.source.Error())
	}
	return sb.String()
}

// Unwrap returns the source error so that errors.Is / errors.As work.
func (e *Error) Unwrap() error { return e.source }
