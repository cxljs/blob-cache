package common

// Event identifies why a cache entry left the in-memory cache, mirroring
// foyer's `Event`.
type Event int

const (
	// EventEvict means the cache evicted the entry to admit another one.
	EventEvict Event = iota
	// EventReplace means the entry was replaced by a newer value with the
	// same key.
	EventReplace
	// EventRemove means the entry was removed explicitly.
	EventRemove
	// EventClear means the whole cache was cleared.
	EventClear
)

func (e Event) String() string {
	switch e {
	case EventEvict:
		return "Evict"
	case EventReplace:
		return "Replace"
	case EventRemove:
		return "Remove"
	case EventClear:
		return "Clear"
	default:
		return "Unknown"
	}
}

// EventListener is notified when a cache entry leaves the in-memory cache.
// It is the counterpart of foyer's `EventListener` trait.
type EventListener[K comparable, V any] interface {
	OnLeave(reason Event, key K, value V)
}

// EventListenerFunc adapts a plain function to the EventListener interface.
type EventListenerFunc[K comparable, V any] func(reason Event, key K, value V)

// OnLeave implements EventListener.
func (f EventListenerFunc[K, V]) OnLeave(reason Event, key K, value V) {
	f(reason, key, value)
}
