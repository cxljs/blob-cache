package common

// Hint suggests the priority of an entry to the eviction algorithm. The
// meaning differs per algorithm and some algorithms ignore it. Mirrors
// foyer's `Hint`.
type Hint int

const (
	// HintNormal is the default hint shared by all eviction algorithms.
	HintNormal Hint = iota
	// HintLow suggests the entry priority is low. Used by LRU.
	HintLow
)

func (h Hint) String() string {
	switch h {
	case HintNormal:
		return "Normal"
	case HintLow:
		return "Low"
	default:
		return "Unknown"
	}
}

// Location advises where a cache entry should be stored. Only takes effect
// the first time an entry is handled; afterwards the entry may not follow
// the advice. Mirrors foyer's `Location`.
type Location int

const (
	// LocationDefault prefers in-memory cache for an in-memory cache, and
	// the default behavior for a hybrid cache.
	LocationDefault Location = iota
	// LocationInMem prefers to store the entry in the in-memory cache only.
	LocationInMem
	// LocationOnDisk prefers to store the entry in the disk cache only.
	LocationOnDisk
)

func (l Location) String() string {
	switch l {
	case LocationDefault:
		return "Default"
	case LocationInMem:
		return "InMem"
	case LocationOnDisk:
		return "OnDisk"
	default:
		return "Unknown"
	}
}

// Age is the entry age in the disk cache, used by the hybrid cache. Mirrors
// foyer's `Age`.
type Age int

const (
	// AgeFresh means the entry is fresh, not yet or just inserted into the
	// disk cache.
	AgeFresh Age = iota
	// AgeYoung means the entry is young and will be kept in the disk cache
	// for a while.
	AgeYoung
	// AgeOld means the entry is old and will be evicted from the disk cache
	// soon.
	AgeOld
)

func (a Age) String() string {
	switch a {
	case AgeFresh:
		return "Fresh"
	case AgeYoung:
		return "Young"
	case AgeOld:
		return "Old"
	default:
		return "Unknown"
	}
}

// Source tells where a cache entry value comes from. Mirrors foyer's
// `Source`.
type Source int

const (
	// SourceOuter means the entry comes from outer code (e.g. a fetch).
	SourceOuter Source = iota
	// SourceMemory means the entry comes from the in-memory cache.
	SourceMemory
	// SourceDisk means the entry comes from the disk cache.
	SourceDisk
)

func (s Source) String() string {
	switch s {
	case SourceOuter:
		return "Outer"
	case SourceMemory:
		return "Memory"
	case SourceDisk:
		return "Disk"
	default:
		return "Unknown"
	}
}

// Properties carries per-entry metadata shared by the in-memory cache and
// the disk cache. It is the concrete counterpart of foyer's `Properties`
// trait / `GenericProperties`.
type Properties struct {
	// Phantom marks an entry that is NOT actually inserted into the cache.
	// It is used internally to keep APIs consistent and must not be set by
	// external callers.
	Phantom bool
	// Hint is the eviction priority hint.
	Hint Hint
	// Location advises where to store the entry (hybrid cache).
	Location Location
	// Age is the entry age in the disk cache (hybrid cache).
	Age Age
}

// WithHint returns a copy of the properties with the given hint.
func (p Properties) WithHint(hint Hint) Properties {
	p.Hint = hint
	return p
}

// WithLocation returns a copy of the properties with the given location.
func (p Properties) WithLocation(location Location) Properties {
	p.Location = location
	return p
}

// WithAge returns a copy of the properties with the given age.
func (p Properties) WithAge(age Age) Properties {
	p.Age = age
	return p
}
