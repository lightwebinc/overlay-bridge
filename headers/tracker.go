package headers

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// Tracker adapts a RootSource to the SDK's ChainTracker, with a cache in front
// and a record of where each answer came from.
//
// The provenance record is not bookkeeping. The claim this bridge makes is
// that SPV inside the engine is fed by the header lane; a run in which every
// root was served by the fallback has not demonstrated that claim, and without
// counting the two apart nobody can tell the difference after the fact.
type Tracker struct {
	src RootSource

	mu     sync.RWMutex
	cache  map[uint32]chainhash.Hash
	height uint32

	fromLane, fromFallback, misses uint64
}

var _ chaintracker.ChainTracker = (*Tracker)(nil)

// NewTracker returns a tracker over src.
func NewTracker(src RootSource) *Tracker {
	return &Tracker{src: src, cache: map[uint32]chainhash.Hash{}}
}

// Learn records a root the lane delivered and advances the reported height.
//
// The lane reader MUST call this for every accepted observation. It is the
// only writer of the tracker's height, and an engine asking how far the chain
// has got is told zero until it is called: a host that answers roots but
// reports height zero looks to an engine like a chain that has not started.
func (t *Tracker) Learn(height uint32, root chainhash.Hash) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cache[height] = root
	if height > t.height {
		t.height = height
	}
}

// IsValidRootForHeight reports whether root is the merkle root committed at
// height.
func (t *Tracker) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	if root == nil {
		return false, nil
	}
	t.mu.RLock()
	cached, ok := t.cache[height]
	t.mu.RUnlock()
	if ok {
		t.count(&t.fromLane)
		return cached.IsEqual(root), nil
	}
	got, err := t.src.RootAt(ctx, height)
	if err != nil {
		t.count(&t.misses)
		return false, err
	}
	if got == nil {
		t.count(&t.misses)
		return false, nil
	}
	// Learn it, but do NOT advance the reported height from a fallback answer:
	// the height is a claim about how far this host's own view of the chain
	// has got, and a backfilled root from elsewhere is not evidence of that.
	t.mu.Lock()
	t.cache[height] = *got
	t.mu.Unlock()

	if lane, isStore := t.src.(interface{ Known(uint32) bool }); isStore && lane.Known(height) {
		t.count(&t.fromLane)
	} else {
		t.count(&t.fromFallback)
	}
	return got.IsEqual(root), nil
}

// CurrentHeight reports the best height this host has reached.
//
// This is live, not decorative: the round-one host reads it on its unproven
// eviction paths and serves it to its own callers.
func (t *Tracker) CurrentHeight(_ context.Context) (uint32, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.height, nil
}

// TrackerStats reports where answers came from.
type TrackerStats struct{ FromLane, FromFallback, Misses uint64 }

// Stats returns a snapshot.
func (t *Tracker) Stats() TrackerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TrackerStats{FromLane: t.fromLane, FromFallback: t.fromFallback, Misses: t.misses}
}

func (t *Tracker) count(p *uint64) {
	t.mu.Lock()
	*p++
	t.mu.Unlock()
}
