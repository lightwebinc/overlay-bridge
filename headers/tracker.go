package headers

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// Tracker adapts a RootSource to the SDK's ChainTracker for an in-process Go
// consumer.
//
// It holds NO cache of roots. An earlier version cached every root the lane
// delivered, and that cache could diverge from the store's canonical chain in
// two ways that a chain tracker must never allow: a competing header at a
// height already held overwrote the cached root although the store's
// canonical chain had not changed, and a reorganisation rewrote the store's
// canonical chain while the cache kept the old roots for ever. Either way the
// tracker would answer true for a root the chain no longer commits to. The
// store already answers a root in one map lookup and already caches fallback
// answers, so there is nothing for a second cache to buy that is worth that.
//
// Provenance of answers is booked by the store, not here, because the path the
// engine's reads actually take is the read API over the store, which never
// touches this adapter.
type Tracker struct {
	src RootSource

	mu     sync.RWMutex
	height uint32
}

var _ chaintracker.ChainTracker = (*Tracker)(nil)

// NewTracker returns a tracker over src.
func NewTracker(src RootSource) *Tracker {
	return &Tracker{src: src}
}

// Learn advances the reported height. Call it when a header becomes the
// TIP, not for every header that chains: a competing header at a height
// already held is chained but is not the tip, and must not be reported as
// progress.
//
// The lane reader MUST call this. It is the only writer of the tracker's
// height, and an engine asking how far the chain has got is told zero until
// it is called: a host that answers roots but reports height zero looks to an
// engine like a chain that has not started.
func (t *Tracker) Learn(height uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if height > t.height {
		t.height = height
	}
}

// IsValidRootForHeight reports whether root is the merkle root committed at
// height, asking the source every time so the answer always follows the
// canonical chain.
func (t *Tracker) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	if root == nil {
		return false, nil
	}
	got, err := t.src.RootAt(ctx, height)
	if err != nil {
		return false, err
	}
	if got == nil {
		return false, nil
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
