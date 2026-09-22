// Package guard keeps one publication from going round twice.
//
// The bridge has two sides that can put the same object on the plane: the feed,
// which receives an object the plane delivered, and the facade, which receives
// a client's submission. An application that reads from the plane and submits
// what it read back to its own host would publish a loop. The guard is the
// bounded seen-set that stops it.
//
// # Both sides mark, and both mark BEFORE they act
//
// A client publish is never delivered by the feed, so a feed-only guard cannot
// recognise a re-publish of a client's own object; and the object the feed
// delivered is exactly the one a naive application is most likely to hand
// straight back. So the feed marks [registry.Delivered] before the engine
// submit, and the facade marks [registry.Submitted] on inbound before
// forwarding. Marking before acting is the whole trick and the only ordering
// that works: marking afterwards leaves a window in which the echo arrives
// before the mark lands.
//
// # Identity is the delivered bytes, and that is safe only because nothing propagates
//
// Ruling D4 keys on the object's ContentID as delivered: SHA-256d of the
// payload bytes, the plane's own object identity. The guard therefore parses
// nothing. It holds no BEEF reader, no go-sdk transaction import, and no
// subject-txid extraction, which deletes a whole class of failure the earlier
// design had to carry.
//
// That is sound under ruling D3, where the engine's own propagation is switched
// off and the plane is the propagation. It would NOT be sufficient if an HTTPS
// propagation posture were ever re-enabled: an engine re-serialises what it
// propagates, so an echo of our own object would arrive with different bytes,
// therefore a different ContentID, and this guard would not catch it. Re-enabling
// propagation is a new decision that has to re-open this one.
package guard

import (
	"time"

	"github.com/lightwebinc/teranode-bridge/registry"
)

// Guard is a bounded, self-rotating seen-set over object identities. It is safe
// for concurrent use.
type Guard struct{ r *registry.Registry }

// New returns a guard holding at most maxEntries identities for roughly ttl
// each. An entry survives between ttl/2 and ttl: the underlying registry rotates
// generations rather than tracking per-entry expiry, which is what keeps the
// set bounded without a sweeper goroutine.
//
// Non-positive arguments take the registry's own defaults (30 minutes, 2^20
// entries) rather than disabling the guard, because a guard that silently
// admits everything is worse than one sized conservatively.
func New(ttl time.Duration, maxEntries int) *Guard {
	return &Guard{r: registry.New(ttl, maxEntries)}
}

// Mark records the object in direction dir and reports whether it was already
// known, and in which direction. A hit refreshes the entry's age.
//
// Callers act on known: a facade inbound that is already Delivered is a loop
// and must not be published; one that is already Submitted is a duplicate
// publish, not a second one.
func (g *Guard) Mark(contentID, topicID [32]byte, dir registry.Direction) (prev registry.Direction, known bool) {
	return g.r.Mark(Key(contentID, topicID), dir)
}

// Lookup reports the recorded direction without marking.
func (g *Guard) Lookup(contentID, topicID [32]byte) (registry.Direction, bool) {
	return g.r.Lookup(Key(contentID, topicID))
}

// Stats returns a snapshot for the collector.
func (g *Guard) Stats() registry.Stats { return g.r.Stats() }
