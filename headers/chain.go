// Package headers follows a BRC-135 header lane and serves it to an overlay
// engine as that engine's chain tracker.
//
// The lane carries bare 80-byte headers with no height on the wire, so height
// is derived by chaining from a known anchor. The result is the host's own
// view of the chain: every object the engine admits is verified against
// headers this host received itself, from the same network that delivered the
// object, with no third-party header service in the loop and no rate limit to
// be subject to. That is the whole point, and it is why this package enforces
// rather than reports.
//
// # This is the SPV root of trust
//
// An engine verifies every BUMP against what this package answers. A header
// accepted here that should not have been is a merkle root the engine will
// trust. So:
//
//   - Proof of work is ENFORCED, not labelled. A header that does not meet its
//     target is rejected and never chains.
//   - Proof of work is checked against a configured floor, not only against
//     the header's own declared target. A header carries the target it claims
//     to meet, so checking it against itself is a check an attacker writes the
//     answer to. MinBits is the floor the operator sets from the network they
//     are on.
//   - A height may be claimed by more than one header. Competing headers are
//     all retained and the canonical chain is tracked explicitly, so a fork
//     does not silently overwrite the root a previous header committed to.
package headers

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Size is a serialised block header.
const Size = 80

// ErrHeaderSize reports a header that is not exactly 80 bytes.
var ErrHeaderSize = errors.New("headers: not 80 bytes")

// ErrProofOfWork reports a header that does not meet its target, or that
// declares a target easier than the configured floor.
var ErrProofOfWork = errors.New("headers: insufficient proof of work")

// Anchor is the known point the chain derives heights from.
type Anchor struct {
	Hash   chainhash.Hash
	Height uint32
}

// RootSource answers which merkle root a height commits to.
type RootSource interface {
	RootAt(ctx context.Context, height uint32) (*chainhash.Hash, error)
}

// HeaderLookup resolves a header the lane did not carry, so the chain can
// re-anchor across a gap instead of orphaning every header that follows. The
// lane is a live tail: a consumer that connects late, or loses its connection
// across a restart, sees headers whose parents it never received.
type HeaderLookup interface {
	HeaderAt(ctx context.Context, hash chainhash.Hash) (height uint32, root chainhash.Hash, err error)
}

// Options configures a Store.
type Options struct {
	// Anchor is the known starting point. Required.
	Anchor Anchor
	// Fallback answers roots below the anchor and across gaps.
	Fallback RootSource
	// Lookup re-anchors the chain after a gap in the tail.
	Lookup HeaderLookup
	// LookupTimeout bounds each Lookup call. Default 5s. It is never
	// unbounded: the lookup happens on the lane's hot path, and an
	// unbounded call there stalls the lane behind a slow third party.
	LookupTimeout time.Duration
	// MinBits is the compact-target floor. A header declaring an easier
	// target than this is rejected before its hash is even compared. Zero
	// disables the floor, which is only correct where the operator knows the
	// lane is trusted end to end.
	MinBits uint32
	// Window is the retained height window behind the tip. Zero is unbounded,
	// which is only appropriate for a short-lived process.
	Window uint32
	Log    *slog.Logger
}

// Observation is what one delivered header turned out to be.
type Observation struct {
	Hash     chainhash.Hash
	Height   uint32
	Root     chainhash.Hash
	Chained  bool // its parent was known
	Reanchor bool // its parent was resolved through Lookup (a gap in the tail)
	Tip      bool // it became the best tip
	Replaced bool // another header already claimed this height
}

type entry struct {
	root chainhash.Hash
	prev chainhash.Hash
}

// Store is the header chain. It is safe for concurrent use.
type Store struct {
	mu sync.Mutex
	// byHash is every header seen or anchored, to its height.
	byHash map[chainhash.Hash]uint32
	// at is every header known at a height. A height may hold more than one:
	// competing headers are retained rather than overwritten.
	at map[uint32]map[chainhash.Hash]entry
	// canon is the canonical chain: height to the hash on the best chain.
	canon  map[uint32]chainhash.Hash
	tip    chainhash.Hash
	tipH   uint32
	lowest uint32 // lowest retained height, for windowed pruning

	opt Options

	observed, rejected, orphaned, reanchored, replaced, pruned uint64
}

var _ RootSource = (*Store)(nil)

// New returns a Store anchored at o.Anchor.
func New(o Options) (*Store, error) {
	if o.Anchor.Hash == (chainhash.Hash{}) {
		return nil, fmt.Errorf("headers: an anchor hash is required")
	}
	if o.LookupTimeout <= 0 {
		o.LookupTimeout = 5 * time.Second
	}
	return &Store{
		byHash: map[chainhash.Hash]uint32{o.Anchor.Hash: o.Anchor.Height},
		at:     map[uint32]map[chainhash.Hash]entry{},
		canon:  map[uint32]chainhash.Hash{},
		tip:    o.Anchor.Hash,
		tipH:   o.Anchor.Height,
		lowest: o.Anchor.Height,
		opt:    o,
	}, nil
}

// Handle consumes one header off the lane. It satisfies lanes.Handler for
// objfmt.ClassBlockHeader.
func (s *Store) Handle(ctx context.Context, hdr []byte) error {
	_, err := s.Observe(ctx, hdr)
	return err
}

// Observe records one header.
//
// A header that fails proof of work is rejected and never chains. A header
// whose parent is unknown is an orphan: it is reported, counted, and not
// chained, because a tail cannot assign it a height.
func (s *Store) Observe(ctx context.Context, hdr []byte) (Observation, error) {
	if len(hdr) != Size {
		return Observation{}, ErrHeaderSize
	}
	hash := headerHash(hdr)
	var prev, root chainhash.Hash
	copy(prev[:], hdr[4:36])
	copy(root[:], hdr[36:68])
	bits := binary.LittleEndian.Uint32(hdr[72:76])

	// Enforce, before anything is recorded. Both halves matter: the floor
	// rejects a header that declares an easy target, and the work check
	// rejects one that declares a hard target it does not meet.
	if err := s.checkWork(hash, bits); err != nil {
		s.bump(&s.rejected)
		if s.opt.Log != nil {
			s.opt.Log.Warn("headers: rejected", "err", err, "hash", hash.String(), "bits", bits)
		}
		return Observation{Hash: hash, Root: root}, err
	}

	obs := Observation{Hash: hash, Root: root}

	s.mu.Lock()
	if h, ok := s.byHash[hash]; ok {
		s.mu.Unlock()
		obs.Height, obs.Chained = h, true
		s.bump(&s.observed)
		return obs, nil // a re-delivery; nothing changes
	}
	parentH, known := s.byHash[prev]
	s.mu.Unlock()

	if !known && s.opt.Lookup != nil {
		// The parent never came down the lane. Resolve it once, under a bound,
		// with the lock released: this is the lane's hot path and a slow third
		// party must not hold it.
		lctx, cancel := context.WithTimeout(ctx, s.opt.LookupTimeout)
		h, r, err := s.opt.Lookup.HeaderAt(lctx, prev)
		cancel()
		if err == nil {
			s.mu.Lock()
			// Re-check under the lock. Two concurrent observations can both
			// reach here, and without this the second overwrites the first's
			// anchor with an identical-but-separately-fetched result, or with
			// a stale one if the source changed between the calls.
			if existing, ok := s.byHash[prev]; ok {
				parentH, known = existing, true
			} else {
				s.byHash[prev] = h
				s.putLocked(h, prev, entry{root: r})
				parentH, known, obs.Reanchor = h, true, true
			}
			s.mu.Unlock()
			if obs.Reanchor {
				s.bump(&s.reanchored)
			}
		} else if s.opt.Log != nil {
			s.opt.Log.Warn("headers: re-anchor lookup failed", "err", err, "parent", prev.String())
		}
	}

	if !known {
		s.bump(&s.orphaned)
		s.bump(&s.observed)
		return obs, nil // unchained: this tail cannot give it a height
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	obs.Height, obs.Chained = parentH+1, true
	if _, taken := s.canon[obs.Height]; taken {
		obs.Replaced = true
		s.replaced++
	}
	s.byHash[hash] = obs.Height
	s.putLocked(obs.Height, hash, entry{root: root, prev: prev})

	// Extend or reorganise. A strictly greater height wins, and so does a
	// direct extension of the current tip; anything else is retained but does
	// not move the canonical chain.
	if prev == s.tip || obs.Height > s.tipH {
		s.tip, s.tipH = hash, obs.Height
		obs.Tip = true
		s.recanonLocked(hash, obs.Height)
		s.pruneLocked()
	}
	s.observed++
	return obs, nil
}

// putLocked records a header at a height without displacing a competitor.
func (s *Store) putLocked(h uint32, hash chainhash.Hash, e entry) {
	m := s.at[h]
	if m == nil {
		m = map[chainhash.Hash]entry{}
		s.at[h] = m
	}
	m[hash] = e
	if h < s.lowest {
		s.lowest = h
	}
}

// recanonLocked walks back from a new tip, rewriting the canonical chain until
// it meets a height that already agrees. On a plain extension that is one
// iteration; on a reorganisation it is the depth of the fork.
func (s *Store) recanonLocked(hash chainhash.Hash, height uint32) {
	for {
		if cur, ok := s.canon[height]; ok && cur == hash {
			return
		}
		s.canon[height] = hash
		m, ok := s.at[height]
		if !ok {
			return
		}
		e, ok := m[hash]
		if !ok || e.prev == (chainhash.Hash{}) || height == 0 {
			return
		}
		ph, ok := s.byHash[e.prev]
		if !ok || ph != height-1 {
			return
		}
		hash, height = e.prev, ph
	}
}

// pruneLocked drops heights that have fallen out of the window. It deletes by
// height rather than sweeping the maps: a full-map sweep on every observation
// is what made this structure's predecessor burn most of a core.
func (s *Store) pruneLocked() {
	if s.opt.Window == 0 || s.tipH < s.opt.Window {
		return
	}
	cutoff := s.tipH - s.opt.Window
	for h := s.lowest; h < cutoff; h++ {
		for hash := range s.at[h] {
			delete(s.byHash, hash)
		}
		delete(s.at, h)
		delete(s.canon, h)
		s.pruned++
	}
	if cutoff > s.lowest {
		s.lowest = cutoff
	}
}

// Tip returns the best header seen so far.
func (s *Store) Tip() (chainhash.Hash, uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tip, s.tipH
}

// RootAt serves the canonical chain's root for a height, falling back below
// the anchor and across gaps.
func (s *Store) RootAt(ctx context.Context, height uint32) (*chainhash.Hash, error) {
	s.mu.Lock()
	hash, ok := s.canon[height]
	var e entry
	if ok {
		e, ok = s.at[height][hash]
	}
	s.mu.Unlock()
	if ok {
		r := e.root
		return &r, nil
	}
	if s.opt.Fallback == nil {
		return nil, fmt.Errorf("headers: height %d not on the lane and no fallback configured", height)
	}
	return s.opt.Fallback.RootAt(ctx, height)
}

// Known reports whether the height's root came off the lane rather than the
// fallback. It is the discriminator for the claim that verification is fed by
// the lane: a run in which every root was served by the fallback has not
// demonstrated it.
func (s *Store) Known(height uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.canon[height]
	return ok
}

// checkWork enforces the floor and then the header's own target.
func (s *Store) checkWork(hash chainhash.Hash, bits uint32) error {
	target, ok := compactToBig(bits)
	if !ok {
		return fmt.Errorf("%w: malformed target 0x%08x", ErrProofOfWork, bits)
	}
	if s.opt.MinBits != 0 {
		floor, ok := compactToBig(s.opt.MinBits)
		if !ok {
			return fmt.Errorf("%w: malformed floor 0x%08x", ErrProofOfWork, s.opt.MinBits)
		}
		// A LARGER target is easier work. A header may claim a harder target
		// than the floor, never an easier one.
		if target.Cmp(floor) > 0 {
			return fmt.Errorf("%w: declared target 0x%08x is easier than the floor 0x%08x", ErrProofOfWork, bits, s.opt.MinBits)
		}
	}
	if workValue(hash).Cmp(target) > 0 {
		return fmt.Errorf("%w: hash does not meet target 0x%08x", ErrProofOfWork, bits)
	}
	return nil
}

func (s *Store) bump(p *uint64) {
	s.mu.Lock()
	*p++
	s.mu.Unlock()
}

func headerHash(hdr []byte) chainhash.Hash {
	first := sha256.Sum256(hdr)
	second := sha256.Sum256(first[:])
	var h chainhash.Hash
	copy(h[:], second[:])
	return h
}

// compactToBig expands a compact target. ok is false for a zero mantissa,
// which no valid header carries.
func compactToBig(bits uint32) (*big.Int, bool) {
	exp := bits >> 24
	mant := bits & 0x007fffff
	if mant == 0 {
		return nil, false
	}
	t := new(big.Int).SetUint64(uint64(mant))
	if exp <= 3 {
		t.Rsh(t, 8*uint(3-exp))
	} else {
		t.Lsh(t, 8*uint(exp-3))
	}
	return t, true
}

// workValue reads the hash as the big-endian integer proof of work compares.
// The hash is little-endian on the wire.
func workValue(hash chainhash.Hash) *big.Int {
	var be [32]byte
	for i := range hash {
		be[i] = hash[31-i]
	}
	return new(big.Int).SetBytes(be[:])
}
