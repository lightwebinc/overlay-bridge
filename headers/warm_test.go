package headers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// countingFallback answers every height with a root derived from it, fails the
// heights in fail, and counts calls.
type countingFallback struct {
	calls atomic.Int64
	fail  map[uint32]bool
	delay time.Duration

	mu    sync.Mutex
	asked []uint32
}

func (c *countingFallback) RootAt(ctx context.Context, h uint32) (*chainhash.Hash, error) {
	c.calls.Add(1)
	c.mu.Lock()
	c.asked = append(c.asked, h)
	c.mu.Unlock()
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.fail[h] {
		return nil, errors.New("no such height")
	}
	r := chainhash.Hash{byte(h), byte(h >> 8), byte(h >> 16), 0xEE}
	return &r, nil
}

// TestWarmMovesTheRoundTripOffTheVerifyPath is the reason warm-up exists: after
// it, a root at or below the anchor is answered without asking the fallback.
func TestWarmMovesTheRoundTripOffTheVerifyPath(t *testing.T) {
	fb := &countingFallback{}
	s := newStore(t, Options{Anchor: Anchor{Hash: chainhash.Hash{0xAA}, Height: 1000}, Fallback: fb})
	if err := s.Warm(t.Context(), 100, 4); err != nil {
		t.Fatal(err)
	}
	st := s.WarmStats()
	if !st.Done || st.Target != 100 || st.Loaded != 100 || st.Failed != 0 {
		t.Fatalf("warm stats %+v, want done, target 100, loaded 100", st)
	}
	before := fb.calls.Load()
	for h := uint32(901); h <= 1000; h++ {
		r, err := s.RootAt(t.Context(), h)
		if err != nil {
			t.Fatalf("height %d: %v", h, err)
		}
		if r[0] != byte(h) || r[1] != byte(h>>8) {
			t.Fatalf("height %d: wrong root %s", h, r)
		}
	}
	if got := fb.calls.Load() - before; got != 0 {
		t.Fatalf("%d fallback calls for warmed heights, want 0", got)
	}
	// A warmed root is the anchor's answer for a height the lane could not
	// have carried: history, never lane, and never fallback.
	if st := s.Stats(); st.FromHistory != 100 || st.FromFallback != 0 || st.FromLane != 0 {
		t.Fatalf("provenance %+v: warmed roots must book as history", st)
	}
	// A height outside the warmed range still goes to the fallback.
	if _, err := s.RootAt(t.Context(), 900); err != nil {
		t.Fatal(err)
	}
	if got := fb.calls.Load() - before; got != 1 {
		t.Fatalf("height below the warmed range: %d fallback calls, want 1", got)
	}
}

// TestWarmLoadsNewestFirst: a warm-up cut short must have loaded the recent end,
// which is where proofs point.
func TestWarmLoadsNewestFirst(t *testing.T) {
	fb := &countingFallback{}
	s := newStore(t, Options{Anchor: Anchor{Hash: chainhash.Hash{0xAA}, Height: 50}, Fallback: fb})
	if err := s.Warm(t.Context(), 10, 1); err != nil {
		t.Fatal(err)
	}
	want := []uint32{50, 49, 48, 47, 46, 45, 44, 43, 42, 41}
	if len(fb.asked) != len(want) {
		t.Fatalf("asked %v, want %v", fb.asked, want)
	}
	for i := range want {
		if fb.asked[i] != want[i] {
			t.Fatalf("asked %v, want %v", fb.asked, want)
		}
	}
}

// TestWarmClampsAtGenesis: a chain shorter than the depth warms from height 0.
func TestWarmClampsAtGenesis(t *testing.T) {
	fb := &countingFallback{}
	s := newStore(t, Options{Anchor: Anchor{Hash: chainhash.Hash{0xAA}, Height: 5}, Fallback: fb})
	if err := s.Warm(t.Context(), 2016, 3); err != nil {
		t.Fatal(err)
	}
	if st := s.WarmStats(); st.Target != 6 || st.Loaded != 6 {
		t.Fatalf("warm stats %+v, want target 6 (heights 0..5)", st)
	}
}

// TestWarmFailuresAreCountedNotFatal: a height the fallback cannot answer is
// left to be fetched on first use.
func TestWarmFailuresAreCountedNotFatal(t *testing.T) {
	fb := &countingFallback{fail: map[uint32]bool{98: true, 99: true}}
	s := newStore(t, Options{Anchor: Anchor{Hash: chainhash.Hash{0xAA}, Height: 100}, Fallback: fb})
	if err := s.Warm(t.Context(), 10, 2); err != nil {
		t.Fatal(err)
	}
	if st := s.WarmStats(); st.Loaded != 8 || st.Failed != 2 || !st.Done {
		t.Fatalf("warm stats %+v, want 8 loaded, 2 failed, done", st)
	}
	// The failed height is asked again on use, and a recovered fallback answers.
	delete(fb.fail, 99)
	if _, err := s.RootAt(t.Context(), 99); err != nil {
		t.Fatalf("height 99 after warm-up: %v", err)
	}
}

// TestWarmStopsAtItsDeadline: a slow fallback bounds the warm-up, never the
// bridge's startup.
func TestWarmStopsAtItsDeadline(t *testing.T) {
	fb := &countingFallback{delay: 20 * time.Millisecond}
	s := newStore(t, Options{Anchor: Anchor{Hash: chainhash.Hash{0xAA}, Height: 100000}, Fallback: fb})
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.Warm(ctx, 2016, 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want the deadline", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("warm-up ran %s past a 100ms deadline", el)
	}
	st := s.WarmStats()
	if !st.Done || st.Loaded == 0 || st.Loaded >= st.Target {
		t.Fatalf("warm stats %+v, want done with a partial load", st)
	}
}

// TestWarmDoesNotShadowTheLane: once the lane carries a height, the lane's root
// answers, not a warmed one.
func TestWarmDoesNotShadowTheLane(t *testing.T) {
	fb := &countingFallback{}
	anchor := chainhash.Hash{0xAA}
	s := newStore(t, Options{Anchor: Anchor{Hash: anchor, Height: 100}, Fallback: fb, MinBits: easyBits})
	if err := s.Warm(t.Context(), 10, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Observe(t.Context(), mineHeader(t, anchor, rootN(7), easyBits)); err != nil {
		t.Fatal(err)
	}
	r, err := s.RootAt(t.Context(), 101)
	if err != nil || *r != rootN(7) {
		t.Fatalf("height 101: %v %v, want the lane's root", r, err)
	}
	if st := s.Stats(); st.FromLane != 1 {
		t.Fatalf("provenance %+v, want the lane", st)
	}
}

func TestWarmWithoutFallback(t *testing.T) {
	s := newStore(t, Options{})
	if err := s.Warm(t.Context(), 10, 1); err == nil {
		t.Fatal("warm-up with no fallback must say so")
	}
	if !s.WarmStats().Done {
		t.Fatal("a warm-up that cannot run is still done; readiness must not wait on it")
	}
}
