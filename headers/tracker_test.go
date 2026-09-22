package headers

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

type stubSource struct {
	roots map[uint32]chainhash.Hash
	err   error
	calls int
}

func (s *stubSource) RootAt(_ context.Context, h uint32) (*chainhash.Hash, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	r, ok := s.roots[h]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

// TestCurrentHeightReadsZeroUntilLearned pins the trap the promotion note
// calls out: the tracker's height has exactly one writer, and a lane reader
// that forgets to call Learn leaves the engine told the chain has not started.
func TestCurrentHeightReadsZeroUntilLearned(t *testing.T) {
	tr := NewTracker(&stubSource{})
	if h, err := tr.CurrentHeight(context.Background()); err != nil || h != 0 {
		t.Fatalf("CurrentHeight = %d/%v, want 0", h, err)
	}
	tr.Learn(812345)
	if h, _ := tr.CurrentHeight(context.Background()); h != 812345 {
		t.Fatalf("CurrentHeight = %d after Learn, want 812345", h)
	}
	// Height never goes backwards.
	tr.Learn(1000)
	if h, _ := tr.CurrentHeight(context.Background()); h != 812345 {
		t.Fatalf("CurrentHeight went backwards to %d", h)
	}
}

func TestIsValidRootForHeight(t *testing.T) {
	want := rootN(3)
	tr := NewTracker(&stubSource{roots: map[uint32]chainhash.Hash{101: want}})

	ok, err := tr.IsValidRootForHeight(context.Background(), &want, 101)
	if err != nil || !ok {
		t.Fatalf("matching root: %v/%v", ok, err)
	}
	other := rootN(4)
	ok, err = tr.IsValidRootForHeight(context.Background(), &other, 101)
	if err != nil || ok {
		t.Fatalf("mismatched root: %v/%v, want false/nil", ok, err)
	}
	if nilOK, err := tr.IsValidRootForHeight(context.Background(), nil, 101); err != nil || nilOK {
		t.Fatalf("nil root: %v/%v", nilOK, err)
	}
}

func TestTrackerPropagatesSourceError(t *testing.T) {
	tr := NewTracker(&stubSource{err: errors.New("no such height")})
	r := rootN(1)
	if _, err := tr.IsValidRootForHeight(context.Background(), &r, 5); err == nil {
		t.Fatal("source error was swallowed")
	}
}

// TestTrackerFollowsTheCanonicalChain is the regression for a poisoned cache.
// The tracker used to cache every root the lane delivered, so a competing
// header at a height already held overwrote the cached root although the
// store's canonical chain had not changed, and the tracker then answered true
// for a root the chain did not commit to. It holds no cache now, and this
// pins that its answer tracks the store through a fork and a reorganisation.
func TestTrackerFollowsTheCanonicalChain(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	tr := NewTracker(s)
	anchor, _ := s.Tip()

	a1 := mineHeader(t, anchor, rootN(1), easyBits)
	obsA1, _ := s.Observe(context.Background(), a1)
	if ok, _ := tr.IsValidRootForHeight(context.Background(), &obsA1.Root, 101); !ok {
		t.Fatal("canonical root at 101 not valid")
	}

	// A competing header at 101 that does NOT become the tip.
	b1 := mineHeader(t, anchor, rootN(2), easyBits)
	obsB1, _ := s.Observe(context.Background(), b1)
	if obsB1.Tip {
		t.Fatal("test setup: b1 unexpectedly became the tip")
	}
	// The tracker must still answer for a1's root, not b1's.
	if ok, _ := tr.IsValidRootForHeight(context.Background(), &obsA1.Root, 101); !ok {
		t.Fatal("a competing non-tip header displaced the canonical root")
	}
	if ok, _ := tr.IsValidRootForHeight(context.Background(), &obsB1.Root, 101); ok {
		t.Fatal("a competing non-tip header's root was answered as valid")
	}

	// Now extend b's branch so it reorganises the chain: the tracker's answer
	// must follow, with no stale cache to hold the old root.
	b2 := mineHeader(t, obsB1.Hash, rootN(3), easyBits)
	if obs, _ := s.Observe(context.Background(), b2); !obs.Tip {
		t.Fatal("test setup: b2 did not become the tip")
	}
	if ok, _ := tr.IsValidRootForHeight(context.Background(), &obsB1.Root, 101); !ok {
		t.Fatal("after the reorg, b1's root should now be canonical at 101")
	}
	if ok, _ := tr.IsValidRootForHeight(context.Background(), &obsA1.Root, 101); ok {
		t.Fatal("after the reorg, a1's root was still answered as valid")
	}
}

// TestStoreBooksProvenanceOnTheServedPath pins that provenance is counted
// where the engine's reads actually go. An earlier version counted it on the
// in-process tracker, which the engine never calls, so the series that exists
// to prove "verification is fed by the lane" read zero for ever.
func TestStoreBooksProvenanceOnTheServedPath(t *testing.T) {
	fb := &stubSource{roots: map[uint32]chainhash.Hash{50: rootN(9)}}
	s := newStore(t, Options{MinBits: easyBits, Fallback: fb})
	anchor, _ := s.Tip()
	obs, _ := s.Observe(context.Background(), mineHeader(t, anchor, rootN(1), easyBits))

	if _, err := s.RootAt(context.Background(), obs.Height); err != nil {
		t.Fatalf("lane root: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.RootAt(context.Background(), 50); err != nil {
			t.Fatalf("fallback root: %v", err)
		}
	}
	if _, err := s.RootAt(context.Background(), 999999); err == nil {
		// no fallback entry, so the stub returns nil,nil: a miss
	}
	st := s.Stats()
	if st.FromLane != 1 || st.FromFallback != 3 {
		t.Fatalf("provenance = lane %d / fallback %d, want 1 / 3", st.FromLane, st.FromFallback)
	}
	// ...and the fallback was asked ONCE for the three answers: cached.
	if fb.calls != 2 { // one for height 50, one for the miss at 999999
		t.Fatalf("fallback called %d times, want 2 (height 50 cached after the first)", fb.calls)
	}
}
