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
	tr.Learn(812345, rootN(7))
	if h, _ := tr.CurrentHeight(context.Background()); h != 812345 {
		t.Fatalf("CurrentHeight = %d after Learn, want 812345", h)
	}
	// Height never goes backwards on an older root arriving.
	tr.Learn(1000, rootN(8))
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

func TestTrackerCachesAndCountsProvenance(t *testing.T) {
	want := rootN(3)
	src := &stubSource{roots: map[uint32]chainhash.Hash{101: want}}
	tr := NewTracker(src)

	for i := 0; i < 3; i++ {
		if ok, err := tr.IsValidRootForHeight(context.Background(), &want, 101); err != nil || !ok {
			t.Fatalf("lookup %d: %v/%v", i, ok, err)
		}
	}
	if src.calls != 1 {
		t.Fatalf("source called %d times, want 1: the cache is not in front", src.calls)
	}
	// The source is a bare stub, not a Store, so it cannot claim lane
	// provenance: the first answer books as fallback, the rest as cache.
	st := tr.Stats()
	if st.FromFallback != 1 || st.FromLane != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestFallbackAnswerDoesNotAdvanceHeight pins that a backfilled root is not
// evidence of how far THIS host's view of the chain has got. Advancing the
// height from a fallback would let a host with a dead lane report a healthy
// tip.
func TestFallbackAnswerDoesNotAdvanceHeight(t *testing.T) {
	want := rootN(3)
	tr := NewTracker(&stubSource{roots: map[uint32]chainhash.Hash{900000: want}})
	if _, err := tr.IsValidRootForHeight(context.Background(), &want, 900000); err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if h, _ := tr.CurrentHeight(context.Background()); h != 0 {
		t.Fatalf("height advanced to %d from a fallback answer", h)
	}
}

func TestTrackerPropagatesSourceError(t *testing.T) {
	tr := NewTracker(&stubSource{err: errors.New("no such height")})
	r := rootN(1)
	if _, err := tr.IsValidRootForHeight(context.Background(), &r, 5); err == nil {
		t.Fatal("source error was swallowed")
	}
	if tr.Stats().Misses != 1 {
		t.Fatalf("stats = %+v", tr.Stats())
	}
}

// TestTrackerOverStoreBooksLaneProvenance wires the real pair, which is what
// makes the "SPV is fed by the lane" claim checkable rather than assumed.
func TestTrackerOverStoreBooksLaneProvenance(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()
	hdr := mineHeader(t, anchor, rootN(1), easyBits)
	obs, err := s.Observe(context.Background(), hdr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	tr := NewTracker(s)

	ok, err := tr.IsValidRootForHeight(context.Background(), &obs.Root, obs.Height)
	if err != nil || !ok {
		t.Fatalf("root from the lane: %v/%v", ok, err)
	}
	if st := tr.Stats(); st.FromLane != 1 || st.FromFallback != 0 {
		t.Fatalf("stats = %+v, want the answer booked to the lane", st)
	}
}
