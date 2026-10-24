package headers

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// The provenance boundary. Every root the anchor supplies is booked as one of
// two things, decided by height alone:
//
//   - history: at or below the store's startup anchor (or pruned out of its
//     window). The lane is a live tail from the anchor, so it could never have
//     carried these heights to this process, and nearly every object carries
//     an ancestor mined before the process started.
//   - fallback: above it. The lane should have carried the height and did not
//     (a gap, a re-anchored parent, a lane that stopped). This is the posture
//     change OverlayBridgeHeadersFromFallback exists to report.
//
// A header received on the lane is booked as lane wherever it sits.

const testAnchor = 100

func provenanceStore(t *testing.T, o Options) *Store {
	t.Helper()
	o.Anchor = Anchor{Hash: chainhash.Hash{0xAA}, Height: testAnchor}
	if o.MinBits == 0 {
		o.MinBits = easyBits
	}
	return newStore(t, o)
}

func wantProvenance(t *testing.T, s *Store, lane, history, fallback, miss uint64) {
	t.Helper()
	st := s.Stats()
	if st.FromLane != lane || st.FromHistory != history || st.FromFallback != fallback || st.Misses != miss {
		t.Fatalf("provenance lane/history/fallback/miss = %d/%d/%d/%d, want %d/%d/%d/%d",
			st.FromLane, st.FromHistory, st.FromFallback, st.Misses, lane, history, fallback, miss)
	}
}

func mustRoot(t *testing.T, s *Store, h uint32) chainhash.Hash {
	t.Helper()
	r, err := s.RootAt(t.Context(), h)
	if err != nil || r == nil {
		t.Fatalf("RootAt(%d) = %v, %v", h, r, err)
	}
	return *r
}

// A warmed root at or below the anchor, including the anchor's own height, is
// history: read from the cache, booked once per read, never as a lane gap.
func TestProvenanceWarmedBelowAnchorIsHistory(t *testing.T) {
	fb := &countingFallback{}
	s := provenanceStore(t, Options{Fallback: fb})
	if err := s.Warm(t.Context(), 10, 2); err != nil {
		t.Fatal(err)
	}
	calls := fb.calls.Load()
	for _, h := range []uint32{testAnchor, testAnchor - 5, testAnchor - 9} {
		mustRoot(t, s, h)
	}
	if fb.calls.Load() != calls {
		t.Fatal("a warmed height reached the anchor")
	}
	wantProvenance(t, s, 0, 3, 0, 0)
}

// A height below the anchor that the warm-up did not cover is fetched once,
// cached, and booked as history on the fetch and on every cached read.
func TestProvenanceFetchedBelowAnchorIsHistory(t *testing.T) {
	fb := &countingFallback{}
	s := provenanceStore(t, Options{Fallback: fb})
	mustRoot(t, s, 50)
	mustRoot(t, s, 50)
	if got := fb.calls.Load(); got != 1 {
		t.Fatalf("%d anchor reads for one height, want 1 (cached)", got)
	}
	wantProvenance(t, s, 0, 2, 0, 0)
}

// Above the anchor, a height the lane did not deliver is a gap: fallback on the
// fetch and still fallback when the cache answers it again. The cache must not
// launder a gap into history.
func TestProvenanceAboveAnchorGapIsFallback(t *testing.T) {
	fb := &countingFallback{}
	s := provenanceStore(t, Options{Fallback: fb})
	anchor, _ := s.Tip()
	obs, err := s.Observe(t.Context(), mineHeader(t, anchor, rootN(1), easyBits))
	if err != nil || obs.Height != testAnchor+1 {
		t.Fatalf("observe: %+v %v", obs, err)
	}
	// The lane stops at 101; a proof names 102.
	mustRoot(t, s, testAnchor+2)
	mustRoot(t, s, testAnchor+2)
	// The height the lane did deliver is still the lane's.
	if r := mustRoot(t, s, testAnchor+1); r != rootN(1) {
		t.Fatalf("height 101 root %s, want the lane's", r)
	}
	wantProvenance(t, s, 1, 0, 2, 0)
}

// A height above the anchor that is first a gap and then re-anchored (the lane
// delivers its child, and the store fetches the parent by hash) stays fallback:
// re-anchoring changes how the store holds the height, not who supplied it.
// When the lane finally delivers the header itself it becomes the lane's.
func TestProvenanceReanchoredAboveAnchorStaysFallback(t *testing.T) {
	fb := &countingFallback{}
	lk := &stubLookup{height: testAnchor + 2, root: rootN(2)}
	s := provenanceStore(t, Options{Fallback: fb, Lookup: lk})
	anchor, _ := s.Tip()

	h101 := mineHeader(t, anchor, rootN(1), easyBits)
	o101, err := s.Observe(t.Context(), h101)
	if err != nil {
		t.Fatal(err)
	}
	h102 := mineHeader(t, o101.Hash, rootN(2), easyBits)
	h103 := mineHeader(t, headerHash(h102), rootN(3), easyBits)

	// 102 is missed by the lane: a proof naming it is a gap.
	mustRoot(t, s, testAnchor+2)
	wantProvenance(t, s, 0, 0, 1, 0)

	// 103 arrives; its parent 102 is re-anchored from the anchor.
	o103, err := s.Observe(t.Context(), h103)
	if err != nil || !o103.Reanchor || o103.Height != testAnchor+3 {
		t.Fatalf("observe 103: %+v %v", o103, err)
	}
	if r := mustRoot(t, s, testAnchor+2); r != rootN(2) {
		t.Fatalf("height 102 root %s, want the re-anchored %s", r, rootN(2))
	}
	wantProvenance(t, s, 0, 0, 2, 0)
	if s.Known(testAnchor + 2) {
		t.Fatal("a re-anchored height reports known")
	}

	// The lane delivers 102 late: from now on the answer is first-hand.
	if _, err := s.Observe(t.Context(), h102); err != nil {
		t.Fatal(err)
	}
	mustRoot(t, s, testAnchor+2)
	wantProvenance(t, s, 1, 0, 2, 0)
}

// A reorg that crosses the startup anchor can make a re-anchored header
// canonical AT or BELOW the anchor. That height is history the lane never
// carried to this process. The competing header the lane did deliver, at the
// anchor's own height, is the lane's.
func TestProvenanceReanchoredAtOrBelowAnchorIsHistory(t *testing.T) {
	lk := &stubLookup{height: testAnchor - 1, root: rootN(9)}
	s := provenanceStore(t, Options{Fallback: &countingFallback{}, Lookup: lk})

	var forkParent chainhash.Hash
	forkParent[0] = 0xEE
	b100 := mineHeader(t, forkParent, rootN(7), easyBits)
	ob100, err := s.Observe(t.Context(), b100)
	if err != nil || !ob100.Reanchor || ob100.Height != testAnchor || ob100.Tip {
		t.Fatalf("observe b100: %+v %v", ob100, err)
	}
	ob101, err := s.Observe(t.Context(), mineHeader(t, ob100.Hash, rootN(8), easyBits))
	if err != nil || !ob101.Tip || ob101.Height != testAnchor+1 {
		t.Fatalf("observe b101: %+v %v", ob101, err)
	}

	if r := mustRoot(t, s, testAnchor-1); r != rootN(9) {
		t.Fatalf("height 99 root %s, want the re-anchored %s", r, rootN(9))
	}
	wantProvenance(t, s, 0, 1, 0, 0)
	if r := mustRoot(t, s, testAnchor); r != rootN(7) {
		t.Fatalf("height 100 root %s, want the lane's %s", r, rootN(7))
	}
	mustRoot(t, s, testAnchor+1)
	wantProvenance(t, s, 2, 1, 0, 0)
}

// A height the lane delivered is the lane's, and costs the anchor nothing.
func TestProvenanceLaneHit(t *testing.T) {
	fb := &countingFallback{}
	s := provenanceStore(t, Options{Fallback: fb})
	prev, _ := s.Tip()
	for i := byte(1); i <= 3; i++ {
		obs, err := s.Observe(t.Context(), mineHeader(t, prev, rootN(i), easyBits))
		if err != nil {
			t.Fatal(err)
		}
		prev = obs.Hash
	}
	for h := uint32(testAnchor + 1); h <= testAnchor+3; h++ {
		mustRoot(t, s, h)
	}
	if fb.calls.Load() != 0 {
		t.Fatal("a lane height reached the anchor")
	}
	wantProvenance(t, s, 3, 0, 0, 0)
}

// No answer is a miss on either side of the boundary, never history or
// fallback.
func TestProvenanceMiss(t *testing.T) {
	none := provenanceStore(t, Options{})
	for _, h := range []uint32{50, testAnchor + 5} {
		if _, err := none.RootAt(t.Context(), h); err == nil {
			t.Fatalf("RootAt(%d) with no fallback answered", h)
		}
	}
	wantProvenance(t, none, 0, 0, 0, 2)

	failing := provenanceStore(t, Options{Fallback: &countingFallback{fail: map[uint32]bool{50: true, testAnchor + 5: true}}})
	for _, h := range []uint32{50, testAnchor + 5} {
		if _, err := failing.RootAt(t.Context(), h); err == nil {
			t.Fatalf("RootAt(%d) with a failing fallback answered", h)
		}
	}
	wantProvenance(t, failing, 0, 0, 0, 2)
}

// A height pruned out of the retention window was let go by design: the
// anchor's answer for it is history, while the heights the window still holds
// are the lane's and a height above the tip is still a gap.
func TestProvenancePrunedIsHistory(t *testing.T) {
	fb := &countingFallback{}
	s := provenanceStore(t, Options{Fallback: fb, Window: 5})
	prev, _ := s.Tip()
	for i := 0; i < 20; i++ {
		obs, err := s.Observe(t.Context(), mineHeader(t, prev, rootN(byte(i+1)), easyBits))
		if err != nil {
			t.Fatal(err)
		}
		prev = obs.Hash
	}
	// Tip 120, window 5: heights below 115 are pruned.
	if s.Known(110) {
		t.Fatal("test setup: height 110 is still held")
	}
	mustRoot(t, s, 110)
	wantProvenance(t, s, 0, 1, 0, 0)
	mustRoot(t, s, 118)
	wantProvenance(t, s, 1, 1, 0, 0)
	mustRoot(t, s, 121)
	wantProvenance(t, s, 1, 1, 1, 0)
}

// The chaintracks read path books the same way, and its known flag is
// unchanged: only a header received on the lane is known.
func TestHeaderForHeightProvenance(t *testing.T) {
	nf := &namingFallback{stubSource{roots: map[uint32]chainhash.Hash{50: rootN(5), testAnchor + 7: rootN(6)}}}
	s := provenanceStore(t, Options{Fallback: nf})
	anchor, _ := s.Tip()
	if _, err := s.Observe(t.Context(), mineHeader(t, anchor, rootN(1), easyBits)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		h     uint32
		known bool
	}{{50, false}, {testAnchor + 7, false}, {testAnchor + 1, true}} {
		_, _, known, err := s.HeaderForHeight(context.Background(), tc.h)
		if err != nil {
			t.Fatalf("HeaderForHeight(%d): %v", tc.h, err)
		}
		if known != tc.known {
			t.Fatalf("HeaderForHeight(%d) known = %v, want %v", tc.h, known, tc.known)
		}
	}
	wantProvenance(t, s, 1, 1, 1, 0)

	if _, _, _, err := s.HeaderForHeight(context.Background(), 60); err == nil {
		t.Fatal("an unknown height answered")
	}
	wantProvenance(t, s, 1, 1, 1, 1)
}
