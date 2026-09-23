package headers

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// easyBits is the maximum-difficulty-floor compact target: everything meets
// it. It is what a devnet uses, and it is exactly why a header may not be
// checked against its own declared bits alone.
const easyBits = 0x207fffff

// mineHeader builds an 80-byte header on prev with the given root and bits,
// grinding the nonce until it meets its own target. At easyBits this is the
// first try.
func mineHeader(t *testing.T, prev, root chainhash.Hash, bits uint32) []byte {
	t.Helper()
	hdr := make([]byte, Size)
	binary.LittleEndian.PutUint32(hdr[0:4], 1)
	copy(hdr[4:36], prev[:])
	copy(hdr[36:68], root[:])
	binary.LittleEndian.PutUint32(hdr[68:72], 1700000000)
	binary.LittleEndian.PutUint32(hdr[72:76], bits)
	target, ok := compactToBig(bits)
	if !ok {
		t.Fatalf("bad bits 0x%08x", bits)
	}
	for nonce := uint32(0); ; nonce++ {
		binary.LittleEndian.PutUint32(hdr[76:80], nonce)
		if workValue(headerHash(hdr)).Cmp(target) <= 0 {
			return hdr
		}
		if nonce > 1<<22 {
			t.Fatal("could not mine a header at the requested difficulty")
		}
	}
}

func rootN(n byte) chainhash.Hash {
	var r chainhash.Hash
	r[0] = n
	return r
}

func newStore(t *testing.T, o Options) *Store {
	t.Helper()
	if o.Anchor.Hash == (chainhash.Hash{}) {
		o.Anchor = Anchor{Hash: chainhash.Hash{0xAA}, Height: 100}
	}
	s, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestObserveChainsFromTheAnchor(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()

	h1 := mineHeader(t, anchor, rootN(1), easyBits)
	obs, err := s.Observe(context.Background(), h1)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.Chained || obs.Height != 101 || !obs.Tip {
		t.Fatalf("obs = %+v, want chained at 101 and tip", obs)
	}
	got, err := s.RootAt(context.Background(), 101)
	if err != nil || !got.IsEqual(&obs.Root) {
		t.Fatalf("RootAt(101) = %v/%v", got, err)
	}
	if !s.Known(101) {
		t.Fatal("height 101 should be known from the lane")
	}
	if _, h := s.Tip(); h != 101 {
		t.Fatalf("tip height = %d, want 101", h)
	}
}

// TestProofOfWorkIsEnforced is defect 1: the original computed a PoW flag and
// chained the header regardless, so a header that did not meet its target
// still became a merkle root the engine would trust.
func TestProofOfWorkIsEnforced(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()
	hdr := mineHeader(t, anchor, rootN(1), easyBits)
	// Claim a far harder target than the hash actually meets.
	binary.LittleEndian.PutUint32(hdr[72:76], 0x1d00ffff)

	obs, err := s.Observe(context.Background(), hdr)
	if !errors.Is(err, ErrProofOfWork) {
		t.Fatalf("err = %v, want ErrProofOfWork", err)
	}
	if obs.Chained {
		t.Fatal("a header failing proof of work was chained")
	}
	if _, h := s.Tip(); h != 100 {
		t.Fatalf("tip advanced to %d on a rejected header", h)
	}
	if s.Stats().Rejected != 1 {
		t.Fatalf("stats = %+v", s.Stats())
	}
}

// TestFloorRejectsASelfDeclaredEasyTarget is defect 2, and it is the one a
// header can lie its way past. A header carries the target it claims to meet,
// so checking it only against itself is a check the sender writes the answer
// to. The floor is the operator's, not the header's.
func TestFloorRejectsASelfDeclaredEasyTarget(t *testing.T) {
	// The store's floor is a real mainnet-shaped target...
	s := newStore(t, Options{MinBits: 0x1d00ffff})
	anchor, _ := s.Tip()
	// ...and the header declares the trivial one, which it does meet.
	hdr := mineHeader(t, anchor, rootN(1), easyBits)

	_, err := s.Observe(context.Background(), hdr)
	if !errors.Is(err, ErrProofOfWork) {
		t.Fatalf("err = %v, want ErrProofOfWork: an easy self-declared target must not pass", err)
	}

	// With no floor configured the same header is accepted, which is exactly
	// the posture the floor exists to replace.
	s2 := newStore(t, Options{})
	if _, err := s2.Observe(context.Background(), hdr); err != nil {
		t.Fatalf("with no floor: %v", err)
	}
}

// TestCompetingHeadersAreRetained is defect 3: the original wrote roots[h]
// unconditionally, so the second header at a height silently replaced the
// first and the engine's view of that height changed underneath it.
func TestCompetingHeadersAreRetained(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()

	a := mineHeader(t, anchor, rootN(1), easyBits)
	obsA, err := s.Observe(context.Background(), a)
	if err != nil {
		t.Fatalf("observe a: %v", err)
	}
	b := mineHeader(t, anchor, rootN(2), easyBits)
	obsB, err := s.Observe(context.Background(), b)
	if err != nil {
		t.Fatalf("observe b: %v", err)
	}
	if !obsB.Replaced {
		t.Fatal("a competing header at a claimed height was not reported as such")
	}
	if s.Stats().Replaced != 1 {
		t.Fatalf("stats = %+v", s.Stats())
	}
	// The canonical answer stays with the first: b did not extend the tip and
	// is not higher, so it is retained without becoming canonical.
	got, err := s.RootAt(context.Background(), 101)
	if err != nil {
		t.Fatalf("RootAt: %v", err)
	}
	if !got.IsEqual(&obsA.Root) {
		t.Fatalf("canonical root = %v, want a's %v", got, obsA.Root)
	}
	// Both are retained, so a reorganisation can select the other later.
	s.mu.Lock()
	n := len(s.at[101])
	s.mu.Unlock()
	if n != 2 {
		t.Fatalf("retained %d headers at 101, want 2", n)
	}
}

// TestReorgRewritesTheCanonicalChain pins that a longer branch takes over and
// the roots follow it, rather than the store keeping a stale answer.
func TestReorgRewritesTheCanonicalChain(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()

	a1 := mineHeader(t, anchor, rootN(1), easyBits)
	obsA1, _ := s.Observe(context.Background(), a1)

	// A competing branch from the same anchor, two long.
	b1 := mineHeader(t, anchor, rootN(2), easyBits)
	obsB1, err := s.Observe(context.Background(), b1)
	if err != nil {
		t.Fatalf("observe b1: %v", err)
	}
	b2 := mineHeader(t, obsB1.Hash, rootN(3), easyBits)
	obsB2, err := s.Observe(context.Background(), b2)
	if err != nil {
		t.Fatalf("observe b2: %v", err)
	}
	if !obsB2.Tip || obsB2.Height != 102 {
		t.Fatalf("b2 = %+v, want tip at 102", obsB2)
	}
	got, err := s.RootAt(context.Background(), 101)
	if err != nil {
		t.Fatalf("RootAt: %v", err)
	}
	if !got.IsEqual(&obsB1.Root) {
		t.Fatalf("after reorg height 101 root = %v, want b1's %v (was a1's %v)", got, obsB1.Root, obsA1.Root)
	}
}

type stubLookup struct {
	mu     sync.Mutex
	calls  int
	height uint32
	root   chainhash.Hash
	delay  time.Duration
	err    error
}

func (l *stubLookup) HeaderAt(ctx context.Context, _ chainhash.Hash) (uint32, chainhash.Hash, error) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	if l.delay > 0 {
		select {
		case <-time.After(l.delay):
		case <-ctx.Done():
			return 0, chainhash.Hash{}, ctx.Err()
		}
	}
	return l.height, l.root, l.err
}

func TestReanchorClosesAGap(t *testing.T) {
	lk := &stubLookup{height: 200, root: rootN(9)}
	s := newStore(t, Options{MinBits: easyBits, Lookup: lk})

	// A header whose parent this tail never saw.
	var unseen chainhash.Hash
	unseen[0] = 0xEE
	hdr := mineHeader(t, unseen, rootN(1), easyBits)

	obs, err := s.Observe(context.Background(), hdr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.Reanchor || !obs.Chained || obs.Height != 201 {
		t.Fatalf("obs = %+v, want re-anchored and chained at 201", obs)
	}
	if s.Stats().Reanchored != 1 {
		t.Fatalf("stats = %+v", s.Stats())
	}
}

// TestLookupIsBounded is defect 4. The original made an unbounded
// context.Background() call on the lane's hot path, so a slow third party
// stalled the lane indefinitely.
func TestLookupIsBounded(t *testing.T) {
	lk := &stubLookup{height: 200, root: rootN(9), delay: time.Hour}
	s := newStore(t, Options{MinBits: easyBits, Lookup: lk, LookupTimeout: 20 * time.Millisecond})

	var unseen chainhash.Hash
	unseen[0] = 0xEE
	hdr := mineHeader(t, unseen, rootN(1), easyBits)

	start := time.Now()
	obs, err := s.Observe(context.Background(), hdr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("observe took %v: the lookup was not bounded", elapsed)
	}
	if obs.Reanchor {
		t.Fatal("a timed-out lookup was treated as a successful re-anchor")
	}
	if s.Stats().Orphaned != 1 {
		t.Fatalf("stats = %+v, want the header counted as an orphan", s.Stats())
	}
}

// TestConcurrentReanchorResolvesOnce is the other half of defect 4: the
// original released the lock across the fetch and did not re-check on
// re-acquire, so two concurrent observations both fetched and the second
// overwrote the first's anchor.
func TestConcurrentReanchorResolvesOnce(t *testing.T) {
	lk := &stubLookup{height: 200, root: rootN(9), delay: 10 * time.Millisecond}
	s := newStore(t, Options{MinBits: easyBits, Lookup: lk})

	var unseen chainhash.Hash
	unseen[0] = 0xEE
	a := mineHeader(t, unseen, rootN(1), easyBits)
	b := mineHeader(t, unseen, rootN(2), easyBits)

	var wg sync.WaitGroup
	for _, hdr := range [][]byte{a, b} {
		wg.Add(1)
		go func(h []byte) {
			defer wg.Done()
			if _, err := s.Observe(context.Background(), h); err != nil {
				t.Errorf("Observe: %v", err)
			}
		}(hdr)
	}
	wg.Wait()

	// Exactly one re-anchor is recorded however many lookups raced.
	if got := s.Stats().Reanchored; got != 1 {
		t.Fatalf("reanchored = %d, want exactly 1", got)
	}
	if _, ok := s.byHash[unseen]; !ok {
		t.Fatal("the resolved parent was not retained")
	}
}

// TestWindowPrunes is defect 5: the original grew both maps forever.
func TestWindowPrunes(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits, Window: 5})
	prev, _ := s.Tip()
	for i := 0; i < 20; i++ {
		hdr := mineHeader(t, prev, rootN(byte(i+1)), easyBits)
		obs, err := s.Observe(context.Background(), hdr)
		if err != nil {
			t.Fatalf("observe %d: %v", i, err)
		}
		prev = obs.Hash
	}
	st := s.Stats()
	if st.TipHeight != 120 {
		t.Fatalf("tip = %d, want 120", st.TipHeight)
	}
	if st.Heights > 6 {
		t.Fatalf("retained %d heights with a window of 5", st.Heights)
	}
	if st.Pruned == 0 {
		t.Fatal("nothing was pruned")
	}
	// The window is behind the tip, so recent heights still answer...
	if !s.Known(120) {
		t.Fatal("the tip height was pruned")
	}
	// ...and old ones have genuinely gone.
	if s.Known(101) {
		t.Fatal("a height far outside the window is still retained")
	}
}

func TestObserveRejectsWrongSize(t *testing.T) {
	s := newStore(t, Options{})
	for _, n := range []int{0, 79, 81} {
		if _, err := s.Observe(context.Background(), make([]byte, n)); !errors.Is(err, ErrHeaderSize) {
			t.Fatalf("%d bytes: err = %v, want ErrHeaderSize", n, err)
		}
	}
}

func TestRedeliveryIsIdempotent(t *testing.T) {
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()
	hdr := mineHeader(t, anchor, rootN(1), easyBits)

	first, _ := s.Observe(context.Background(), hdr)
	second, err := s.Observe(context.Background(), hdr)
	if err != nil {
		t.Fatalf("re-delivery: %v", err)
	}
	if second.Height != first.Height || !second.Chained {
		t.Fatalf("re-delivery = %+v, want the same height as %+v", second, first)
	}
	if s.Stats().Replaced != 0 {
		t.Fatal("a re-delivery was counted as a competing header")
	}
}

// TestReanchoredRootBooksAgainstTheAnchor is the attribution defect, measured
// live in the VM lab: a header re-anchored across a gap sits in the canonical
// map like any other, so RootAt used to book its root as a LANE answer even
// though the anchor produced it. overlay_bridge_tracker_roots_total exists to
// say whether the lane feeds verification, and
// OverlayBridgeHeadersFromFallback alerts on it, so crediting the lane for a
// third party's answer makes the one crutch detector we have read clean while
// the host is leaning on the anchor.
//
// The header DELIVERED on the lane still books as lane. Only its fetched
// parent books as fallback.
func TestReanchoredRootBooksAgainstTheAnchor(t *testing.T) {
	lk := &stubLookup{height: 200, root: rootN(9)}
	s := newStore(t, Options{MinBits: easyBits, Lookup: lk})

	var unseen chainhash.Hash
	unseen[0] = 0xEE
	hdr := mineHeader(t, unseen, rootN(1), easyBits)

	obs, err := s.Observe(context.Background(), hdr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.Reanchor {
		t.Fatalf("obs = %+v, want a re-anchor", obs)
	}

	// 200 is the parent the ANCHOR supplied.
	if _, err := s.RootAt(context.Background(), 200); err != nil {
		t.Fatalf("RootAt(200): %v", err)
	}
	if st := s.Stats(); st.FromFallback != 1 || st.FromLane != 0 {
		t.Fatalf("after reading the re-anchored parent: stats = %+v, want FromFallback 1 and FromLane 0", st)
	}

	// 201 is the header the LANE delivered.
	if _, err := s.RootAt(context.Background(), 201); err != nil {
		t.Fatalf("RootAt(201): %v", err)
	}
	if st := s.Stats(); st.FromFallback != 1 || st.FromLane != 1 {
		t.Fatalf("after reading the lane's own header: stats = %+v, want FromFallback 1 and FromLane 1", st)
	}
}
