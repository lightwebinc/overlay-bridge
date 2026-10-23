package feed

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/lightwebinc/shard-common/objfmt"
)

// shadowStub is a fake engine for the shadow side: it records what it was
// handed, answers a fixed STEAK or error, and can be made to block so a
// test can fill the queue.
type shadowStub struct {
	mu      sync.Mutex
	calls   []string
	bodies  [][]byte
	markers []string
	steak   overlay.Steak
	err     error
	block   chan struct{} // when set, every submit waits on it
}

func (s *shadowStub) Submit(ctx context.Context, topic string, object []byte) (overlay.Steak, error) {
	return s.SubmitMarked(ctx, topic, object, "")
}

func (s *shadowStub) SubmitMarked(ctx context.Context, topic string, object []byte, marker string) (overlay.Steak, error) {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, topic)
	s.bodies = append(s.bodies, append([]byte(nil), object...))
	s.markers = append(s.markers, marker)
	return s.steak, s.err
}

func (s *shadowStub) n() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// markedStub is a primary that records the marker it was handed.
type markedStub struct {
	stubSubmitter
	markers []string
}

func (m *markedStub) SubmitMarked(ctx context.Context, topic string, object []byte, marker string) (overlay.Steak, error) {
	m.markers = append(m.markers, marker)
	return m.Submit(ctx, topic, object)
}

func startShadow(t *testing.T, sh *Shadow) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = sh.Start(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for !sh.started.Load() {
		if time.Now().After(deadline) {
			t.Fatal("shadow workers did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitFor polls until cond holds or the test's patience runs out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func total(s ShadowStats) uint64 {
	var n uint64
	for _, v := range s.Kinds {
		n += v
	}
	return n
}

func TestShadowIsFedEverySubmitAndCountsAMatch(t *testing.T) {
	primary := &stubSubmitter{steak: admitted("tm_example")}
	shadowEng := &shadowStub{steak: admitted("tm_example")}
	sh := &Shadow{Submit: shadowEng, Workers: 1}
	startShadow(t, sh)
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	waitFor(t, "the shadow submit", func() bool { return total(sh.Stats()) == 1 })
	st := sh.Stats()
	if st.Kinds[ShadowMatch] != 1 || st.Submitted != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if shadowEng.calls[0] != "tm_example" || !bytes.Equal(shadowEng.bodies[0], beefObj) {
		t.Fatalf("shadow got %v %q", shadowEng.calls, shadowEng.bodies[0])
	}
	// The primary's own books are those of a feed with no shadow.
	if fs := f.Stats(); fs.Submitted != 1 || fs.Steak[SteakKey{"tm_example", OutcomeAdmitted}] != 1 {
		t.Fatalf("primary stats = %+v", fs)
	}
}

func TestShadowGetsTheSameMarker(t *testing.T) {
	primary := &markedStub{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	shadowEng := &shadowStub{steak: admitted("tm_example")}
	sh := &Shadow{Submit: shadowEng, Workers: 1}
	startShadow(t, sh)
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	f.Verify = fixedVerifier{vd: spv.Verdict{Result: spv.ResultOK}}
	f.VerifyMode = VerifyEnforce
	f.MarkerKey = bytes.Repeat([]byte{7}, 32)
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	waitFor(t, "the shadow submit", func() bool { return shadowEng.n() == 1 })
	if len(primary.markers) != 1 || primary.markers[0] == "" {
		t.Fatalf("primary marker %v", primary.markers)
	}
	if shadowEng.markers[0] != primary.markers[0] {
		t.Fatalf("shadow marker %q, primary %q", shadowEng.markers[0], primary.markers[0])
	}
}

func TestClassify(t *testing.T) {
	ai := func(outs ...uint32) *overlay.AdmittanceInstructions {
		return &overlay.AdmittanceInstructions{OutputsToAdmit: outs}
	}
	for _, tc := range []struct {
		name            string
		primary, shadow *overlay.AdmittanceInstructions
		want            string
	}{
		{"both admit the same", ai(0, 1), ai(1, 0), ShadowMatch},
		{"both empty (the duplicate answer)", ai(), ai(), ShadowMatch},
		{"both nil", nil, nil, ShadowMatch},
		{"empty and nil are both nothing", ai(), nil, ShadowMatch},
		{"primary admits, shadow empty", ai(0), ai(), ShadowPrimaryOnly},
		{"primary admits, shadow none", ai(0), nil, ShadowPrimaryOnly},
		{"primary none, shadow admits", nil, ai(0), ShadowShadowOnly},
		{"primary empty, shadow retains", ai(), &overlay.AdmittanceInstructions{CoinsToRetain: []uint32{2}}, ShadowShadowOnly},
		{"both admit, not the same", ai(0), ai(1), ShadowDiffer},
		{"same outputs, other coins", &overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{0}, CoinsToRetain: []uint32{1}}, ai(0), ShadowDiffer},
	} {
		if got := Classify(tc.primary, tc.shadow); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestShadowErrorLeavesThePrimaryAlone(t *testing.T) {
	primary := &stubSubmitter{steak: admitted("tm_example")}
	shadowEng := &shadowStub{err: errors.New("connection refused")}
	sh := &Shadow{Submit: shadowEng, Workers: 1}
	startShadow(t, sh)
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	waitFor(t, "the shadow error", func() bool { return sh.Stats().Kinds[ShadowError] == 1 })
	if fs := f.Stats(); fs.Submitted != 1 || fs.EngineError != 0 {
		t.Fatalf("primary stats = %+v", fs)
	}
	// A shadow that answers without an entry for the topic is an error too.
	shadowEng.err, shadowEng.steak = nil, overlay.Steak{"tm_other": &overlay.AdmittanceInstructions{}}
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	waitFor(t, "the second shadow error", func() bool { return sh.Stats().Kinds[ShadowError] == 2 })
}

func TestShadowIsOfferedAPrimaryFailureToo(t *testing.T) {
	primary := &stubSubmitter{err: &StatusError{Code: 400, Body: "bad"}}
	shadowEng := &shadowStub{steak: admitted("tm_example")}
	sh := &Shadow{Submit: shadowEng, Workers: 1}
	startShadow(t, sh)
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	_ = f.Handle(context.Background(), rec)
	waitFor(t, "the shadow submit", func() bool { return total(sh.Stats()) == 1 })
	if st := sh.Stats(); st.Kinds[ShadowShadowOnly] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestShadowQueueOverflowDropsNeverBlocks is the property the whole design
// rests on: a shadow that has stopped answering costs the primary path
// nothing but a counter.
func TestShadowQueueOverflowDropsNeverBlocks(t *testing.T) {
	primary := &stubSubmitter{steak: admitted("tm_example")}
	shadowEng := &shadowStub{steak: admitted("tm_example"), block: make(chan struct{})}
	sh := &Shadow{Submit: shadowEng, Workers: 1, QueueDepth: 1}
	startShadow(t, sh)
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			if err := f.Handle(context.Background(), rec); err != nil {
				t.Errorf("Handle %d: %v", i, err)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the primary path blocked behind the shadow")
	}
	if fs := f.Stats(); fs.Submitted != 5 {
		t.Fatalf("primary submitted %d, want 5", fs.Submitted)
	}
	// One is held by the worker, one sits in the queue, the rest dropped.
	waitFor(t, "drops", func() bool { return sh.Stats().Kinds[ShadowDropped] >= 3 })
	close(shadowEng.block)
	waitFor(t, "the held submits", func() bool { return sh.Stats().Kinds[ShadowMatch] == 2 })
	if st := sh.Stats(); st.Kinds[ShadowDropped] != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestShadowNotStartedDrops(t *testing.T) {
	primary := &stubSubmitter{steak: admitted("tm_example")}
	sh := &Shadow{Submit: &shadowStub{steak: admitted("tm_example")}}
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if st := sh.Stats(); st.Kinds[ShadowDropped] != 1 || total(st) != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestShadowLogsOnlyTheFirstMismatches(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	primary := &stubSubmitter{steak: admitted("tm_example")}
	shadowEng := &shadowStub{steak: overlay.Steak{"tm_example": &overlay.AdmittanceInstructions{}}}
	sh := &Shadow{Submit: shadowEng, Workers: 1, LogMismatches: 2, Log: log}
	startShadow(t, sh)
	f := newFeed(t, primary, "tm_example")
	f.Shadow = sh
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	for i := 0; i < 4; i++ {
		if err := f.Handle(context.Background(), rec); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	waitFor(t, "four comparisons", func() bool { return sh.Stats().Kinds[ShadowPrimaryOnly] == 4 })
	if n := strings.Count(buf.String(), "MISMATCH"); n != 2 {
		t.Fatalf("%d mismatch lines, want 2:\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "topic=tm_example") || !strings.Contains(buf.String(), "txid=") {
		t.Fatalf("log lacks txid or topic:\n%s", buf.String())
	}
}

func TestObjectTxID(t *testing.T) {
	atomic := append([]byte{1, 1, 1, 1}, bytes.Repeat([]byte{0xab}, 32)...)
	atomic = append(atomic, 0xff)
	if got := objectTxID(atomic); got != strings.Repeat("ab", 32) {
		t.Fatalf("atomic: %s", got)
	}
	if got := objectTxID([]byte("plain")); !strings.HasPrefix(got, "content:") || len(got) != len("content:")+64 {
		t.Fatalf("plain: %s", got)
	}
}
