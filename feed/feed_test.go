package feed

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// beefObj is the smallest thing that passes the marker gate: a BRC-95 Atomic
// BEEF prefix. The feed makes no structural claim beyond the marker, so this
// is enough and deliberately so.
var beefObj = append([]byte{0x01, 0x01, 0x01, 0x01}, []byte("object bytes")...)

type stubSubmitter struct {
	calls  []string
	bodies [][]byte
	steak  overlay.Steak
	err    error
}

func (s *stubSubmitter) Submit(_ context.Context, topic string, object []byte) (overlay.Steak, error) {
	s.calls = append(s.calls, topic)
	cp := make([]byte, len(object))
	copy(cp, object)
	s.bodies = append(s.bodies, cp)
	return s.steak, s.err
}

func admitted(topic string) overlay.Steak {
	return overlay.Steak{topic: &overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{0}}}
}

func newFeed(t *testing.T, sub Submitter, topics ...string) *Feed {
	t.Helper()
	return &Feed{
		Topics: NewTopicMap(topics),
		Submit: sub,
		Guard:  guard.New(time.Minute, 1024),
		// A unit test must never sit on the production backoff. Retry
		// BEHAVIOUR is asserted explicitly by the tests that care, with their
		// own timings; everything else wants the give-up path immediately.
		SubmitRetries: -1,
	}
}

func TestHandleSubmitsWithTheElectedName(t *testing.T) {
	sub := &stubSubmitter{steak: admitted("tm_example")}
	f := newFeed(t, sub, "tm_example")
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)

	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sub.calls) != 1 || sub.calls[0] != "tm_example" {
		t.Fatalf("submitted to %v, want [tm_example]", sub.calls)
	}
	if string(sub.bodies[0]) != string(beefObj) {
		t.Fatalf("object was altered in flight")
	}
	st := f.Stats()
	if st.Submitted != 1 || st.Steak[SteakKey{"tm_example", OutcomeAdmitted}] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestHandleMarksGuardBeforeSubmitting is the ordering the loop guard depends
// on. Marking after the submit leaves a window in which a client's echo
// arrives before the mark lands and is published back up the tunnel.
func TestHandleMarksGuardBeforeSubmitting(t *testing.T) {
	g := guard.New(time.Minute, 1024)
	topic := "tm_example"
	topicID := objfmt.TopicID(topic)
	contentID := objfmt.ContentID(beefObj)

	var markedDuringSubmit bool
	sub := &stubSubmitterFunc{fn: func(context.Context, string, []byte) (overlay.Steak, error) {
		_, markedDuringSubmit = g.Lookup(contentID, topicID)
		return admitted(topic), nil
	}}
	f := &Feed{Topics: NewTopicMap([]string{topic}), Submit: sub, Guard: g}

	if err := f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(topicID, beefObj)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !markedDuringSubmit {
		t.Fatal("guard was not marked before the engine submit")
	}
	if dir, ok := g.Lookup(contentID, topicID); !ok || dir != registry.Delivered {
		t.Fatalf("guard direction = %v/%v, want Delivered", dir, ok)
	}
}

type stubSubmitterFunc struct {
	fn func(context.Context, string, []byte) (overlay.Steak, error)
}

func (s *stubSubmitterFunc) Submit(ctx context.Context, topic string, object []byte) (overlay.Steak, error) {
	return s.fn(ctx, topic, object)
}

// TestHandleRejects covers every refusal path. Each is lanes.ErrReject, which
// keeps the connection: the framing is intact and the object is the sender's
// problem, not ours.
func TestHandleRejects(t *testing.T) {
	big := make([]byte, 64)
	copy(big, []byte{0x01, 0x01, 0x01, 0x01})

	for _, tc := range []struct {
		name   string
		record []byte
		max    int
		want   func(Stats) uint64
	}{
		{
			name:   "object over the ceiling",
			record: objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), big),
			max:    16,
			want:   func(s Stats) uint64 { return s.Rejected },
		},
		{
			name:   "object does not lead with a BEEF marker",
			record: objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), []byte("not beef at all")),
			want:   func(s Stats) uint64 { return s.Rejected },
		},
		{
			name:   "topic this bridge did not elect",
			record: objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_other"), beefObj),
			want:   func(s Stats) uint64 { return s.UnknownTopic },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := &stubSubmitter{steak: admitted("tm_example")}
			f := newFeed(t, sub, "tm_example")
			f.MaxObject = tc.max
			err := f.Handle(context.Background(), tc.record)
			if !errors.Is(err, lanes.ErrReject) {
				t.Fatalf("err = %v, want lanes.ErrReject", err)
			}
			if len(sub.calls) != 0 {
				t.Fatalf("a rejected object reached the engine: %v", sub.calls)
			}
			if got := tc.want(f.Stats()); got != 1 {
				t.Fatalf("counter = %d, want 1; stats = %+v", got, f.Stats())
			}
		})
	}
}

// TestHandleParseErrorIsNotAReject separates a malformed record from a refused
// one: a reject is the sender's format problem, a parse error means the codec
// and the lane disagree, which is ours.
func TestHandleParseError(t *testing.T) {
	f := newFeed(t, &stubSubmitter{}, "tm_example")
	err := f.Handle(context.Background(), []byte{0x00, 0x01, 0x02})
	if err == nil || errors.Is(err, lanes.ErrReject) {
		t.Fatalf("err = %v, want a non-reject error", err)
	}
	if f.Stats().ParseError != 1 {
		t.Fatalf("stats = %+v", f.Stats())
	}
}

// TestSubmitRetriesThenRecovers is the data-loss fix. Dropping an object on
// the first engine error lost 55 and 56 objects on the two lab hosts in a
// single afternoon, every one of them while the engine was restarting for a
// deploy — an object that had already crossed the fabric and been written to
// the process, thrown away for a condition that clears in seconds.
func TestSubmitRetriesThenRecovers(t *testing.T) {
	sub := &flakySubmitter{failures: 2, steak: admitted("tm_example")}
	f := newFeed(t, sub, "tm_example")
	f.SubmitRetries = 3
	f.SubmitBackoff = time.Millisecond

	if err := f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	st := f.Stats()
	if st.Submitted != 1 || st.EngineError != 0 {
		t.Fatalf("stats = %+v, want the object submitted and NO engine error", st)
	}
	if st.Retried != 1 {
		t.Fatalf("stats = %+v, want Retried 1: a recovered submit must be distinguishable from one that never had trouble", st)
	}
	if sub.calls != 3 {
		t.Fatalf("submitter called %d times, want 3 (two failures then success)", sub.calls)
	}
}

// TestSubmitGivesUpAfterTheBudget proves the retry is BOUNDED. An unbounded
// retry on a worker would hold that worker through a real outage and turn a
// recoverable stall into a stalled bridge.
func TestSubmitGivesUpAfterTheBudget(t *testing.T) {
	sub := &flakySubmitter{failures: 1000}
	f := newFeed(t, sub, "tm_example")
	f.SubmitRetries = 2
	f.SubmitBackoff = time.Millisecond

	if err := f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)); err == nil {
		t.Fatal("want an error once the budget is spent")
	}
	if st := f.Stats(); st.EngineError != 1 || st.Submitted != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if sub.calls != 3 {
		t.Fatalf("submitter called %d times, want 3 (the first attempt plus 2 retries)", sub.calls)
	}
}

type flakySubmitter struct {
	failures int
	calls    int
	steak    overlay.Steak
}

func (s *flakySubmitter) Submit(_ context.Context, _ string, _ []byte) (overlay.Steak, error) {
	s.calls++
	if s.calls <= s.failures {
		return nil, errors.New("connection refused")
	}
	return s.steak, nil
}

func TestHandleEngineErrorIsCountedAndReturned(t *testing.T) {
	sub := &stubSubmitter{err: errors.New("connection refused")}
	f := newFeed(t, sub, "tm_example")
	err := f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj))
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v", err)
	}
	if f.Stats().EngineError != 1 {
		t.Fatalf("stats = %+v", f.Stats())
	}
}

// TestOutcomeClassification pins the three-value vocabulary, and in particular
// that an empty admittance is the expected duplicate answer rather than an
// error: the delivery pool replays its last object on every reconnect, so
// duplicates are the steady state.
func TestOutcomeClassification(t *testing.T) {
	topic := "tm_example"
	for _, tc := range []struct {
		name  string
		steak overlay.Steak
		want  string
	}{
		{"admitted", admitted(topic), OutcomeAdmitted},
		{"duplicate", overlay.Steak{topic: &overlay.AdmittanceInstructions{}}, OutcomeEmpty},
		{"no entry for our topic", overlay.Steak{"tm_other": &overlay.AdmittanceInstructions{}}, OutcomeError},
		{"nil entry", overlay.Steak{topic: nil}, OutcomeError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := outcomeOf(tc.steak, topic); got != tc.want {
				t.Fatalf("outcome = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestHandleCopiesBeforeHashing pins that the handler owns the bytes it
// hashes. The codec returns the object as a slice into the record and the lane
// reader aliases its own buffer, so a handler that hashed the alias could
// record an identity for bytes that have since changed.
func TestHandleCopiesBeforeHashing(t *testing.T) {
	g := guard.New(time.Minute, 1024)
	topic := "tm_example"
	topicID := objfmt.TopicID(topic)
	sub := &stubSubmitter{steak: admitted(topic)}
	f := &Feed{Topics: NewTopicMap([]string{topic}), Submit: sub, Guard: g}

	rec := objfmt.EncodeBEEFDelivery(topicID, beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Scribble over the record the way a reused lane buffer would.
	for i := range rec {
		rec[i] = 0xFF
	}
	if _, ok := g.Lookup(objfmt.ContentID(beefObj), topicID); !ok {
		t.Fatal("guard entry did not survive the record buffer being reused")
	}
	if string(sub.bodies[0]) != string(beefObj) {
		t.Fatal("the engine was handed an aliased buffer")
	}
}

// TestSinkModeCountsAndDropsWithoutAnEngine is the regression for a nil
// dereference: in sink mode there is no submitter, and a well-formed delivery
// for an elected topic used to reach f.Submit.Submit on a nil interface and
// take the whole bridge down on the first object.
func TestSinkModeCountsAndDropsWithoutAnEngine(t *testing.T) {
	f := &Feed{Topics: NewTopicMap([]string{"tm_example"}), Submit: nil}
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle in sink mode: %v", err)
	}
	if st := f.Stats(); st.Sunk != 1 || st.Submitted != 0 {
		t.Fatalf("stats = %+v, want one sunk and none submitted", st)
	}
}

// slowSubmitter blocks until released, standing in for a stalled engine.
type slowSubmitter struct {
	release chan struct{}
	calls   atomic.Int64
}

func (s *slowSubmitter) Submit(ctx context.Context, topic string, _ []byte) (overlay.Steak, error) {
	s.calls.Add(1)
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return admitted(topic), nil
}

// TestHandleDoesNotWaitOnTheEngine is the regression for the engine submit
// running inline in the lane's read loop. A stalled engine used to hold the
// delivery socket for the whole submit timeout, so the edge's write deadline
// tripped, it dropped and redialled, the pool replayed, and an engine outage
// became a connection storm. With workers, Handle returns as soon as the
// object is queued.
func TestHandleDoesNotWaitOnTheEngine(t *testing.T) {
	sub := &slowSubmitter{release: make(chan struct{})}
	f := &Feed{Topics: NewTopicMap([]string{"tm_example"}), Submit: sub, Workers: 1, QueueDepth: 8}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.Start(ctx) }()
	for !f.started.Load() {
		time.Sleep(time.Millisecond)
	}

	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	done := make(chan error, 1)
	go func() { done <- f.Handle(ctx, rec) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Handle: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Handle blocked on a stalled engine; the lane's socket would be held past the edge's deadline")
	}
	// The engine really is stalled, and the object really is waiting on it.
	if sub.calls.Load() == 0 {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) && sub.calls.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	close(sub.release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && f.Stats().Submitted == 0 {
		time.Sleep(time.Millisecond)
	}
	if f.Stats().Submitted != 1 {
		t.Fatalf("stats = %+v, want the queued object submitted once released", f.Stats())
	}
}

// TestHandleShedsWhenTheQueueIsFull pins the shed policy: a full queue refuses
// the delivery visibly rather than blocking the lane. The host misses that
// object until its own catch-up, which is a real loss and is preferred to
// stalling every later delivery behind it.
func TestHandleShedsWhenTheQueueIsFull(t *testing.T) {
	sub := &slowSubmitter{release: make(chan struct{})}
	defer close(sub.release)
	f := &Feed{Topics: NewTopicMap([]string{"tm_example"}), Submit: sub, Workers: 1, QueueDepth: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.Start(ctx) }()
	for !f.started.Load() {
		time.Sleep(time.Millisecond)
	}

	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj)
	// First fills the single worker, second fills the queue of one, third sheds.
	for i := 0; i < 2; i++ {
		if err := f.Handle(ctx, rec); err != nil {
			t.Fatalf("handle %d: %v", i, err)
		}
	}
	// Give the worker a moment to pick the first off the queue.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && sub.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if err := f.Handle(ctx, rec); !errors.Is(err, ErrShed) {
		// The queue may already have drained one; try once more.
		if err := f.Handle(ctx, rec); !errors.Is(err, ErrShed) {
			t.Fatalf("a full queue did not shed: %v", err)
		}
	}
	if f.Stats().Shed == 0 {
		t.Fatalf("stats = %+v, want a shed count", f.Stats())
	}
}

// TestHandleRecordPayloadLandsOnEveryElectedName covers the record-carrying
// delivery: the plane delivered once, under the first elected topic it
// matched, with every name the publisher wrote in the payload. The feed
// submits the object to each elected name in the record (the other elected
// topic would otherwise never see it), marks the guard for each, and leaves
// names it did not elect alone.
func TestHandleRecordPayloadLandsOnEveryElectedName(t *testing.T) {
	sub := &stubSubmitter{steak: admitted("tm_a")}
	f := newFeed(t, sub, "tm_a", "tm_c")
	payload, err := objfmt.EncodeBEEFRecord([]string{"tm_a", "tm_b", "tm_c", "tm_label"}, beefObj)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_a"), payload)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sub.calls) != 2 || sub.calls[0] != "tm_a" || sub.calls[1] != "tm_c" {
		t.Fatalf("submitted to %v, want [tm_a tm_c]", sub.calls)
	}
	for i, body := range sub.bodies {
		if string(body) != string(beefObj) {
			t.Fatalf("submit %d carried the record, not the object", i)
		}
	}
	cid := objfmt.ContentID(beefObj)
	for _, name := range []string{"tm_a", "tm_c"} {
		if dir, ok := f.Guard.Lookup(cid, objfmt.TopicID(name)); !ok || dir != registry.Delivered {
			t.Fatalf("guard not marked Delivered for %s", name)
		}
	}
	if _, ok := f.Guard.Lookup(cid, objfmt.TopicID("tm_b")); ok {
		t.Fatal("guard marked for a topic this bridge did not elect")
	}

	// A record with the matched name only, or a bare object, is one submit.
	sub2 := &stubSubmitter{steak: admitted("tm_a")}
	f2 := newFeed(t, sub2, "tm_a", "tm_c")
	if err := f2.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_a"), beefObj)); err != nil {
		t.Fatalf("bare Handle: %v", err)
	}
	if len(sub2.calls) != 1 {
		t.Fatalf("bare object submitted to %v, want [tm_a]", sub2.calls)
	}

	// A malformed record inside the delivery is a parse error, not a reject.
	bad := append(append([]byte(nil), payload...), 0x00)
	if err := f2.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_a"), bad)); err == nil {
		t.Fatal("trailing byte after the record accepted")
	}
}
