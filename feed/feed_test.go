package feed

import (
	"context"
	"errors"
	"strings"
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
