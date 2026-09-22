package facade

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/registry"
)

var beefObj = append([]byte{0x01, 0x01, 0x01, 0x01}, []byte("object bytes")...)

type stubEngine struct {
	wrapped bool
	err     error
	calls   int
}

func (s *stubEngine) Submit(ctx context.Context, topic string, obj []byte) (overlay.Steak, error) {
	a, err := s.SubmitDetail(ctx, topic, obj)
	return a.Steak, err
}

func (s *stubEngine) SubmitDetail(_ context.Context, topic string, _ []byte) (feed.Answer, error) {
	s.calls++
	if s.err != nil {
		return feed.Answer{}, s.err
	}
	return feed.Answer{
		Steak:   overlay.Steak{topic: &overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{0}}},
		Wrapped: s.wrapped,
	}, nil
}

type stubPublisher struct {
	records [][]byte
	err     error
}

func (p *stubPublisher) Publish(_ context.Context, rec []byte) error {
	if p.err != nil {
		return p.err
	}
	p.records = append(p.records, rec)
	return nil
}

func newFacade(t *testing.T, eng feed.DetailSubmitter, pub Publisher, topics ...string) (*Facade, *guard.Guard) {
	t.Helper()
	set := map[string]struct{}{}
	for _, n := range topics {
		set[n] = struct{}{}
	}
	g := guard.New(time.Minute, 1024)
	return New(Config{Engine: eng, Publish: pub, Guard: g, Topics: set}), g
}

func submit(t *testing.T, f *Facade, topics string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(string(body)))
	if topics != "" {
		req.Header.Set("x-topics", topics)
	}
	rec := httptest.NewRecorder()
	f.Handler().ServeHTTP(rec, req)
	return rec
}

// TestSubmitForwardsThenPublishes is the unknown-to-the-guard row: forward to
// the engine, publish once.
func TestSubmitForwardsThenPublishes(t *testing.T) {
	eng := &stubEngine{}
	pub := &stubPublisher{}
	f, g := newFacade(t, eng, pub, "tm_example")

	rec := submit(t, f, "tm_example", beefObj)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	if eng.calls != 1 {
		t.Fatalf("engine calls = %d, want 1", eng.calls)
	}
	if len(pub.records) != 1 {
		t.Fatalf("published %d records, want 1", len(pub.records))
	}
	// One record per topic, never a multi-topic record: the open ingress
	// admits a single topic and books the rest as a refusal nobody diagnoses.
	topicsIn, _, err := decodeRecordTopics(pub.records[0])
	if err != nil {
		t.Fatalf("decode published record: %v", err)
	}
	if len(topicsIn) != 1 || topicsIn[0] != "tm_example" {
		t.Fatalf("record names %v, want exactly [tm_example]", topicsIn)
	}
	// The object is now marked, so a re-submission is recognised.
	if _, known := g.Lookup(objfmt.ContentID(beefObj), objfmt.TopicID("tm_example")); !known {
		t.Fatal("guard was not marked")
	}
	if st := f.Stats(); st.OK != 1 || st.LoopDropped != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func decodeRecordTopics(rec []byte) ([]string, []byte, error) {
	r, _, err := objfmt.DecodeBEEFRecord(rec)
	if err != nil {
		return nil, nil, err
	}
	return r.Topics, r.Object, nil
}

// TestLoopDroppedForwardsButDoesNotPublish is the row that matters most: an
// object the plane delivered must never be pushed back up the tunnel.
func TestLoopDroppedForwardsButDoesNotPublish(t *testing.T) {
	eng := &stubEngine{}
	pub := &stubPublisher{}
	f, g := newFacade(t, eng, pub, "tm_example")

	// The feed marks Delivered when the plane delivers an object.
	g.Mark(objfmt.ContentID(beefObj), objfmt.TopicID("tm_example"), registry.Delivered)

	rec := submit(t, f, "tm_example", beefObj)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	if eng.calls != 1 {
		t.Fatalf("engine calls = %d, want 1: a loop is still forwarded, the host dedups", eng.calls)
	}
	if len(pub.records) != 0 {
		t.Fatalf("published %d records, want 0: this was a loop", len(pub.records))
	}
	if st := f.Stats(); st.LoopDropped != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestRetryDoesNotPublishTwice: a client retrying the same bytes forwards
// again (harmless) but must not put a second copy on the plane.
func TestRetryDoesNotPublishTwice(t *testing.T) {
	eng := &stubEngine{}
	pub := &stubPublisher{}
	f, _ := newFacade(t, eng, pub, "tm_example")

	for i := 0; i < 3; i++ {
		if rec := submit(t, f, "tm_example", beefObj); rec.Code != http.StatusOK {
			t.Fatalf("submit %d: code = %d", i, rec.Code)
		}
	}
	if eng.calls != 3 {
		t.Fatalf("engine calls = %d, want 3", eng.calls)
	}
	if len(pub.records) != 1 {
		t.Fatalf("published %d records, want exactly 1", len(pub.records))
	}
}

// TestReplyKeepsTheHostsShape: a client written against either engine must be
// able to read the reply when it is pointed at the facade.
func TestReplyKeepsTheHostsShape(t *testing.T) {
	t.Run("bare host answers bare", func(t *testing.T) {
		f, _ := newFacade(t, &stubEngine{wrapped: false}, &stubPublisher{}, "tm_example")
		rec := submit(t, f, "tm_example", beefObj)
		var bare map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &bare); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, isWrapped := bare["STEAK"]; isWrapped {
			t.Fatalf("bare host's answer was wrapped: %s", rec.Body)
		}
		if _, ok := bare["tm_example"]; !ok {
			t.Fatalf("topic missing from bare answer: %s", rec.Body)
		}
	})
	t.Run("wrapping host answers wrapped", func(t *testing.T) {
		f, _ := newFacade(t, &stubEngine{wrapped: true}, &stubPublisher{}, "tm_example")
		rec := submit(t, f, "tm_example", beefObj)
		var wrapped map[string]map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &wrapped); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := wrapped["STEAK"]["tm_example"]; !ok {
			t.Fatalf("wrapping host's answer was normalised: %s", rec.Body)
		}
	})
}

func TestSubmitRejects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		topics string
		body   []byte
		want   int
	}{
		{"no topics header", "", beefObj, http.StatusBadRequest},
		{"empty topics header", "   ", beefObj, http.StatusBadRequest},
		{"topic this bridge does not carry", "tm_other", beefObj, http.StatusBadRequest},
		{"body is not a BEEF object", "tm_example", []byte("hello"), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := &stubEngine{}
			pub := &stubPublisher{}
			f, _ := newFacade(t, eng, pub, "tm_example")
			if rec := submit(t, f, tc.topics, tc.body); rec.Code != tc.want {
				t.Fatalf("code = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if eng.calls != 0 || len(pub.records) != 0 {
				t.Fatalf("a rejected submission reached the engine or the plane")
			}
		})
	}
}

func TestOversizeObjectIsRefused(t *testing.T) {
	eng := &stubEngine{}
	f, _ := newFacade(t, eng, &stubPublisher{}, "tm_example")
	f.cfg.MaxObject = 16
	big := append([]byte{0x01, 0x01, 0x01, 0x01}, make([]byte, 64)...)
	if rec := submit(t, f, "tm_example", big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413", rec.Code)
	}
	if eng.calls != 0 {
		t.Fatal("an oversize object reached the engine")
	}
}

// TestEngineErrorIsABadGateway: the caller requeues the whole submission
// rather than being handed a per-item verdict list to vote on.
func TestEngineErrorIsABadGateway(t *testing.T) {
	eng := &stubEngine{err: errors.New("connection refused")}
	pub := &stubPublisher{}
	f, _ := newFacade(t, eng, pub, "tm_example")
	if rec := submit(t, f, "tm_example", beefObj); rec.Code != http.StatusBadGateway {
		t.Fatalf("code = %d, want 502", rec.Code)
	}
	if len(pub.records) != 0 {
		t.Fatal("an object was published although the engine rejected it")
	}
	if f.Stats().EngineError != 1 {
		t.Fatalf("stats = %+v", f.Stats())
	}
}

// TestPublishFailureIsCounted pins that a failed publish is visible. The
// object is admitted locally but is NOT on the plane, and reporting a clean
// publish would make the bridge claim a publication it never made.
func TestPublishFailureIsCounted(t *testing.T) {
	pub := &stubPublisher{err: errors.New("queue full")}
	f, _ := newFacade(t, &stubEngine{}, pub, "tm_example")
	submit(t, f, "tm_example", beefObj)
	if f.Stats().PublishFailed != 1 {
		t.Fatalf("stats = %+v", f.Stats())
	}
}

// TestMultiTopicPublishesOneRecordEach pins the shape the open ingress
// requires: never one record naming several topics.
func TestMultiTopicPublishesOneRecordEach(t *testing.T) {
	pub := &stubPublisher{}
	f, _ := newFacade(t, &stubEngine{}, pub, "tm_a", "tm_b")
	if rec := submit(t, f, "tm_a,tm_b", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	if len(pub.records) != 2 {
		t.Fatalf("published %d records, want one per topic", len(pub.records))
	}
	for i, r := range pub.records {
		names, _, err := decodeRecordTopics(r)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if len(names) != 1 {
			t.Fatalf("record %d names %v; a multi-topic record is refused at ingress", i, names)
		}
	}
}
