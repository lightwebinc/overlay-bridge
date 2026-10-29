package feed

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/shard-common/objfmt"
)

// alsoStub is a target safe for the also workers' goroutines.
type alsoStub struct {
	mu     sync.Mutex
	calls  []string
	bodies [][]byte
	errs   []error // answered in order; the last repeats
	block  chan struct{}
}

func (s *alsoStub) Submit(_ context.Context, topic string, object []byte) (overlay.Steak, error) {
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, topic)
	s.bodies = append(s.bodies, append([]byte(nil), object...))
	if len(s.errs) == 0 {
		return admitted(topic), nil
	}
	i := len(s.calls) - 1
	if i >= len(s.errs) {
		i = len(s.errs) - 1
	}
	return nil, s.errs[i]
}

func (s *alsoStub) n() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func startAlso(t *testing.T, a *Also) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = a.Start(ctx) }()
	waitFor(t, "also workers", a.started.Load)
}

func TestAlsoResults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		errs      []error
		want      string
		wantCalls int
	}{
		{"taken", nil, AlsoOK, 1},
		{"refused, asked once", []error{&StatusError{Code: 400, Body: "piece: bad signature"}}, AlsoRefused, 1},
		{"recovers on a retry", []error{&StatusError{Code: 503}, nil}, AlsoOK, 2},
		{"fails every try", []error{errors.New("connection refused")}, AlsoError, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &alsoStub{errs: tc.errs}
			a := &Also{Submit: stub, Name: "tm_x_*", Target: "http://p", Workers: 1, Retries: 2, Backoff: time.Millisecond}
			startAlso(t, a)
			a.Offer("tm_x_a", beefObj)
			waitFor(t, tc.want, func() bool { return a.Stats().Results[tc.want] == 1 })
			if got := stub.n(); got != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", got, tc.wantCalls)
			}
			if b := a.Stats().QueuedBytes; b != 0 {
				t.Fatalf("queued bytes after the drain = %d", b)
			}
		})
	}
}

// An offer never blocks and never queues past its bounds: before the
// workers run, past the count, and past the bytes.
func TestAlsoSheds(t *testing.T) {
	var nilAlso *Also
	nilAlso.Offer("tm_x_a", beefObj) // a nil target is a no-op

	a := &Also{Submit: &alsoStub{}, Workers: 1}
	a.Offer("tm_x_a", beefObj)
	if a.Stats().Results[AlsoShed] != 1 {
		t.Fatal("an offer before the workers run was not shed")
	}

	stub := &alsoStub{block: make(chan struct{})}
	b := &Also{Submit: stub, Workers: 1, QueueDepth: 8, QueueBytes: int64(2*len(beefObj) + 1)}
	startAlso(t, b)
	b.Offer("tm_x_a", beefObj) // taken by the worker, which blocks: its bytes still count
	b.Offer("tm_x_a", beefObj) // queued
	b.Offer("tm_x_a", beefObj) // past the byte bound
	if got := b.Stats().Results[AlsoShed]; got != 1 {
		t.Fatalf("shed = %d, want 1 past the byte bound", got)
	}
	close(stub.block)
	waitFor(t, "the drain", func() bool { return b.Stats().Results[AlsoOK] == 2 })
	if q := b.Stats().QueuedBytes; q != 0 {
		t.Fatalf("queued bytes after the drain = %d", q)
	}

	c := &Also{Submit: &alsoStub{}, Workers: 1, QueueBytes: 4}
	startAlso(t, c)
	c.Offer("tm_x_a", beefObj) // larger than the whole bound
	if c.Stats().Results[AlsoShed] != 1 {
		t.Fatal("an object larger than the byte bound was queued")
	}
}

func TestParseRouteSpecAlso(t *testing.T) {
	rs, err := ParseRouteSpec("tm_bstore_*=http://127.0.0.1:8280,also=http://127.0.0.1:8095, also = http://127.0.0.1:8096")
	if err != nil {
		t.Fatal(err)
	}
	if rs.Pattern != "tm_bstore_*" || rs.Primary != "http://127.0.0.1:8280" || rs.Shadow != "" ||
		len(rs.Also) != 2 || rs.Also[0] != "http://127.0.0.1:8095" || rs.Also[1] != "http://127.0.0.1:8096" {
		t.Fatalf("spec = %+v", rs)
	}
	for in, want := range map[string]string{
		"tm_*=http://h:1,also=http://h:1/":                  "already",
		"tm_*=http://h:1,shadow=http://h:2,also=http://h:2": "already",
		"tm_*=http://h:1,also=http://h:3,also=http://h:3":   "already",
		"tm_*=http://h:1,also=http://h:3/api/v1":            "bare origin",
		"tm_*=http://h:1,also":                              "not key=value",
	} {
		if _, err := ParseRouteSpec(in); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want one containing %q", in, err, want)
		}
	}
}

// The feed hands a routed topic's deliveries to the route's also target
// whatever the primary answers, and leaves other topics alone.
func TestFeedHandsDeliveriesToAlso(t *testing.T) {
	stub := &alsoStub{}
	a := &Also{Submit: stub, Name: "tm_bstore_*", Target: "http://p", Workers: 1}
	startAlso(t, a)
	primary := &stubSubmitter{err: &StatusError{Code: 400, Body: "refused"}}
	f := newFeed(t, &stubSubmitter{steak: admitted("tm_borg_lab")}, "tm_bstore_lab", "tm_borg_lab")
	f.Routes = []*Route{{Pattern: "tm_bstore_*", Submit: primary, Also: []*Also{a}}}

	for _, topic := range []string{"tm_bstore_lab", "tm_borg_lab"} {
		_ = f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID(topic), beefObj))
	}
	waitFor(t, "the also target", func() bool { return a.Stats().Results[AlsoOK] == 1 })
	if len(primary.calls) != 1 {
		t.Fatalf("primary calls = %v", primary.calls)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.calls) != 1 || stub.calls[0] != "tm_bstore_lab" || string(stub.bodies[0]) != string(beefObj) {
		t.Fatalf("also target got %v", stub.calls)
	}
}
