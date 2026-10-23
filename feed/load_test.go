package feed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
)

// TestLoadAccounting pins what the queue, worker and shed series read: a
// worker stalled on a routed engine counts busy under that route's submit
// stage, the object behind it is the queue's depth, the next delivery sheds
// under the route of its topic, and the wait a worker saw is reported when
// it takes an object.
func TestLoadAccounting(t *testing.T) {
	routed := &slowSubmitter{release: make(chan struct{})}
	def := &slowSubmitter{release: make(chan struct{})}
	close(def.release)
	var waits atomic.Int64
	f := &Feed{
		Topics:     NewTopicMap([]string{"tm_app_a", "tm_lab"}),
		Submit:     def,
		Routes:     []*Route{{Pattern: "tm_app_*", Submit: routed}},
		Workers:    1,
		QueueDepth: 1,
		OnDequeued: func(time.Duration) { waits.Add(1) },
	}
	if got := f.RouteNames(); len(got) != 2 || got[0] != DefaultRoute || got[1] != "tm_app_*" {
		t.Fatalf("route names %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.Start(ctx) }()
	for !f.started.Load() {
		time.Sleep(time.Millisecond)
	}

	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_app_a"), beefObj)
	if err := f.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a worker busy on the route", func() bool { return f.Busy()[BusyKey{Route: "tm_app_*", Stage: StageSubmit}] == 1 })
	if q, c := f.Backlog(); q != 0 || c != 1 {
		t.Fatalf("backlog %d/%d with the only object on the worker", q, c)
	}
	if err := f.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if q, _ := f.Backlog(); q != 1 {
		t.Fatalf("backlog %d, want the second object queued", q)
	}
	if err := f.Handle(ctx, rec); !errors.Is(err, ErrShed) {
		t.Fatalf("a full queue did not shed: %v", err)
	}
	if err := f.Handle(ctx, objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_lab"), beefObj)); !errors.Is(err, ErrShed) {
		t.Fatalf("a full queue did not shed: %v", err)
	}
	st := f.Stats()
	if st.Shed != 2 || st.ShedByRoute["tm_app_*"] != 1 || st.ShedByRoute[DefaultRoute] != 1 {
		t.Fatalf("shed %d by route %v", st.Shed, st.ShedByRoute)
	}

	close(routed.release)
	waitFor(t, "both objects submitted", func() bool { return f.Stats().Submitted == 2 })
	waitFor(t, "the worker idle", func() bool { return f.Busy()[BusyKey{Route: "tm_app_*", Stage: StageSubmit}] == 0 })
	if waits.Load() != 2 {
		t.Fatalf("dequeue waits reported %d, want 2", waits.Load())
	}
}
