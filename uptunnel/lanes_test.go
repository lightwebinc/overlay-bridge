package uptunnel

import (
	"context"
	"testing"
	"time"
)

// laneRig is n lanes, each in front of its own sink.
type laneRig struct {
	sinks  []*sink
	queues []*Queue
	lanes  *Lanes
}

func newLaneRig(t *testing.T, n int) *laneRig {
	t.Helper()
	r := &laneRig{}
	for i := 0; i < n; i++ {
		s := newSink(t)
		t.Cleanup(s.close)
		q := NewQueue(&Client{Addrs: []string{s.addr()}}, 64, nil)
		q.Wait = 5 * time.Second // a publisher ahead of the drain waits for room
		r.sinks, r.queues = append(r.sinks, s), append(r.queues, q)
	}
	r.lanes = NewLanes(r.queues...)
	return r
}

// TestLanesShareLevelLoad: with every lane draining, records are dealt across
// all of them, each record whole on one lane, and every one is sent.
func TestLanesShareLevelLoad(t *testing.T) {
	r := newLaneRig(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.lanes.Run(ctx) }()
	const n = 300
	for i := 0; i < n; i++ {
		if err := r.lanes.Publish(ctx, []byte{0xBE, 0xEF, byte(i), byte(i >> 8)}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, "every record sent", func() bool { return r.lanes.Stats().Sent == n })
	for i, s := range r.lanes.LaneStats() {
		if s.Sent < n/6 {
			t.Errorf("lane %d sent %d of %d: level lanes should take turns", i, s.Sent, n)
		}
	}
	if st := r.lanes.Stats(); st.Shed != 0 || st.Failed != 0 {
		t.Fatalf("stats = %+v, want nothing shed or failed", st)
	}
}

// TestLanesPreferTheEmptierLane: a lane that holds more queued bytes is
// offered less. Nothing drains here, so the split is the picker's alone.
func TestLanesPreferTheEmptierLane(t *testing.T) {
	a := NewQueue(&Client{}, 64, nil)
	b := NewQueue(&Client{}, 64, nil)
	l := NewLanes(a, b)
	ctx := context.Background()
	// Lane a starts 1000 bytes behind.
	if err := a.Publish(ctx, make([]byte, 1000), nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := l.Publish(ctx, make([]byte, 100), nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	// Ten 100-byte records all fit under a's head start, so b took them all.
	if got := b.Stats().Depth; got != 10 {
		t.Fatalf("the emptier lane holds %d of 10 records", got)
	}
	// Level now (1000 bytes each): the next two go one to each.
	for i := 0; i < 2; i++ {
		if err := l.Publish(ctx, make([]byte, 100), nil); err != nil {
			t.Fatal(err)
		}
	}
	if a.Stats().Depth != 2 || b.Stats().Depth != 11 {
		t.Fatalf("level lanes: a holds %d, b holds %d, want 2 and 11", a.Stats().Depth, b.Stats().Depth)
	}
}

// TestLanesPassOverAFailedLane: a lane that has just failed a record gets no
// more while another is healthy, and is offered again after the hold; with
// every lane failed they are all offered.
func TestLanesPassOverAFailedLane(t *testing.T) {
	a := NewQueue(&Client{}, 64, nil)
	b := NewQueue(&Client{}, 64, nil)
	l := NewLanes(a, b)
	now := time.Now()
	l.now = func() time.Time { return now }
	ctx := context.Background()

	a.failedAt.Store(now.UnixNano())
	for i := 0; i < 6; i++ {
		if err := l.Publish(ctx, []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if a.Stats().Depth != 0 || b.Stats().Depth != 6 {
		t.Fatalf("failed lane a holds %d, healthy lane b holds %d, want 0 and 6", a.Stats().Depth, b.Stats().Depth)
	}

	// Both failed: the emptier one is offered rather than nothing.
	b.failedAt.Store(now.UnixNano())
	if err := l.Publish(ctx, []byte{1}, nil); err != nil {
		t.Fatal(err)
	}
	if a.Stats().Depth != 1 {
		t.Fatalf("with every lane failed the emptier lane holds %d, want 1", a.Stats().Depth)
	}

	// Past the hold, a is healthy again and, far emptier, takes what comes.
	now = now.Add(DefaultLaneHold + time.Millisecond)
	b.failedAt.Store(now.UnixNano())
	for i := 0; i < 3; i++ {
		if err := l.Publish(ctx, []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if a.Stats().Depth != 4 {
		t.Fatalf("after the hold lane a holds %d, want 4", a.Stats().Depth)
	}
}

// TestLanesDeadLaneShedsToTheOther runs the real drain with one lane's
// ingress gone: its first record fails, and the rest of the stream goes out
// on the lane that works.
func TestLanesDeadLaneShedsToTheOther(t *testing.T) {
	live := newSink(t)
	defer live.close()
	dead := newSink(t)
	deadAddr := dead.addr()
	dead.close() // nothing listens there now
	qLive := NewQueue(&Client{Addrs: []string{live.addr()}}, 64, nil)
	qDead := NewQueue(&Client{Addrs: []string{deadAddr}}, 64, nil)
	l := NewLanes(qDead, qLive)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Run(ctx) }()

	const n = 200
	for i := 0; i < n; i++ {
		if err := l.Publish(ctx, []byte{0xBE, 0xEF, byte(i)}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		time.Sleep(200 * time.Microsecond) // let a failure be seen before the next record
	}
	waitFor(t, "the stream to settle", func() bool {
		s := l.Stats()
		return s.Sent+s.Failed == n
	})
	s := l.Stats()
	if s.Failed > n/10 {
		t.Fatalf("%d of %d records failed on the dead lane: it should be passed over after its first failure", s.Failed, n)
	}
	if got := qLive.Stats().Sent; got < n*9/10 {
		t.Fatalf("the live lane sent %d of %d", got, n)
	}
}
