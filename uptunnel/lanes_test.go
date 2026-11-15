package uptunnel

import (
	"context"
	"errors"
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

// TestLanesFullLaneHandsToAnother: fewest bytes queued is not room. A lane
// full by count, of small records, is dealt the next record because it holds
// the fewest bytes; the record goes to a lane that has room instead of
// waiting there or being shed.
func TestLanesFullLaneHandsToAnother(t *testing.T) {
	a := NewQueue(&Client{}, 4, nil)
	b := NewQueue(&Client{}, 4, nil)
	a.Wait, b.Wait = 5*time.Second, 5*time.Second
	l := NewLanes(a, b)
	ctx := context.Background()
	for i := 0; i < 4; i++ { // lane a: full by count, four bytes queued
		if err := a.Publish(ctx, []byte{byte(i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Publish(ctx, make([]byte, 4096), nil); err != nil { // lane b: one large record, three slots free
		t.Fatal(err)
	}
	start := time.Now()
	if err := l.Publish(ctx, []byte{0xBE, 0xEF}, nil); err != nil {
		t.Fatalf("publish with room on the other lane: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the publisher waited %v on the full lane", took)
	}
	if a.Stats().Depth != 4 || b.Stats().Depth != 2 {
		t.Fatalf("a holds %d, b holds %d, want 4 and 2", a.Stats().Depth, b.Stats().Depth)
	}
	if st := l.Stats(); st.Shed != 0 || st.Waited != 0 {
		t.Fatalf("stats = %+v, want nothing shed and no wait", st)
	}
}

// TestLanesAllFullWaitsThenSheds: with no room on any lane the publisher
// waits the dealt lane's Wait and the record is shed, once, on that lane.
func TestLanesAllFullWaitsThenSheds(t *testing.T) {
	a := NewQueue(&Client{}, 1, nil)
	b := NewQueue(&Client{}, 1, nil)
	a.Wait, b.Wait = 40*time.Millisecond, 40*time.Millisecond
	l := NewLanes(a, b)
	ctx := context.Background()
	for _, q := range []*Queue{a, b} {
		if err := q.Publish(ctx, []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	err := l.Publish(ctx, []byte{2}, nil)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("publish with every lane full: %v, want ErrQueueFull", err)
	}
	if took := time.Since(start); took < 30*time.Millisecond {
		t.Fatalf("refused after %v, before the wait", took)
	}
	if st := l.Stats(); st.Shed != 1 || st.Waited != 1 {
		t.Fatalf("stats = %+v, want one wait and one shed", st)
	}
}

// TestLanesWaiterTakesRoomOnAnotherLane: a publisher waiting on its dealt
// lane takes room that opens on another.
func TestLanesWaiterTakesRoomOnAnotherLane(t *testing.T) {
	s := newSink(t)
	t.Cleanup(s.close)
	a := NewQueue(&Client{}, 1, nil)                          // never drains
	b := NewQueue(&Client{Addrs: []string{s.addr()}}, 1, nil) // drains once Run starts
	a.Wait, b.Wait = 5*time.Second, 5*time.Second
	l := NewLanes(a, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := a.Publish(ctx, []byte{0xBE, 0xEF, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, make([]byte, 64), nil); err != nil { // more bytes than a: a is dealt the next record
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- l.Publish(ctx, []byte{0xBE, 0xEF, 2}, nil) }()
	select {
	case err := <-done:
		t.Fatalf("published with every lane full: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	go func() { _ = b.Run(ctx) }() // lane b starts to drain
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("publish after room opened on the other lane: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the publisher kept waiting on its own lane")
	}
}

// TestLanesWaiterTakesALaneBackInService: which lanes are in service is read
// again at every attempt. A publisher waiting on a full lane takes a lane
// whose hold ends during its wait.
func TestLanesWaiterTakesALaneBackInService(t *testing.T) {
	a := NewQueue(&Client{}, 1, nil)
	b := NewQueue(&Client{}, 4, nil)
	a.Wait, b.Wait = 5*time.Second, 5*time.Second
	l := NewLanes(a, b)
	l.Hold = 60 * time.Millisecond
	ctx := context.Background()
	if err := a.Publish(ctx, []byte{1}, nil); err != nil { // lane a: full
		t.Fatal(err)
	}
	b.fail() // lane b: empty, and out of service for the hold
	start := time.Now()
	if err := l.Publish(ctx, []byte{2}, nil); err != nil {
		t.Fatalf("publish across the end of the other lane's hold: %v", err)
	}
	if took := time.Since(start); took < 40*time.Millisecond || took > 2*time.Second {
		t.Fatalf("published after %v, want about the hold (60ms)", took)
	}
	if a.Stats().Depth != 1 || b.Stats().Depth != 1 {
		t.Fatalf("a holds %d, b holds %d, want 1 and 1", a.Stats().Depth, b.Stats().Depth)
	}
}
