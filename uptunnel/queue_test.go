package uptunnel

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newBoundedQueue(depth int, maxBytes int64, wait time.Duration) *Queue {
	q := NewQueue(&Client{}, depth, nil)
	q.MaxBytes, q.Wait = maxBytes, wait
	return q
}

// takeOneAfter takes one record off the queue the way Run does, after a
// delay, so a publish waiting for room sees the room arrive.
func takeOneAfter(q *Queue, d time.Duration) {
	go func() {
		time.Sleep(d)
		q.taken(<-q.ch)
	}()
}

// TestQueueByteBoundWaitsThenAdmits: a queue full by bytes holds the
// publisher rather than refusing it, and admits the record once the drain
// has made room within the wait. Overload is then request latency, not a
// 503 the client treats as a failed put.
func TestQueueByteBoundWaitsThenAdmits(t *testing.T) {
	q := newBoundedQueue(8, 10, 2*time.Second)
	ctx := context.Background()
	if err := q.Publish(ctx, make([]byte, 6), nil); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	takeOneAfter(q, 100*time.Millisecond)
	start := time.Now()
	if err := q.Publish(ctx, make([]byte, 6), nil); err != nil {
		t.Fatalf("second publish: %v, want it admitted once the first was taken", err)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("second publish returned after %v; it should have waited for the drain", waited)
	}
	st := q.Stats()
	if st.Enqueued != 2 || st.Shed != 0 || st.Waited != 1 || st.Bytes != 6 || st.BytesCapacity != 10 || st.Depth != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestQueueByteBoundShedsAfterTheWait: no room within the wait is the shed it
// always was, with the record's bytes released and nothing else disturbed.
func TestQueueByteBoundShedsAfterTheWait(t *testing.T) {
	q := newBoundedQueue(8, 10, 50*time.Millisecond)
	ctx := context.Background()
	if err := q.Publish(ctx, make([]byte, 6), nil); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	start := time.Now()
	if err := q.Publish(ctx, make([]byte, 6), nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second publish: %v, want ErrQueueFull", err)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("shed after %v, before the wait had passed", waited)
	}
	if st := q.Stats(); st.Shed != 1 || st.Waited != 1 || st.Enqueued != 1 || st.Bytes != 6 || st.Depth != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestQueueCountBoundStillApplies: the byte bound is in addition to the
// count, and the wait covers both.
func TestQueueCountBoundStillApplies(t *testing.T) {
	q := newBoundedQueue(2, 1<<20, 50*time.Millisecond)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := q.Publish(ctx, []byte("x"), nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := q.Publish(ctx, []byte("x"), nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third publish: %v, want ErrQueueFull", err)
	}
	if st := q.Stats(); st.Depth != 2 || st.Bytes != 2 || st.Shed != 1 || st.Waited != 1 {
		t.Fatalf("stats = %+v", st)
	}
	// Room by count, made by the drain, admits the next.
	takeOneAfter(q, 20*time.Millisecond)
	if err := q.Publish(ctx, []byte("x"), nil); err != nil {
		t.Fatalf("publish after a take: %v", err)
	}
	if st := q.Stats(); st.Depth != 2 || st.Bytes != 2 || st.Enqueued != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestQueueWaitHonorsTheContext: a client that gives up is not held for the
// whole wait, and its record is not counted as shed by the queue.
func TestQueueWaitHonorsTheContext(t *testing.T) {
	q := newBoundedQueue(1, 0, 10*time.Second)
	if err := q.Publish(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := q.Publish(ctx, []byte("y"), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context's", err)
	}
	if held := time.Since(start); held > 5*time.Second {
		t.Fatalf("held %v past the context", held)
	}
	if st := q.Stats(); st.Shed != 0 || st.Waited != 1 || st.Depth != 1 || st.Bytes != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestQueueCarriesAnOversizeRecordAlone: a record larger than the byte bound
// is never refused for its size. It is admitted when the queue is empty, and
// what comes behind it waits for it to be taken.
func TestQueueCarriesAnOversizeRecordAlone(t *testing.T) {
	q := newBoundedQueue(8, 10, 0)
	ctx := context.Background()
	if err := q.Publish(ctx, make([]byte, 25), nil); err != nil {
		t.Fatalf("oversize record on an empty queue: %v", err)
	}
	if err := q.Publish(ctx, []byte("x"), nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("record behind the oversize one: %v, want ErrQueueFull with no wait configured", err)
	}
	q.taken(<-q.ch)
	if err := q.Publish(ctx, []byte("x"), nil); err != nil {
		t.Fatalf("after the take: %v", err)
	}
	if st := q.Stats(); st.Bytes != 1 || st.Depth != 1 || st.Shed != 1 || st.Waited != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestRunReleasesBytesAsItDrains runs the real drain under the byte bound:
// each record is taken, its bytes released and the next publish admitted,
// until every one is sent and nothing is left booked against the bound.
func TestRunReleasesBytesAsItDrains(t *testing.T) {
	s := newSink(t)
	defer s.close()
	q := NewQueue(&Client{Addrs: []string{s.addr()}}, 8, nil)
	// Three bytes a record under a bound of four: no two fit together, so
	// every record after the first needs the drain to take the one before.
	q.MaxBytes, q.Wait = 4, 2*time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Run(ctx) }()
	for i := 0; i < 3; i++ {
		if err := q.Publish(ctx, []byte{0xBE, 0xEF, byte(i)}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, "every record sent", func() bool { return q.Stats().Sent == 3 })
	if st := q.Stats(); st.Enqueued != 3 || st.Shed != 0 || st.Bytes != 0 || st.Depth != 0 {
		t.Fatalf("stats = %+v, want three enqueued and nothing booked", st)
	}
}
