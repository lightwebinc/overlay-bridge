package uptunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// sink is a TCP listener that records every byte it receives per connection.
type sink struct {
	ln    net.Listener
	mu    sync.Mutex
	conns int
	got   [][]byte
}

func newSink(t *testing.T) *sink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &sink{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns++
			s.mu.Unlock()
			go func(c net.Conn) {
				defer c.Close()
				b, _ := io.ReadAll(c)
				if len(b) > 0 {
					s.mu.Lock()
					s.got = append(s.got, b)
					s.mu.Unlock()
				}
			}(c)
		}
	}()
	return s
}

func (s *sink) addr() string { return s.ln.Addr().String() }
func (s *sink) close()       { _ = s.ln.Close() }
func (s *sink) dials() int   { s.mu.Lock(); defer s.mu.Unlock(); return s.conns }

// waitDials waits for the sink to have accepted n connections. Submit returns
// once the client's Write succeeds, which can happen before the server's
// Accept goroutine has run, so reading the count immediately is a race that
// passes or fails on scheduling.
func (s *sink) waitDials(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.dials() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("connections = %d, want %d", s.dials(), n)
}

func TestSubmitWritesWholeRecord(t *testing.T) {
	s := newSink(t)
	defer s.close()
	c := &Client{Addrs: []string{s.addr()}}
	defer c.Close()

	rec := []byte{0xBE, 0xEF, 0x01, 0x02, 0x03}
	if err := c.Submit(context.Background(), rec); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if st := c.Stats(); st.Sent != 1 || st.Bytes != uint64(len(rec)) || st.Redials != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestFailoverAdvancesOnDeadPrimary pins that a dead side A does not strand
// submissions: the client advances to side B and the record lands there.
func TestFailoverAdvancesOnDeadPrimary(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close() // nothing is listening there now

	b := newSink(t)
	defer b.close()

	c := &Client{Addrs: []string{deadAddr, b.addr()}}
	defer c.Close()
	if err := c.Submit(context.Background(), []byte("record")); err != nil {
		t.Fatalf("Submit over failover: %v", err)
	}
	if st := c.Stats(); st.Sent != 1 || st.Failures == 0 {
		t.Fatalf("stats = %+v, want one send and at least one failure", st)
	}
}

// TestFailBackReturnsToPreferred is the change from the sibling bridge's
// forward-only failover. The landing posture is primary/standby: staying on
// the standby after the primary recovers is a silent half-outage.
func TestFailBackReturnsToPreferred(t *testing.T) {
	a := newSink(t)
	defer a.close()
	bs := newSink(t)
	defer bs.close()

	now := time.Now()
	c := &Client{
		Addrs:   []string{a.addr(), bs.addr()},
		Prefer:  time.Minute,
		nowFunc: func() time.Time { return now },
	}
	defer c.Close()

	// Force a move to side B without needing side A to actually fail.
	c.mu.Lock()
	c.advanceLocked()
	c.mu.Unlock()
	if err := c.Submit(context.Background(), []byte("on B")); err != nil {
		t.Fatalf("submit on B: %v", err)
	}
	bs.waitDials(t, 1)

	// Before Prefer elapses it stays put.
	now = now.Add(30 * time.Second)
	if err := c.Submit(context.Background(), []byte("still B")); err != nil {
		t.Fatalf("submit still on B: %v", err)
	}
	if a.dials() != 0 {
		t.Fatalf("failed back too early: side A dials = %d", a.dials())
	}

	// Once it has, the next submission goes back to side A.
	now = now.Add(31 * time.Second)
	if err := c.Submit(context.Background(), []byte("back on A")); err != nil {
		t.Fatalf("submit after fail-back: %v", err)
	}
	a.waitDials(t, 1)
}

// TestSubmitReturnsErrorWhenAllAddressesDead pins that a total outage is
// reported rather than swallowed: the facade needs to know it did not publish.
func TestSubmitReturnsErrorWhenAllAddressesDead(t *testing.T) {
	var dead []string
	for i := 0; i < 2; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		dead = append(dead, ln.Addr().String())
		_ = ln.Close()
	}
	c := &Client{Addrs: dead}
	defer c.Close()
	if err := c.Submit(context.Background(), []byte("x")); err == nil {
		t.Fatal("Submit to two dead addresses returned nil")
	}
}

func TestQueueShedsRatherThanGrows(t *testing.T) {
	// A client with no addresses never drains, so the queue fills and sheds.
	q := NewQueue(&Client{}, 2, nil)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := q.Publish(ctx, []byte("x"), nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := q.Publish(ctx, []byte("x"), nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third publish: %v, want ErrQueueFull", err)
	}
	st := q.Stats()
	if st.Enqueued != 2 || st.Shed != 1 || st.Depth != 2 || st.Capacity != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestQueueTakesOwnership pins the contract: the queue holds the caller's
// buffer rather than copying it, so the caller must hand over a buffer it will
// not reuse. The copy this replaced doubled every publish's allocation on the
// client's synchronous request path, for a caller that never aliased.
func TestQueueTakesOwnership(t *testing.T) {
	q := NewQueue(&Client{}, 4, nil)
	rec := []byte{1, 2, 3}
	if err := q.Publish(context.Background(), rec, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got := (<-q.ch).rec
	if &got[0] != &rec[0] {
		t.Fatal("queue copied the record; the contract is ownership transfer")
	}
}

func TestQueueDrainsAndSurvivesAFailure(t *testing.T) {
	s := newSink(t)
	defer s.close()
	q := NewQueue(&Client{Addrs: []string{s.addr()}}, 8, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- q.Run(ctx) }()

	for i := 0; i < 3; i++ {
		if err := q.Publish(ctx, []byte{0xBE, 0xEF, byte(i)}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if q.Stats().Sent == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := q.Stats(); st.Sent != 3 {
		t.Fatalf("stats = %+v, want 3 sent", st)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

// TestEachClientOwnsItsConnection pins the grammar rule structurally.
//
// The fabric ingress decides a connection's grammar from its first four bytes,
// once, for the life of the connection. A BRC-149 record stream (0xBE 0xEF)
// and a bare transaction stream therefore cannot share a socket: the first
// four bytes have already decided how every later byte is read, so
// interleaving silently corrupts one of the two grammars rather than erroring.
//
// The guarantee is that one Client writes one grammar down one connection and
// nothing else writes to it. This asserts the observable half: two Clients
// against the same address open two connections and never share one.
func TestEachClientOwnsItsConnection(t *testing.T) {
	s := newSink(t)
	defer s.close()

	records := &Client{Addrs: []string{s.addr()}}
	defer records.Close()
	other := &Client{Addrs: []string{s.addr()}}
	defer other.Close()

	if err := records.Submit(context.Background(), []byte{0xBE, 0xEF, 0x01}); err != nil {
		t.Fatalf("record submit: %v", err)
	}
	if err := other.Submit(context.Background(), []byte{0x01, 0x00, 0x00, 0x00}); err != nil {
		t.Fatalf("other submit: %v", err)
	}
	s.waitDials(t, 2)
	if records.conn == other.conn {
		t.Fatal("two clients shared one connection")
	}
}

// TestSentCallbackRunsOnlyOnActualSend pins the contract the facade's loop
// guard depends on: acceptance into the queue is not publication.
func TestSentCallbackRunsOnlyOnActualSend(t *testing.T) {
	// Nothing listening: every send fails, so the callback must never run.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	_ = ln.Close()
	q := NewQueue(&Client{Addrs: []string{dead}}, 4, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Run(ctx) }()

	ran := make(chan struct{}, 1)
	if err := q.Publish(ctx, []byte("x"), func() { ran <- struct{}{} }); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && q.Stats().Failed == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-ran:
		t.Fatal("sent callback ran although the send failed")
	default:
	}

	// A live sink: the callback runs once the record is written.
	s := newSink(t)
	defer s.close()
	q2 := NewQueue(&Client{Addrs: []string{s.addr()}}, 4, nil)
	go func() { _ = q2.Run(ctx) }()
	ran2 := make(chan struct{}, 1)
	if err := q2.Publish(ctx, []byte{0xBE, 0xEF}, func() { ran2 <- struct{}{} }); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-ran2:
	case <-time.After(2 * time.Second):
		t.Fatal("sent callback did not run after a successful send")
	}
}
