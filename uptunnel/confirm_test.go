package uptunnel

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ingress is a test ingress whose behavior per connection is scripted: it can
// read everything, stop after reading, or vanish holding bytes it never read.
type ingress struct {
	ln    net.Listener
	mu    sync.Mutex
	got   bytes.Buffer // everything the ingress process actually read
	conns atomic.Int32
	// handle runs per accepted connection; nil reads to EOF.
	handle func(c *net.TCPConn, in *ingress)
}

func newIngress(t *testing.T, handle func(c *net.TCPConn, in *ingress)) *ingress {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	in := &ingress{ln: ln, handle: handle}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			in.conns.Add(1)
			go func(c *net.TCPConn) {
				if in.handle != nil {
					in.handle(c, in)
					return
				}
				defer func() { _ = c.Close() }()
				in.readAll(c)
			}(c.(*net.TCPConn))
		}
	}()
	return in
}

func (in *ingress) addr() string { return in.ln.Addr().String() }

func (in *ingress) readAll(r io.Reader) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			in.mu.Lock()
			in.got.Write(buf[:n])
			in.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (in *ingress) received() []byte {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]byte(nil), in.got.Bytes()...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRecordsOnARestartingIngressAreResent is item 81: an ingress process that
// stops while holding records it accepted at the TCP level but never read. The
// old client counted every such record sent, and they were gone. Now the
// connection's reset reaches the client inside the grace window, nothing is
// counted, and every record is resent to side B and confirmed there.
func TestRecordsOnARestartingIngressAreResent(t *testing.T) {
	// Side A accepts and lets its kernel acknowledge, but the process never
	// reads; it then dies holding the bytes (close with unread data = RST).
	stopA := make(chan struct{})
	a := newIngress(t, func(c *net.TCPConn, _ *ingress) {
		<-stopA
		_ = c.SetLinger(0)
		_ = c.Close()
	})
	b := newIngress(t, nil)

	c := &Client{Addrs: []string{a.addr(), b.addr()}, ConfirmGrace: 300 * time.Millisecond}
	defer func() { _ = c.Close() }()

	var confirmed, lost atomic.Int32
	done := func(ok bool) {
		if ok {
			confirmed.Add(1)
		} else {
			lost.Add(1)
		}
	}
	recs := [][]byte{{0xBE, 0xEF, 1}, {0xBE, 0xEF, 2}, {0xBE, 0xEF, 3}}
	for _, r := range recs {
		if err := c.Send(context.Background(), r, done); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if st := c.Stats(); st.Written != 3 || st.Confirmed != 0 {
		t.Fatalf("after writes: %+v, want 3 written and none confirmed", st)
	}
	close(stopA)

	waitFor(t, "records confirmed on side B", func() bool {
		c.Poll(context.Background())
		return confirmed.Load() == 3
	})
	if !bytes.Equal(b.received(), bytes.Join(recs, nil)) {
		t.Fatalf("side B read %x, want every record once, in order", b.received())
	}
	st := c.Stats()
	if st.Resent != 3 || st.Lost != 0 || st.PeerClosed != 1 || lost.Load() != 0 {
		t.Fatalf("stats = %+v lost callbacks=%d, want 3 resent, 0 lost, 1 peer close", st, lost.Load())
	}
}

// TestPeerCloseIsSeenBeforeTheNextWrite is the borg timeline: the ingress
// stopped well before the next publish. The client must notice the close on
// its own and put the next record on a live connection, not into a dead one.
func TestPeerCloseIsSeenBeforeTheNextWrite(t *testing.T) {
	// Side A reads the first record and then the process exits cleanly.
	a := newIngress(t, func(c *net.TCPConn, in *ingress) {
		buf := make([]byte, 3)
		if _, err := io.ReadFull(c, buf); err == nil {
			in.mu.Lock()
			in.got.Write(buf)
			in.mu.Unlock()
		}
		_ = c.Close()
	})
	b := newIngress(t, nil)
	c := &Client{Addrs: []string{a.addr(), b.addr()}, ConfirmGrace: 20 * time.Millisecond}
	defer func() { _ = c.Close() }()

	var confirmed atomic.Int32
	done := func(ok bool) {
		if ok {
			confirmed.Add(1)
		}
	}
	if err := c.Send(context.Background(), []byte{0xBE, 0xEF, 1}, done); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "side A to close", func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.cur != nil && c.cur.dead.Load() })

	// Side A is now down for good (the host is rebooting).
	_ = a.ln.Close()
	if err := c.Send(context.Background(), []byte{0xBE, 0xEF, 2}, done); err != nil {
		t.Fatalf("second send: %v", err)
	}
	waitFor(t, "both records confirmed", func() bool {
		c.Poll(context.Background())
		return confirmed.Load() == 2
	})
	if got := b.received(); !bytes.Equal(got, []byte{0xBE, 0xEF, 2}) {
		t.Fatalf("side B read %x, want only the second record", got)
	}
	if st := c.Stats(); st.Resent != 0 || st.PeerClosed != 1 {
		t.Fatalf("stats = %+v, want no resend (the first record was read before a clean close)", st)
	}
}

// TestQueueCountsSentOnlyOnConfirmation pins the counter the open question
// asked for: publish_total{result="sent"} moves only when the ingress has
// confirmed, and a record that cannot be confirmed anywhere ends as failed.
func TestQueueCountsSentOnlyOnConfirmation(t *testing.T) {
	in := newIngress(t, nil)
	c := &Client{Addrs: []string{in.addr()}, ConfirmGrace: 200 * time.Millisecond}
	q := NewQueue(c, 8, nil)
	q.PollInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Run(ctx) }()

	ran := make(chan struct{}, 1)
	if err := q.Publish(ctx, []byte{0xBE, 0xEF, 9}, func() { ran <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "record written", func() bool { return c.Stats().Written == 1 })
	if q.Stats().Sent != 0 {
		t.Fatal("sent counted before the grace window passed")
	}
	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("sent callback never ran")
	}
	if st := q.Stats(); st.Sent != 1 || st.Failed != 0 || st.Unconfirmed != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestResendsAreBounded: with every address gone, a written record that dies
// with its connection is resent at most MaxResends times and then reported
// lost, never left waiting or counted sent.
func TestResendsAreBounded(t *testing.T) {
	stop := make(chan struct{})
	a := newIngress(t, func(c *net.TCPConn, _ *ingress) {
		<-stop
		_ = c.SetLinger(0)
		_ = c.Close()
	})
	c := &Client{Addrs: []string{a.addr()}, ConfirmGrace: time.Minute, MaxResends: 2}
	defer func() { _ = c.Close() }()
	var lost atomic.Int32
	if err := c.Send(context.Background(), []byte{0xBE, 0xEF, 7}, func(ok bool) {
		if !ok {
			lost.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_ = a.ln.Close()
	close(stop)
	waitFor(t, "record reported lost", func() bool {
		c.Poll(context.Background())
		return lost.Load() == 1
	})
	if st := c.Stats(); st.Lost != 1 || st.Confirmed != 0 {
		t.Fatalf("stats = %+v", st)
	}
}
