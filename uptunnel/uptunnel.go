// Package uptunnel streams BRC-149 submission records to the fabric's BEEF
// ingress over a long-lived TCP connection through the consumer tunnel.
//
// # The stream owns its connection
//
// The fabric ingress decides a TCP connection's grammar ONCE, from its first
// four bytes: 0xBE 0xEF means a BRC-149 record stream, 0xE3E1F3E8 means framed
// objects, and anything else is read as bare transactions. A record stream
// therefore needs its own socket and may never be interleaved with extended
// format transactions, because the first four bytes have already decided how
// every later byte is read. This package enforces that structurally: one
// [Client] carries one grammar, and nothing else writes to its connection.
//
// The stream is self-delimiting, so a partial write desynchronises the reader
// and cannot be recovered by retrying the remainder. Every write goes out in
// one call and any write error closes the connection; the next submission
// redials.
//
// # Failover and fail-back
//
// Addrs is the slot's inner addresses in failover order, side A then side B,
// never hot-hot. A failed submission advances to the next address and retries
// once, so a side flip does not strand submissions on the dead side. Unlike a
// forward-only failover, this client returns to the preferred address after
// [Client.Prefer] has passed, because the landing posture is primary/standby
// with fail-back rather than sticky failover.
//
// # Source address
//
// LocalAddr pins the source to the slot inner the bridge publishes from. This
// is not cosmetic. The fabric excludes a consumer's own traffic from its
// delivery by deriving a key from the source address it observes, so a
// submission that leaves from the wrong source is delivered back to us: a full
// recursive SPV verify per object inside the engine before its duplicate check
// discards it, and billable egress on the way in. Left unpinned, the source is
// whatever the host's address selection picks on a machine that also carries a
// fleet interface and an endpoint plane.
//
// # A written record is not a sent record
//
// The record stream is one-way: the ingress never answers. A successful Write
// only means the local kernel took the bytes, and a write to an ingress that
// has just stopped succeeds locally while the bytes are discarded. So the
// client does three things a fire-and-forget writer does not:
//
//   - It reads its own connection. The ingress never writes, so any return
//     from Read is the peer closing (EOF) or failing (reset, timeout), and the
//     next submission redials instead of writing into a dead socket. TCP
//     keepalive and TCP_USER_TIMEOUT bound how long a silent peer (a host that
//     vanished without a FIN) can hold unacknowledged bytes.
//   - It counts a record confirmed only once the ingress host's TCP has
//     acknowledged its last byte (on Linux, from the socket's unacknowledged
//     send queue) and the connection has then stayed up for [Client.ConfirmGrace].
//     A peer that closes holding bytes it never read resets the connection,
//     and the grace window is what lets that reset reach the client before
//     the record is counted.
//   - When a connection dies, every record on it that is not confirmed is
//     resent on the next connection, at most [Client.MaxResends] times, and is
//     reported lost after that. After a clean close (FIN) only the bytes the
//     peer never acknowledged are resent, because a peer that closes cleanly
//     has read everything it acknowledged.
//
// Resending is safe because the plane is idempotent per record: the ingress
// claims SHA-256(ContentID ‖ TopicID) in its object-plane dedup store before
// emitting (fleet-wide when the shared dedup tier is configured), listeners
// claim the same pair at egress, and the engine answers a duplicate with an
// empty admittance. Resending only what is unconfirmed keeps duplicates to the
// rare record the peer read but whose acknowledgement was lost with the
// connection.
//
// What this cannot see: a record the ingress process read and then lost before
// emitting it (a crash between read and send). Only an acknowledgement in the
// record stream itself could close that.
//
// # The endpoint-plane route is not this package's business
//
// The route to the ingress belongs to the endpoint-plane failover keeper. This
// client never installs, checks or repairs one, and must not refuse to start
// because the route is momentarily absent: that is the keeper's failover
// window, which close-and-advance already covers.
package uptunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultConfirmGrace is how long a record must sit acknowledged by the
	// ingress host, on a connection that stays up, before it counts as sent.
	DefaultConfirmGrace = time.Second
	// DefaultMaxResends bounds how many times one record is resent after the
	// connection it was written on died.
	DefaultMaxResends = 3
	// DefaultUserTimeout bounds how long written bytes may stay unacknowledged
	// before the kernel fails the connection (TCP_USER_TIMEOUT), and sets the
	// keepalive budget for an idle connection to a vanished peer.
	DefaultUserTimeout = 15 * time.Second

	// maxUnconfirmed bounds the records held for confirmation. Past it, records
	// the peer has acknowledged are confirmed without waiting out the grace.
	maxUnconfirmed = 1 << 16
)

// Client is safe for concurrent use; writes serialize on one connection.
type Client struct {
	// Addrs are host:port slot inners in failover order, side A then side B.
	Addrs []string
	// LocalAddr pins the source address of every dial. See the package docs:
	// the wrong source costs a verify and billable egress per object.
	LocalAddr *net.TCPAddr
	// Prefer returns to Addrs[0] after this long away from it. Zero is sticky
	// failover, which is not the landing posture.
	Prefer time.Duration
	// Timeout is the per-write ceiling when the context carries no deadline.
	Timeout time.Duration
	// ConfirmGrace is how long an acknowledged record waits on a live
	// connection before it counts as sent (0 = DefaultConfirmGrace).
	ConfirmGrace time.Duration
	// MaxResends bounds resends per record (0 = DefaultMaxResends).
	MaxResends int
	// UserTimeout is TCP_USER_TIMEOUT and the keepalive budget
	// (0 = DefaultUserTimeout).
	UserTimeout time.Duration
	Log         *slog.Logger

	mu      sync.Mutex
	conn    net.Conn
	cur     *stream
	idx     int
	leftAt  time.Time // when we first moved off Addrs[0]
	nowFunc func() time.Time

	// unconfirmed are records written on cur, oldest first; resend are records
	// from a dead connection waiting for the next one.
	unconfirmed []*pending
	resend      []*pending

	written, bytes, failures, redials   atomic.Uint64
	confirmed, resent, lost, peerClosed atomic.Uint64
	unconfirmedGauge                    atomic.Int64
}

// pending is one written record awaiting confirmation.
type pending struct {
	b       []byte
	end     uint64    // cumulative byte offset on its connection after this record
	at      time.Time // when it was written
	resends int
	done    func(confirmed bool)
}

// stream is one connection and what its reader has learned about the peer.
type stream struct {
	conn    net.Conn
	written uint64 // bytes written
	acked   uint64 // bytes the peer has acknowledged, as last read
	dead    atomic.Bool
	clean   atomic.Bool // the peer closed with EOF, not a reset or timeout
}

// Stats is a point-in-time snapshot.
//
// Written counts records the local kernel accepted, which is not proof that
// they reached the ingress; Confirmed is. Resent counts records written again
// after their connection died, Lost the records given up on after
// [Client.MaxResends], PeerClosed the connections the ingress closed or
// failed, and Unconfirmed the records written and not yet confirmed or
// resent.
type Stats struct {
	Written, Bytes, Failures, Redials   uint64
	Confirmed, Resent, Lost, PeerClosed uint64
	Unconfirmed                         uint64
}

// Stats returns a counter snapshot.
func (c *Client) Stats() Stats {
	u := c.unconfirmedGauge.Load()
	if u < 0 {
		u = 0
	}
	return Stats{
		Written: c.written.Load(), Bytes: c.bytes.Load(), Failures: c.failures.Load(), Redials: c.redials.Load(),
		Confirmed: c.confirmed.Load(), Resent: c.resent.Load(), Lost: c.lost.Load(), PeerClosed: c.peerClosed.Load(),
		Unconfirmed: uint64(u),
	}
}

func (c *Client) now() time.Time {
	if c.nowFunc != nil {
		return c.nowFunc()
	}
	return time.Now()
}

func (c *Client) grace() time.Duration {
	if c.ConfirmGrace > 0 {
		return c.ConfirmGrace
	}
	return DefaultConfirmGrace
}

func (c *Client) maxResends() int {
	if c.MaxResends > 0 {
		return c.MaxResends
	}
	return DefaultMaxResends
}

func (c *Client) userTimeout() time.Duration {
	if c.UserTimeout > 0 {
		return c.UserTimeout
	}
	return DefaultUserTimeout
}

// Submit writes one record and does not track its confirmation for the
// caller. It is [Client.Send] with no callback.
func (c *Client) Submit(ctx context.Context, b []byte) error {
	return c.Send(ctx, b, nil)
}

// Send writes one submission record's bytes in a single call. On a write
// error it fails over to the next address and retries once; the second failure
// is returned and the record was not written.
//
// A nil return means written, not delivered. done, if non-nil, is called
// exactly once, off the caller's lock: done(true) when the record is
// confirmed, done(false) when it is given up on after its resends. Callers
// must call [Client.Poll] periodically (the [Queue] does) so confirmations and
// resends happen while no new record is being sent.
func (c *Client) Send(ctx context.Context, b []byte, done func(confirmed bool)) error {
	var cbs []func()
	c.mu.Lock()
	c.reapLocked(&cbs)
	c.pollLocked(&cbs)
	c.maybeFailBackLocked()
	err := c.sendLocked(ctx, &pending{b: b, done: done}, &cbs)
	c.pollLocked(&cbs)
	c.mu.Unlock()
	for _, f := range cbs {
		f()
	}
	if err != nil {
		return fmt.Errorf("uptunnel: %w", err)
	}
	return nil
}

// Poll confirms acknowledged records, notices a connection the peer has
// closed, and resends what that connection left unconfirmed.
func (c *Client) Poll(ctx context.Context) {
	var cbs []func()
	c.mu.Lock()
	c.reapLocked(&cbs)
	c.pollLocked(&cbs)
	if len(c.resend) > 0 {
		if err := c.sendLocked(ctx, nil, &cbs); err != nil {
			// Neither address took the resends. Each failed flush spends one
			// resend, so a record cannot wait on a dead plane for ever.
			c.chargeResendsLocked(&cbs)
		}
	}
	c.mu.Unlock()
	for _, f := range cbs {
		f()
	}
}

// sendLocked flushes pending resends and then writes rec (nil = flush only),
// with one failover retry.
func (c *Client) sendLocked(ctx context.Context, rec *pending, cbs *[]func()) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.ensureConnLocked(ctx); err != nil {
			lastErr = err
			c.failures.Add(1)
			c.advanceLocked()
			continue
		}
		if err := c.flushResendLocked(ctx, cbs); err != nil {
			lastErr = err
			continue
		}
		if rec == nil {
			return nil
		}
		if err := c.writeLocked(ctx, rec); err != nil {
			c.breakLocked(cbs)
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// flushResendLocked writes every record waiting from a dead connection.
func (c *Client) flushResendLocked(ctx context.Context, cbs *[]func()) error {
	for len(c.resend) > 0 {
		r := c.resend[0]
		if err := c.writeLocked(ctx, r); err != nil {
			c.breakLocked(cbs)
			return err
		}
		c.resend = c.resend[1:]
		c.resent.Add(1)
	}
	return nil
}

// writeLocked writes one record on the current connection.
//
// A partial write poisons a self-delimiting stream: the reader cannot
// resynchronise, so on any error the caller drops the connection rather than
// retry the remainder.
func (c *Client) writeLocked(ctx context.Context, r *pending) error {
	dl, ok := ctx.Deadline()
	if !ok {
		t := c.Timeout
		if t == 0 {
			t = 10 * time.Second
		}
		dl = c.now().Add(t)
	}
	s := c.cur
	_ = s.conn.SetWriteDeadline(dl)
	if _, err := s.conn.Write(r.b); err != nil {
		return err
	}
	s.written += uint64(len(r.b))
	r.end = s.written
	r.at = c.now()
	c.unconfirmed = append(c.unconfirmed, r)
	c.unconfirmedGauge.Add(1)
	c.written.Add(1)
	c.bytes.Add(uint64(len(r.b)))
	return nil
}

// breakLocked drops a connection a write failed on.
func (c *Client) breakLocked(cbs *[]func()) {
	c.retireLocked(false, cbs)
	c.failures.Add(1)
	c.advanceLocked()
}

// reapLocked retires the connection if its reader saw the peer close it.
func (c *Client) reapLocked(cbs *[]func()) {
	if c.cur == nil || !c.cur.dead.Load() {
		return
	}
	c.peerClosed.Add(1)
	clean := c.cur.clean.Load()
	if c.Log != nil {
		c.Log.Warn("up-tunnel peer closed the connection", "addr", c.cur.conn.RemoteAddr().String(),
			"clean", clean, "unconfirmed", len(c.unconfirmed))
	}
	c.retireLocked(clean, cbs)
	if !clean {
		c.advanceLocked()
	}
}

// retireLocked closes the current connection and settles its unconfirmed
// records: after a clean close, what the peer acknowledged is confirmed;
// everything else is queued for resend, or lost past its budget.
func (c *Client) retireLocked(clean bool, cbs *[]func()) {
	s := c.cur
	if s == nil {
		return
	}
	known := false
	if clean {
		if q, ok := unsentBytes(s.conn); ok && uint64(q) <= s.written {
			s.acked = s.written - uint64(q)
			known = true
		}
	}
	_ = s.conn.Close()
	c.cur, c.conn = nil, nil
	for _, r := range c.unconfirmed {
		c.unconfirmedGauge.Add(-1)
		if known && r.end <= s.acked {
			c.confirmLocked(r, cbs)
			continue
		}
		c.requeueLocked(r, cbs)
	}
	c.unconfirmed = nil
}

// requeueLocked spends one resend on r, or gives it up.
func (c *Client) requeueLocked(r *pending, cbs *[]func()) {
	r.resends++
	if r.resends > c.maxResends() {
		c.loseLocked(r, cbs)
		return
	}
	c.resend = append(c.resend, r)
}

// chargeResendsLocked spends one resend on every waiting record after a flush
// found no address to take them.
func (c *Client) chargeResendsLocked(cbs *[]func()) {
	waiting := c.resend
	c.resend = nil
	for _, r := range waiting {
		c.requeueLocked(r, cbs)
	}
}

func (c *Client) confirmLocked(r *pending, cbs *[]func()) {
	c.confirmed.Add(1)
	if r.done != nil {
		d := r.done
		*cbs = append(*cbs, func() { d(true) })
	}
}

func (c *Client) loseLocked(r *pending, cbs *[]func()) {
	c.lost.Add(1)
	if c.Log != nil {
		c.Log.Error("up-tunnel record not confirmed after resends; it is not on the plane",
			"bytes", len(r.b), "resends", r.resends-1)
	}
	if r.done != nil {
		d := r.done
		*cbs = append(*cbs, func() { d(false) })
	}
}

// pollLocked confirms the records the peer has acknowledged and that have
// outlived the grace window on a live connection.
func (c *Client) pollLocked(cbs *[]func()) {
	s := c.cur
	if s == nil || len(c.unconfirmed) == 0 {
		return
	}
	if q, ok := unsentBytes(s.conn); ok && uint64(q) <= s.written {
		s.acked = s.written - uint64(q)
	} else if !ok {
		// No send-queue reading on this platform: the grace window and the
		// reader's view of the peer are all there is.
		s.acked = s.written
	}
	now := c.now()
	grace := c.grace()
	over := len(c.unconfirmed) > maxUnconfirmed
	n := 0
	for _, r := range c.unconfirmed {
		if r.end > s.acked || (!over && now.Sub(r.at) < grace) {
			break
		}
		c.confirmLocked(r, cbs)
		n++
	}
	if n > 0 {
		c.unconfirmed = c.unconfirmed[n:]
		c.unconfirmedGauge.Add(-int64(n))
	}
}

// Close releases the connection. Records the peer has acknowledged are
// confirmed; anything else is reported lost rather than claimed.
func (c *Client) Close() error {
	var cbs []func()
	c.mu.Lock()
	var err error
	if s := c.cur; s != nil {
		if q, ok := unsentBytes(s.conn); ok && uint64(q) <= s.written {
			s.acked = s.written - uint64(q)
		}
		for _, r := range c.unconfirmed {
			c.unconfirmedGauge.Add(-1)
			if r.end <= s.acked {
				c.confirmLocked(r, &cbs)
			} else {
				c.loseLocked(r, &cbs)
			}
		}
		c.unconfirmed = nil
		err = s.conn.Close()
		c.cur, c.conn = nil, nil
	}
	for _, r := range c.resend {
		c.loseLocked(r, &cbs)
	}
	c.resend = nil
	c.mu.Unlock()
	for _, f := range cbs {
		f()
	}
	return err
}

func (c *Client) ensureConnLocked(ctx context.Context) error {
	if c.cur != nil {
		return nil
	}
	if len(c.Addrs) == 0 {
		return fmt.Errorf("no ingress addresses")
	}
	ut := c.userTimeout()
	d := net.Dialer{
		Timeout:   5 * time.Second,
		LocalAddr: c.localAddr(),
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     ut / 3,
			Interval: ut / 6,
			Count:    2,
		},
		Control: dialControl(ut),
	}
	conn, err := d.DialContext(ctx, "tcp", c.Addrs[c.idx%len(c.Addrs)])
	if err != nil {
		return err
	}
	s := &stream{conn: conn}
	c.cur, c.conn = s, conn
	c.redials.Add(1)
	go watch(s)
	if c.Log != nil {
		c.Log.Info("up-tunnel connected", "addr", conn.RemoteAddr().String(), "source", conn.LocalAddr().String())
	}
	return nil
}

// watch reads the connection until it ends. The ingress never writes on a
// record stream, so a return from Read is the peer closing it (EOF) or the
// connection failing (reset, keepalive or user timeout), and either way the
// connection is finished.
func watch(s *stream) {
	var buf [256]byte
	for {
		if _, err := s.conn.Read(buf[:]); err != nil {
			s.clean.Store(errors.Is(err, io.EOF))
			s.dead.Store(true)
			return
		}
	}
}

// localAddr returns the pinned source, or nil so the dialer selects one. A
// typed nil *net.TCPAddr in the Dialer's interface field is NOT the same as an
// untyped nil and makes the dial fail, so the conversion happens here once.
func (c *Client) localAddr() net.Addr {
	if c.LocalAddr == nil {
		return nil
	}
	return c.LocalAddr
}

// advanceLocked moves to the next ingress address (no-op with a single one)
// and starts the fail-back clock the first time we leave the preferred one.
func (c *Client) advanceLocked() {
	if len(c.Addrs) <= 1 {
		return
	}
	c.idx = (c.idx + 1) % len(c.Addrs)
	if c.idx != 0 && c.leftAt.IsZero() {
		c.leftAt = c.now()
	}
	if c.idx == 0 {
		c.leftAt = time.Time{}
	}
}

// maybeFailBackLocked returns to the preferred address once Prefer has passed.
// Fail-back drops the current connection so the next submission dials side A:
// the landing posture is primary/standby, so staying on the standby after the
// primary recovers is a silent half-outage nobody is alerted to.
//
// It waits for the current connection's records to be confirmed first, so a
// fail-back never turns records in flight into resends.
func (c *Client) maybeFailBackLocked() {
	if c.Prefer <= 0 || c.idx == 0 || c.leftAt.IsZero() {
		return
	}
	if c.now().Sub(c.leftAt) < c.Prefer {
		return
	}
	if c.cur != nil && len(c.unconfirmed) > 0 {
		return
	}
	c.idx = 0
	c.leftAt = time.Time{}
	if c.cur != nil {
		_ = c.cur.conn.Close()
		c.cur, c.conn = nil, nil
	}
	if c.Log != nil {
		c.Log.Info("up-tunnel failing back to preferred address", "addr", c.Addrs[0])
	}
}
