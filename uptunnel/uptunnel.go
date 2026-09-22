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
// # The endpoint-plane route is not this package's business
//
// The route to the ingress belongs to the endpoint-plane failover keeper. This
// client never installs, checks or repairs one, and must not refuse to start
// because the route is momentarily absent: that is the keeper's failover
// window, which close-and-advance already covers.
package uptunnel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
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
	Log     *slog.Logger

	mu      sync.Mutex
	conn    net.Conn
	idx     int
	leftAt  time.Time // when we first moved off Addrs[0]
	nowFunc func() time.Time

	sent, bytes, failures, redials atomic.Uint64
}

// Stats is a point-in-time snapshot.
type Stats struct{ Sent, Bytes, Failures, Redials uint64 }

// Stats returns a counter snapshot.
func (c *Client) Stats() Stats {
	return Stats{Sent: c.sent.Load(), Bytes: c.bytes.Load(), Failures: c.failures.Load(), Redials: c.redials.Load()}
}

func (c *Client) now() time.Time {
	if c.nowFunc != nil {
		return c.nowFunc()
	}
	return time.Now()
}

// Submit writes one submission record's bytes in a single call. On a write
// error it fails over to the next address and retries once; the second failure
// is returned.
func (c *Client) Submit(ctx context.Context, b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maybeFailBackLocked()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.ensureConnLocked(ctx); err != nil {
			lastErr = err
			c.failures.Add(1)
			c.advanceLocked()
			continue
		}
		dl, ok := ctx.Deadline()
		if !ok {
			t := c.Timeout
			if t == 0 {
				t = 10 * time.Second
			}
			dl = c.now().Add(t)
		}
		_ = c.conn.SetWriteDeadline(dl)
		if _, err := c.conn.Write(b); err != nil {
			// A partial write poisons a self-delimiting stream: the reader
			// cannot resynchronise, so drop the connection rather than retry
			// the remainder.
			_ = c.conn.Close()
			c.conn = nil
			c.failures.Add(1)
			c.advanceLocked()
			lastErr = err
			continue
		}
		c.sent.Add(1)
		c.bytes.Add(uint64(len(b)))
		return nil
	}
	return fmt.Errorf("uptunnel: %w", lastErr)
}

// Close releases the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) ensureConnLocked(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	if len(c.Addrs) == 0 {
		return fmt.Errorf("no ingress addresses")
	}
	d := net.Dialer{Timeout: 5 * time.Second, LocalAddr: c.localAddr()}
	conn, err := d.DialContext(ctx, "tcp", c.Addrs[c.idx%len(c.Addrs)])
	if err != nil {
		return err
	}
	c.conn = conn
	c.redials.Add(1)
	if c.Log != nil {
		c.Log.Info("up-tunnel connected", "addr", conn.RemoteAddr().String(), "source", conn.LocalAddr().String())
	}
	return nil
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
func (c *Client) maybeFailBackLocked() {
	if c.Prefer <= 0 || c.idx == 0 || c.leftAt.IsZero() {
		return
	}
	if c.now().Sub(c.leftAt) < c.Prefer {
		return
	}
	c.idx = 0
	c.leftAt = time.Time{}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	if c.Log != nil {
		c.Log.Info("up-tunnel failing back to preferred address", "addr", c.Addrs[0])
	}
}
