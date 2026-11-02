package uptunnel

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultPollInterval is how often the queue asks its client to confirm
// acknowledged records and resend what a dead connection left behind.
const DefaultPollInterval = 200 * time.Millisecond

// ErrQueueFull reports that the publish queue had no room for the record, for
// as long as [Queue.Wait] allows, and the record was not accepted. The caller
// has NOT published: see [Queue.Publish].
var ErrQueueFull = errors.New("uptunnel: publish queue full")

// Queue is the bounded asynchronous front of a [Client].
//
// The facade publishes through it so that a stalled edge cannot head-of-line
// the engine's synchronous submit path: a client waiting on POST /submit is
// waiting on the local engine's admittance, and must not also wait on a tunnel
// whose far side has gone away.
//
// The queue is bounded, by count and by bytes, and sheds rather than growing.
// Memory is what a queue costs, and a count alone let a thousand 4 MiB pieces
// sit in it. A shed record is one that did NOT reach the plane, so shedding
// must be visible: it is counted, it is logged, and [Publish] returns
// [ErrQueueFull] so the caller can decide what to tell its client. Silently
// dropping here would make the bridge claim a publication it never made.
//
// # Waiting for room
//
// A full queue is usually a drain that has fallen behind for a moment, and
// with Wait set a publisher is held for room rather than refused at once: the
// drain takes a record, the waiting publish takes its place, and the client
// sees the wait as request latency instead of a refusal it must retry. Past
// Wait the record is shed as before. The wait is bounded and honors the
// caller's context, so a plane that has gone away still cannot hold a client
// for long.
type Queue struct {
	c   *Client
	ch  chan item
	log *slog.Logger
	// PollInterval is how often Run polls the client (0 = DefaultPollInterval).
	PollInterval time.Duration
	// MaxBytes also bounds the queue by the bytes of the records in it
	// (0 = by count alone). A record is admitted when it fits under the bound
	// or when the queue is empty, so a record larger than the bound on its
	// own is carried alone rather than refused for its size: the object
	// ceiling is the facade's, not this queue's.
	MaxBytes int64
	// Wait is how long Publish waits for room in a full queue before the
	// record is shed (0 = never: a full queue sheds at once).
	Wait time.Duration

	// bytes is the sum of the records in ch: added when a record is enqueued
	// and subtracted when Run takes it, so the bound is on what the queue
	// holds. A record the client has been handed is bounded by the client's
	// own confirmation window, not here.
	bytes atomic.Int64
	// room is closed when Run takes a record while a publish is waiting, and
	// is nil when none is: a waiting publish makes it, Run closes it.
	roomMu sync.Mutex
	room   chan struct{}

	enqueued, sent, shed, failed, waited atomic.Uint64
}

// item is one queued record and what to run once it has actually been sent.
type item struct {
	rec  []byte
	sent func()
}

// QueueStats is a point-in-time snapshot. Depth is the instantaneous backlog
// and Bytes the records' bytes in it; BytesCapacity is the byte bound
// (0 = none).
//
// Sent counts records the ingress confirmed (see the package docs), never a
// record only written to the local socket. Failed counts records not written
// to either address and records lost after their resends. Waited counts
// publishes that found the queue full and waited for room, whether they were
// then enqueued or shed. Resent, Unconfirmed and PeerClosed come from the
// client.
type QueueStats struct {
	Depth, Capacity, Enqueued, Sent, Shed, Failed uint64
	Bytes, BytesCapacity, Waited                  uint64
	Resent, Unconfirmed, PeerClosed               uint64
}

// NewQueue returns a queue of the given depth in front of c. A non-positive
// depth takes a small default rather than an unbounded channel: unbounded here
// would trade a visible shed for an invisible memory leak under the same
// stall.
func NewQueue(c *Client, depth int, log *slog.Logger) *Queue {
	if depth <= 0 {
		depth = 1024
	}
	return &Queue{c: c, ch: make(chan item, depth), log: log}
}

// Publish enqueues one record. It returns [ErrQueueFull] when the queue has
// no room for it, by count or by bytes, within [Queue.Wait], and the record
// is then not published; with Wait zero a full queue refuses at once. The
// queue takes ownership of record; the caller must not touch it afterwards.
//
// sent, if non-nil, runs once the ingress has CONFIRMED the record (see the
// package docs: acknowledged by the ingress host on a connection that then
// stayed up), and never runs if it is not. Acceptance into the queue is not
// publication, and neither is a successful local write: the send can still
// fail after the caller has answered its client, and anything the caller must
// only do for a record that reached the plane belongs in sent, not after
// Publish returns.
//
// The caller owns the decision about what a refusal means to its own client.
// The standing ruling is that a shed object is NOT billed, so whatever status a
// caller returns must not book delivered egress for it.
func (q *Queue) Publish(ctx context.Context, record []byte, sent func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The queue takes OWNERSHIP of record: the caller must not reuse or modify
	// it after Publish returns. Copying here would double the allocation of
	// every publish on the client's synchronous request path, and the one
	// caller already hands over a freshly encoded buffer.
	it := item{rec: record, sent: sent}
	if q.offer(it) {
		return nil
	}
	if q.Wait <= 0 {
		return q.refuse(record)
	}
	q.waited.Add(1)
	timer := time.NewTimer(q.Wait)
	defer timer.Stop()
	for {
		// Take the room signal BEFORE trying again, so a take that lands
		// between the attempt and the wait is not a wakeup lost.
		room := q.roomSignal()
		if q.offer(it) {
			return nil
		}
		select {
		case <-room:
		case <-timer.C:
			return q.refuse(record)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// offer puts a record on the queue if there is room for it by bytes and by
// count, and reports whether it did.
func (q *Queue) offer(it item) bool {
	n := int64(len(it.rec))
	if !q.reserve(n) {
		return false
	}
	select {
	case q.ch <- it:
		q.enqueued.Add(1)
		return true
	default:
		q.bytes.Add(-n)
		return false
	}
}

// reserve books n bytes against the bound. A record fits when the queue stays
// under the bound with it, or when the queue is empty.
func (q *Queue) reserve(n int64) bool {
	for {
		b := q.bytes.Load()
		if q.MaxBytes > 0 && b > 0 && b+n > q.MaxBytes {
			return false
		}
		if q.bytes.CompareAndSwap(b, b+n) {
			return true
		}
	}
}

// refuse sheds a record the queue had no room for.
func (q *Queue) refuse(record []byte) error {
	q.shed.Add(1)
	if q.log != nil {
		q.log.Warn("publish queue full, record not published", "bytes", len(record),
			"depth", len(q.ch), "capacity", cap(q.ch), "queued_bytes", q.bytes.Load(), "max_bytes", q.MaxBytes, "waited", q.Wait)
	}
	return ErrQueueFull
}

// roomSignal returns a channel that closes the next time Run takes a record.
func (q *Queue) roomSignal() <-chan struct{} {
	q.roomMu.Lock()
	defer q.roomMu.Unlock()
	if q.room == nil {
		q.room = make(chan struct{})
	}
	return q.room
}

// taken releases the bytes of a record Run has taken off the queue and wakes
// a publish waiting for room.
func (q *Queue) taken(it item) {
	q.bytes.Add(-int64(len(it.rec)))
	q.roomMu.Lock()
	if q.room != nil {
		close(q.room)
		q.room = nil
	}
	q.roomMu.Unlock()
}

// Run drains the queue into the client until ctx is cancelled. It returns
// ctx.Err() on cancellation, never a submit error: a failed submission is
// counted and the next record is attempted, because one unreachable edge must
// not stop the drain. Between records it polls the client, which is what
// confirms records and resends after a dead connection while the queue is
// idle.
func (q *Queue) Run(ctx context.Context) error {
	every := q.PollInterval
	if every <= 0 {
		every = DefaultPollInterval
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			q.c.Poll(ctx)
		case it := <-q.ch:
			q.taken(it)
			sent := it.sent
			done := func(confirmed bool) {
				if !confirmed {
					q.failed.Add(1)
					return
				}
				q.sent.Add(1)
				if sent != nil {
					sent()
				}
			}
			if err := q.c.Send(ctx, it.rec, done); err != nil {
				q.failed.Add(1)
				if q.log != nil {
					q.log.Error("up-tunnel publish failed; record is not on the plane", "err", err, "bytes", len(it.rec))
				}
			}
		}
	}
}

// Stats returns a snapshot for the collector.
func (q *Queue) Stats() QueueStats {
	cs := q.c.Stats()
	return QueueStats{
		Depth:         uint64(len(q.ch)),
		Capacity:      uint64(cap(q.ch)),
		Bytes:         uint64(max(q.bytes.Load(), 0)),
		BytesCapacity: uint64(max(q.MaxBytes, 0)),
		Enqueued:      q.enqueued.Load(),
		Sent:          q.sent.Load(),
		Shed:          q.shed.Load(),
		Failed:        q.failed.Load(),
		Waited:        q.waited.Load(),
		Resent:        cs.Resent,
		Unconfirmed:   cs.Unconfirmed,
		PeerClosed:    cs.PeerClosed,
	}
}
