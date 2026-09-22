package uptunnel

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
)

// ErrQueueFull reports that the publish queue is full and the record was not
// accepted. The caller has NOT published: see [Queue.Publish].
var ErrQueueFull = errors.New("uptunnel: publish queue full")

// Queue is the bounded asynchronous front of a [Client].
//
// The facade publishes through it so that a stalled edge cannot head-of-line
// the engine's synchronous submit path: a client waiting on POST /submit is
// waiting on the local engine's admittance, and must not also wait on a tunnel
// whose far side has gone away.
//
// The queue is bounded and sheds rather than growing. A shed record is one
// that did NOT reach the plane, so shedding must be visible: it is counted, it
// is logged, and [Publish] returns [ErrQueueFull] so the caller can decide what
// to tell its client. Silently dropping here would make the bridge claim a
// publication it never made.
type Queue struct {
	c   *Client
	ch  chan []byte
	log *slog.Logger

	enqueued, sent, shed, failed atomic.Uint64
}

// QueueStats is a point-in-time snapshot. Depth is the instantaneous backlog.
type QueueStats struct{ Depth, Capacity, Enqueued, Sent, Shed, Failed uint64 }

// NewQueue returns a queue of the given depth in front of c. A non-positive
// depth takes a small default rather than an unbounded channel: unbounded here
// would trade a visible shed for an invisible memory leak under the same
// stall.
func NewQueue(c *Client, depth int, log *slog.Logger) *Queue {
	if depth <= 0 {
		depth = 1024
	}
	return &Queue{c: c, ch: make(chan []byte, depth), log: log}
}

// Publish enqueues one record without blocking. It returns [ErrQueueFull] when
// the queue is full, and the record is then not published.
//
// The caller owns the decision about what that means to its own client. The
// standing ruling is that a shed object is NOT billed, so whatever status a
// caller returns must not book delivered egress for it.
func (q *Queue) Publish(ctx context.Context, record []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The record may alias a request body the caller reuses; the queue outlives
	// the call, so it holds its own copy.
	cp := make([]byte, len(record))
	copy(cp, record)
	select {
	case q.ch <- cp:
		q.enqueued.Add(1)
		return nil
	default:
		q.shed.Add(1)
		if q.log != nil {
			q.log.Warn("publish queue full, record not published", "bytes", len(record), "capacity", cap(q.ch))
		}
		return ErrQueueFull
	}
}

// Run drains the queue into the client until ctx is cancelled. It returns
// ctx.Err() on cancellation, never a submit error: a failed submission is
// counted and the next record is attempted, because one unreachable edge must
// not stop the drain.
func (q *Queue) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec := <-q.ch:
			if err := q.c.Submit(ctx, rec); err != nil {
				q.failed.Add(1)
				if q.log != nil {
					q.log.Error("up-tunnel publish failed", "err", err, "bytes", len(rec))
				}
				continue
			}
			q.sent.Add(1)
		}
	}
}

// Stats returns a snapshot for the collector.
func (q *Queue) Stats() QueueStats {
	return QueueStats{
		Depth:    uint64(len(q.ch)),
		Capacity: uint64(cap(q.ch)),
		Enqueued: q.enqueued.Load(),
		Sent:     q.sent.Load(),
		Shed:     q.shed.Load(),
		Failed:   q.failed.Load(),
	}
}
