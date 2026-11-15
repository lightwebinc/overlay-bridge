package uptunnel

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultLaneHold is how long a lane that has just failed a record is passed
// over while another lane is healthy.
const DefaultLaneHold = 2 * time.Second

// Lanes publishes over several queues: several consumer tunnels, each a
// [Queue] in front of a [Client] of its own.
//
// # Why more than one
//
// One lane is one TCP connection, and the fabric gives the records of one
// source address in one group one sequence space: one flow, on one link, read
// by one goroutine at each hop. A consumer with several tunnels has several
// inner addresses, and a lane per tunnel makes its publication several flows,
// each with its own sequence space. That is the difference from several
// connections out of ONE source address, which interleave a single flow's
// sequence numbers and read downstream as loss: here each lane has its own
// [Client.LocalAddr], and the ingress numbers each lane's records apart.
//
// # Which lane takes a record
//
// A record goes whole to one lane: the one with the fewest bytes queued, the
// lanes taking turns when they are level. A lane that drains slower holds
// more and is offered less, so the split follows what each tunnel carries
// rather than a fixed share. A lane that has just failed a record (its
// ingress unreachable, a record lost after its resends) is passed over for
// [Lanes.Hold] while another lane is healthy, so a dead tunnel sheds its share
// onto the others instead of failing every record it is dealt; with every
// lane failing they are all offered as before, and each record's outcome is
// its lane's.
//
// Records published to one topic are independent objects, and the plane
// promises no order between them, so the order in which two lanes deliver is
// not something a subscriber may rely on, with one lane or several.
type Lanes struct {
	qs []*Queue
	// Hold is how long a failed lane is passed over (0 = DefaultLaneHold).
	Hold time.Duration

	next atomic.Uint64 // rotates the lane a tie goes to
	now  func() time.Time
}

// NewLanes returns the publisher over qs, which must hold at least one queue.
func NewLanes(qs ...*Queue) *Lanes {
	return &Lanes{qs: qs}
}

// Len reports the number of lanes.
func (l *Lanes) Len() int { return len(l.qs) }

func (l *Lanes) hold() time.Duration {
	if l.Hold > 0 {
		return l.Hold
	}
	return DefaultLaneHold
}

func (l *Lanes) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

// pick is the lane the next record goes to: of the healthy lanes (all of
// them when none is), the one with the fewest bytes queued, starting the
// comparison at a lane that rotates so level lanes take turns.
func (l *Lanes) pick() *Queue {
	n := len(l.qs)
	if n == 1 {
		return l.qs[0]
	}
	start := int(l.next.Add(1) % uint64(n))
	cutoff := l.clock().Add(-l.hold()).UnixNano()
	var best, bestAny *Queue
	var bestBytes, bestAnyBytes int64
	for i := 0; i < n; i++ {
		q := l.qs[(start+i)%n]
		b := q.bytes.Load()
		if bestAny == nil || b < bestAnyBytes {
			bestAny, bestAnyBytes = q, b
		}
		if q.failedAt.Load() > cutoff {
			continue
		}
		if best == nil || b < bestBytes {
			best, bestBytes = q, b
		}
	}
	if best != nil {
		return best
	}
	return bestAny
}

// Publish queues record on one lane (see [Lanes] for which) and returns that
// lane's answer: nil once it is queued, [ErrQueueFull] when the lane had no
// room within its wait. The record is owned by the lane from here, as with
// [Queue.Publish].
func (l *Lanes) Publish(ctx context.Context, record []byte, sent func()) error {
	return l.pick().Publish(ctx, record, sent)
}

// Run drains every lane until ctx is done, and returns the first lane's
// error.
func (l *Lanes) Run(ctx context.Context) error {
	errs := make([]error, len(l.qs))
	var wg sync.WaitGroup
	for i, q := range l.qs {
		wg.Go(func() { errs[i] = q.Run(ctx) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Stats is the lanes' statistics summed: what one queue reported when there
// was one.
func (l *Lanes) Stats() QueueStats {
	var t QueueStats
	for _, q := range l.qs {
		s := q.Stats()
		t.Depth += s.Depth
		t.Capacity += s.Capacity
		t.Enqueued += s.Enqueued
		t.Sent += s.Sent
		t.Shed += s.Shed
		t.Failed += s.Failed
		t.Bytes += s.Bytes
		t.BytesCapacity += s.BytesCapacity
		t.Waited += s.Waited
		t.Resent += s.Resent
		t.Unconfirmed += s.Unconfirmed
		t.PeerClosed += s.PeerClosed
	}
	return t
}

// LaneStats is each lane's statistics, in the order the lanes were given.
func (l *Lanes) LaneStats() []QueueStats {
	out := make([]QueueStats, len(l.qs))
	for i, q := range l.qs {
		out[i] = q.Stats()
	}
	return out
}
