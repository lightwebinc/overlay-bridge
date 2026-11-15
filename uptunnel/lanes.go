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
// rather than a fixed share. A lane with no room for the record, by bytes or
// by count, hands it to another that has. A lane that has just failed a record (its
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

// order is the lanes a record may go to now, best first: the lanes that have
// not failed within the hold (every lane when none is healthy), fewest bytes
// queued first, level lanes in the order a rotation that begins at start gives
// them so they take turns. It appends to buf.
func (l *Lanes) order(start int, buf []*Queue) []*Queue {
	n := len(l.qs)
	cutoff := l.clock().Add(-l.hold()).UnixNano()
	out := buf
	for i := 0; i < n; i++ {
		if q := l.qs[(start+i)%n]; q.failedAt.Load() <= cutoff {
			out = append(out, q)
		}
	}
	if len(out) == len(buf) {
		for i := 0; i < n; i++ {
			out = append(out, l.qs[(start+i)%n])
		}
	}
	// A handful of lanes: an insertion sort, which keeps level lanes in
	// rotation order.
	lanes := out[len(buf):]
	for i := 1; i < len(lanes); i++ {
		for j := i; j > 0 && lanes[j].bytes.Load() < lanes[j-1].bytes.Load(); j-- {
			lanes[j], lanes[j-1] = lanes[j-1], lanes[j]
		}
	}
	return out
}

// rotate is where the next record's rotation begins.
func (l *Lanes) rotate() int { return int(l.next.Add(1) % uint64(len(l.qs))) }

// pick is the lane the next record is dealt: the first of [Lanes.order].
func (l *Lanes) pick() *Queue {
	if len(l.qs) == 1 {
		return l.qs[0]
	}
	return l.order(l.rotate(), nil)[0]
}

// lanePoll is how often a publisher waiting for room looks at the lanes
// again. Room on the lane it was dealt wakes it at once; room on another, or a
// lane back in service, is found by this.
const lanePoll = 5 * time.Millisecond

// Publish queues record on one lane and returns nil once it is queued. The
// lane is the one [Lanes] describes; if that lane has no room for the record,
// by bytes or by count, the next best lane in service that has takes it,
// before the publisher waits or the record is shed (fewest bytes queued does
// not mean room: a lane full of small records is full). With no room on any
// of them the publisher waits as on one queue, for the dealt lane's
// [Queue.Wait], looking again at which lanes are in service each time it
// tries, and then gets [ErrQueueFull]; the wait and the shed are booked on
// the lane the record was first dealt. The record is owned by the lane that
// takes it, as with [Queue.Publish].
func (l *Lanes) Publish(ctx context.Context, record []byte, sent func()) error {
	if len(l.qs) == 1 {
		return l.qs[0].Publish(ctx, record, sent)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start := l.rotate()
	var buf [8]*Queue
	it := item{rec: record, sent: sent}
	// offer tries the lanes in service now, best first, and reports the lane
	// the record was dealt (the best) and whether any took it.
	offer := func() (dealt *Queue, ok bool) {
		order := l.order(start, buf[:0])
		for _, q := range order {
			if q.offer(it) {
				return order[0], true
			}
		}
		return order[0], false
	}
	first, ok := offer()
	if ok {
		return nil
	}
	if first.Wait <= 0 {
		return first.refuse(record)
	}
	first.waited.Add(1)
	timer := time.NewTimer(first.Wait)
	defer timer.Stop()
	poll := time.NewTicker(lanePoll)
	defer poll.Stop()
	for {
		// The room signal before the attempt, as in Queue.Publish.
		room := first.roomSignal()
		if _, ok := offer(); ok {
			return nil
		}
		select {
		case <-room:
		case <-poll.C:
		case <-timer.C:
			return first.refuse(record)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
