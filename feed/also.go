package feed

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Also hands every object its route's primary is handed to one more local
// target: a second subscriber at the same site, fed from the same
// subscription. The case it exists for is a storage provider beside an
// overlay host: one consumer elects a bucket's records topic, the host's
// engine indexes the entries and the provider needs them too, to take the
// pieces it holds.
//
// It is not a shadow. Nothing is compared. A target that refuses an object
// (a 4xx) is booked and not asked again, as the primary is not; one that
// fails (a 5xx, or unreachable) is asked again a bounded number of times.
// The offer never blocks its caller: it is a put on a queue bounded by count
// and by bytes, drained by the target's own workers, and an object that
// does not fit is shed and counted. A target that is slow or down costs the
// primary path nothing but the counter.
//
// On the feed an Also is offered every object the plane delivered under the
// route's topics and the bridge's SPV let through; on the facade, every
// submission the primary accepted, which is how the site's other subscriber
// gets what own-traffic exclusion keeps the plane from delivering back.
type Also struct {
	// Submit is the target's client.
	Submit Submitter
	// Name labels the route (its glob) and Target the target's origin, in
	// logs and metrics.
	Name, Target string
	// Workers drain the queue; zero takes 2.
	Workers int
	// QueueDepth bounds the objects waiting; zero takes 1024.
	QueueDepth int
	// QueueBytes bounds the bytes waiting or in a worker's hands; zero
	// takes 256 MiB. An object larger than the bound on its own is shed.
	QueueBytes int64
	// Retries is how many times a failed submit is tried again; zero takes
	// 3, a negative value none.
	Retries int
	// Backoff is the wait before the first retry, growing linearly; zero
	// takes 500 ms.
	Backoff time.Duration
	Log     *slog.Logger

	mu      sync.Mutex
	results map[string]uint64
	qmu     sync.Mutex
	q       chan alsoJob
	bytes   atomic.Int64
	started atomic.Bool
	logged  atomic.Int64
}

// The results of one offer.
const (
	// AlsoOK: the target answered 200.
	AlsoOK = "ok"
	// AlsoRefused: the target answered a 4xx; it is not asked again.
	AlsoRefused = "refused"
	// AlsoError: the target failed or could not be reached on every try.
	AlsoError = "error"
	// AlsoShed: the queue was full, or its workers were not running.
	AlsoShed = "shed"
)

// AlsoResults lists every result, for presetting series at zero.
var AlsoResults = []string{AlsoOK, AlsoRefused, AlsoError, AlsoShed}

type alsoJob struct {
	topic  string
	object []byte
}

// AlsoStats is a snapshot: Results by result, and the queue now.
type AlsoStats struct {
	Results       map[string]uint64
	Queued        int
	Capacity      int
	QueuedBytes   int64
	CapacityBytes int64
}

func (a *Also) add(result string) {
	a.mu.Lock()
	if a.results == nil {
		a.results = make(map[string]uint64, len(AlsoResults))
	}
	a.results[result]++
	a.mu.Unlock()
}

// Running reports whether the workers are draining the queue.
func (a *Also) Running() bool { return a.started.Load() }

// Stats returns a snapshot.
func (a *Also) Stats() AlsoStats {
	a.mu.Lock()
	s := AlsoStats{Results: make(map[string]uint64, len(a.results))}
	for k, v := range a.results {
		s.Results[k] = v
	}
	a.mu.Unlock()
	q := a.queue()
	s.Queued, s.Capacity = len(q), cap(q)
	s.QueuedBytes, s.CapacityBytes = a.bytes.Load(), a.queueBytes()
	return s
}

func (a *Also) workers() int {
	if a.Workers > 0 {
		return a.Workers
	}
	return 2
}

func (a *Also) queueBytes() int64 {
	if a.QueueBytes > 0 {
		return a.QueueBytes
	}
	return 256 << 20
}

func (a *Also) retries() int {
	switch {
	case a.Retries < 0:
		return 0
	case a.Retries == 0:
		return 3
	}
	return a.Retries
}

func (a *Also) backoff() time.Duration {
	if a.Backoff > 0 {
		return a.Backoff
	}
	return 500 * time.Millisecond
}

func (a *Also) queue() chan alsoJob {
	a.qmu.Lock()
	defer a.qmu.Unlock()
	if a.q == nil {
		depth := a.QueueDepth
		if depth <= 0 {
			depth = 1024
		}
		a.q = make(chan alsoJob, depth)
	}
	return a.q
}

// Offer queues one object for the target under one topic. It never blocks,
// and a nil Also accepts the call and does nothing, so callers need no
// guard. The object is held, not copied: callers hand over bytes they no
// longer write to.
func (a *Also) Offer(topic string, object []byte) {
	if a == nil || a.Submit == nil {
		return
	}
	if !a.started.Load() {
		a.add(AlsoShed)
		return
	}
	n := int64(len(object))
	if a.bytes.Add(n) > a.queueBytes() {
		a.bytes.Add(-n)
		a.shed(topic, "the queue's byte bound")
		return
	}
	select {
	case a.queue() <- alsoJob{topic: topic, object: object}:
	default:
		a.bytes.Add(-n)
		a.shed(topic, "the queue is full")
	}
}

func (a *Also) shed(topic, why string) {
	a.add(AlsoShed)
	if a.Log != nil && a.logged.Add(1) <= 20 {
		a.Log.Warn("also: object shed", "route", a.Name, "target", a.Target, "topic", topic, "why", why)
	}
}

// Start runs the workers until ctx is cancelled. Offers made before it runs
// are shed and counted, never queued for a drain that is not coming.
func (a *Also) Start(ctx context.Context) error {
	q := a.queue()
	a.started.Store(true)
	defer a.started.Store(false)
	var wg sync.WaitGroup
	for i := 0; i < a.workers(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-q:
					a.add(a.process(ctx, j))
					a.bytes.Add(-int64(len(j.object)))
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (a *Also) process(ctx context.Context, j alsoJob) string {
	retries, backoff := a.retries(), a.backoff()
	var err error
	for attempt := 0; ; attempt++ {
		if _, err = a.Submit.Submit(ctx, j.topic, j.object); err == nil {
			return AlsoOK
		}
		if Refused(err) {
			if a.Log != nil && a.logged.Add(1) <= 20 {
				a.Log.Warn("also: the target refused the object", "route", a.Name, "target", a.Target, "topic", j.topic, "err", err)
			}
			return AlsoRefused
		}
		if attempt >= retries || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			return AlsoError
		case <-time.After(time.Duration(attempt+1) * backoff):
		}
	}
	if a.Log != nil && a.logged.Add(1) <= 20 {
		a.Log.Warn("also: the target failed, object dropped", "route", a.Name, "target", a.Target, "topic", j.topic, "attempts", retries+1, "err", err)
	}
	return AlsoError
}
