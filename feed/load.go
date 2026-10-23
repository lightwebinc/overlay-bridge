package feed

import "sync"

// The worker stages a busy-worker count is kept for.
const (
	// StageVerify: the worker is running the bridge's own SPV verification
	// of the object (only with VerifyMode shadow or enforce).
	StageVerify = "verify"
	// StageSubmit: the worker is waiting on an engine submit for one of the
	// object's topics, retries and backoff included.
	StageSubmit = "submit"
)

// WorkerStages lists the stages, for presetting series at zero.
var WorkerStages = []string{StageVerify, StageSubmit}

// BusyKey buckets busy workers by the route whose engine the work is for
// (DefaultRoute or a route's pattern) and by stage. Verification counts
// under the route of the topic the plane delivered the object under.
type BusyKey struct{ Route, Stage string }

// busyCount is the number of workers in each stage right now.
type busyCount struct {
	mu sync.Mutex
	n  map[BusyKey]int64
}

// enter counts a worker into a stage and returns the matching exit.
func (b *busyCount) enter(route, stage string) func() {
	k := BusyKey{Route: route, Stage: stage}
	b.mu.Lock()
	if b.n == nil {
		b.n = make(map[BusyKey]int64)
	}
	b.n[k]++
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		b.n[k]--
		b.mu.Unlock()
	}
}

func (b *busyCount) snapshot() map[BusyKey]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[BusyKey]int64, len(b.n))
	for k, v := range b.n {
		out[k] = v
	}
	return out
}

// Busy reports how many workers are in each stage right now, by route. A
// key absent from the map has none.
func (f *Feed) Busy() map[BusyKey]int64 { return f.busy.snapshot() }

// Backlog reports the deliveries queued behind the workers and the queue's
// capacity (-engine-queue). A full queue sheds.
func (f *Feed) Backlog() (queued, capacity int) {
	q := f.queue()
	return len(q), cap(q)
}

// RouteNames lists every route's metrics label: DefaultRoute, then each
// route's pattern in order.
func (f *Feed) RouteNames() []string {
	out := []string{DefaultRoute}
	for _, r := range f.Routes {
		if r != nil {
			out = append(out, r.Name())
		}
	}
	return out
}

// routeName is the metrics label of the route a topic is submitted on.
func (f *Feed) routeName(topic string) string {
	if r := RouteFor(f.Routes, topic); r != nil && r.Submit != nil {
		return r.Name()
	}
	return DefaultRoute
}

// Backlog reports the topic submits queued for the shadow and the queue's
// capacity (-shadow-queue). A full queue drops.
func (s *Shadow) Backlog() (queued, capacity int) {
	q := s.queue()
	return len(q), cap(q)
}
