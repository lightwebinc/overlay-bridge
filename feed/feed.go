// Package feed turns delivered BRC-149 records into ordinary engine submits.
//
// It is the lane handler for objfmt.ClassBEEFDelivery. Each record carries the
// identifier of the topic that matched, the object's length, and the BEEF
// object verbatim. The feed splits on the explicit length, maps the identifier
// back to the name the host elected, and submits the object to the engine on
// the interface the engine already serves. What happens next is what happens
// for any client: the engine parses, verifies by SPV against its chain
// tracker, runs the topic manager, stores admitted outputs, notifies lookup
// services, and answers with admittance instructions.
//
// # The lane is open, so nothing here trusts it
//
// The plane is open to any publisher: the bytes on this lane are whatever some
// publisher chose to send, and the network never parses past the leading
// marker, by design. Every length an object declares is therefore checked
// against the bytes actually present before anything of that size is
// allocated, and a parser failure is a rejected object rather than an
// incident. The codec does the length checking; this package adds the ceiling
// and the marker gate, and refuses anything that fails either.
package feed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// Feed is the lane handler. It holds the only map from topic identifier to
// elected name, computed from the election rather than fetched: the identifier
// is the hash of the name, so the map is derivable and there is nothing to
// look up.
type Feed struct {
	// Topics maps TopicID to the elected topic name.
	Topics map[[32]byte]string
	// Submit is the engine client.
	Submit Submitter
	// Guard is the loop guard. The feed marks an object Delivered before
	// submitting it, so a client that hands the same bytes back to the facade
	// is recognised as a loop rather than published again.
	Guard *guard.Guard
	// MaxObject bounds object bytes. Zero takes the codec default (64 MiB).
	//
	// Keep this at or below the engine's own body limit. Above it, a delivered
	// object becomes a rejection from the engine and an engine error here,
	// which reads as our fault when it is a configuration mismatch.
	MaxObject int
	// Workers is the number of concurrent engine submits. Zero means Handle
	// submits synchronously, in the lane's read loop, which is the right shape
	// for a test and the wrong one for a deployment: the engine's submit can
	// take up to its timeout, and a stall there holds the lane's socket past
	// the edge's write deadline, so the edge drops and redials, the delivery
	// pool replays its last object, and an engine outage becomes a connection
	// storm. With workers, Handle returns as soon as the object is queued.
	Workers int
	// SubmitRetries is how many times a failed engine submit is retried
	// before the object is given up on. Zero takes a small default; a
	// negative value disables retry entirely.
	//
	// It exists because dropping on the first error LOSES DATA, measured: 55
	// and 56 objects on the two lab hosts in one afternoon, every one of them
	// during a window when the engine was restarting for a deploy. The object
	// has already crossed the fabric, been reassembled and been written to
	// this process; the engine being briefly unreachable is the most ordinary
	// failure there is, and it is recoverable by waiting.
	//
	// Retrying here is safe BECAUSE this runs on a worker, off the lane read
	// loop. The constraint that must not be broken is the one in main.go: an
	// engine stall may never hold the delivery socket past the edge's write
	// deadline. A worker blocking costs throughput and, if the queue fills,
	// sheds — which is counted and visible. It never applies backpressure to
	// the socket.
	SubmitRetries int
	// SubmitBackoff is the base delay between submit attempts; each attempt
	// waits one more multiple of it. Zero takes a default.
	SubmitBackoff time.Duration
	// QueueDepth bounds the queued objects behind the workers. Zero takes a
	// small default. When it is full a delivery is SHED: counted, logged, and
	// refused back to the lane, which counts it too and keeps the connection.
	// The host misses that object until its own catch-up finds it, which is a
	// real loss and is preferred to stalling every later delivery behind it.
	QueueDepth int
	// QueueBytes, when set, also bounds the queue by the bytes of the
	// objects in it: a delivery that would take the queue past it is shed.
	// Memory, not the count, is what a queue costs (a lab object is about
	// 1 KB, a BEEF can run to megabytes), so with QueueBytes the count
	// (QueueDepth) can be set deep enough to ride out a burst of small
	// objects while the bytes keep the memory bounded.
	QueueBytes int64
	Log        *slog.Logger

	// Verify is the bridge's own SPV verifier, used as VerifyMode says. Nil,
	// or VerifyMode off, leaves verification to the engine alone.
	Verify     Verifier
	VerifyMode VerifyMode
	// MarkerKey is the key the enforce-mode trust marker is computed under
	// (pkg/spv). Without one, enforce still drops refused objects but sends
	// passing ones unmarked, so the engine verifies them too.
	MarkerKey []byte
	// OnVerified is told of every verdict; OnSubmitted of every submit's
	// outcome and its time from lane arrival to the engine's answer. Both run
	// on a worker and must not block.
	OnVerified  func(spv.Verdict)
	OnSubmitted func(outcome string, d time.Duration)
	// OnDequeued is told, when a worker takes an object off the queue, how
	// long it waited there since it arrived on the lane. It runs on the
	// worker and must not block.
	OnDequeued func(wait time.Duration)

	// Shadow, when set, is offered every topic submit once the primary has
	// answered, with the primary's answer to compare against. It never
	// delays or fails the primary submit.
	Shadow *Shadow
	// Routes send the topics each pattern names to a primary and a shadow of
	// their own (first match wins); every other topic uses Submit and
	// Shadow. See Route.
	Routes []*Route

	c    counters
	busy busyCount

	qmu         sync.Mutex
	q           chan job
	queuedBytes atomic.Int64
	started     atomic.Bool
}

// queue returns the job channel, creating it on first use under the lock so
// Handle and Start can each be first without racing on the field.
func (f *Feed) queue() chan job {
	f.qmu.Lock()
	defer f.qmu.Unlock()
	if f.q == nil {
		depth := f.QueueDepth
		if depth <= 0 {
			depth = 256
		}
		f.q = make(chan job, depth)
	}
	return f.q
}

type job struct {
	names   []string
	object  []byte
	arrived time.Time
}

// ErrShed reports a delivery refused because the engine queue was full.
var ErrShed = errors.New("feed: engine queue full, delivery shed")

// Start runs the workers until ctx is cancelled. It must be called when
// Workers is set; Handle refuses deliveries until it has.
func (f *Feed) Start(ctx context.Context) error {
	if f.Workers <= 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	q := f.queue()
	f.started.Store(true)
	defer f.started.Store(false)
	var wg sync.WaitGroup
	for i := 0; i < f.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-q:
					f.queuedBytes.Add(-int64(len(j.object)))
					if f.OnDequeued != nil && !j.arrived.IsZero() {
						f.OnDequeued(time.Since(j.arrived))
					}
					_ = f.process(ctx, j)
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// NewTopicMap builds the identifier map from elected topic names.
func NewTopicMap(names []string) map[[32]byte]string {
	m := make(map[[32]byte]string, len(names))
	for _, n := range names {
		m[objfmt.TopicID(n)] = n
	}
	return m
}

// Stats returns a counter snapshot.
func (f *Feed) Stats() Stats { return f.c.snapshot() }

func (f *Feed) maxObject() int {
	if f.MaxObject > 0 {
		return f.MaxObject
	}
	return objfmt.DefaultMaxObject
}

// Handle consumes one whole delivery record. It satisfies lanes.Handler.
//
// Returning lanes.ErrReject means the record was well-framed but refused on
// policy: the sender's problem, counted separately, connection kept. Any other
// error is ours or the engine's. Neither drops the connection.
func (f *Feed) Handle(ctx context.Context, record []byte) error {
	arrived := time.Now()
	topicID, payload, n, err := objfmt.DecodeBEEFDelivery(record)
	if err != nil {
		f.c.add(&f.c.parseError)
		return fmt.Errorf("feed: decode delivery record: %w", err)
	}
	if n != len(record) {
		// The lane hands over exactly one object as the codec defines it, so a
		// length disagreement means the codec and the lane disagree, which is
		// a bug rather than a malformed sender.
		f.c.add(&f.c.parseError)
		return fmt.Errorf("feed: record is %d bytes, codec consumed %d", len(record), n)
	}
	// The payload is the publisher's submission record verbatim (every name
	// it wrote, then the object) or, from an older edge, the bare object.
	object, named, err := objfmt.SplitBEEFPayload(payload)
	if err != nil {
		f.c.add(&f.c.parseError)
		return fmt.Errorf("feed: decode delivery payload: %w", err)
	}
	if len(object) > f.maxObject() {
		f.c.add(&f.c.rejected)
		f.logReject("object exceeds maximum", topicID, "bytes", len(object), "max", f.maxObject())
		return lanes.ErrReject
	}
	if !objfmt.IsBEEFObject(object) {
		// The marker gate is the only structural claim made here, and it is
		// the same closed table the network applies at ingress.
		f.c.add(&f.c.rejected)
		f.logReject("object does not lead with a BEEF marker", topicID, "bytes", len(object))
		return lanes.ErrReject
	}
	name, ok := f.Topics[topicID]
	if !ok {
		// A topic joined through the subscription step but absent from this
		// bridge's elected list lands here. It is a named counter with the
		// identifier in the log line on purpose: it is the operator's only
		// signal that a subscription and a bridge configuration have drifted
		// apart, and it reads as nothing else.
		f.c.add(&f.c.unknownTopic)
		f.logReject("delivery for a topic this bridge did not elect", topicID, "bytes", len(object))
		return lanes.ErrReject
	}

	// The plane delivers an object ONCE per subscriber however many of its
	// deliverable topics the subscriber elected, under the first that
	// matched. The other names ride in the payload, so every elected topic
	// the publisher named gets the object here, from this one delivery;
	// names this bridge did not elect are labels and are left alone.
	names := []string{name}
	for _, other := range named {
		if other == name {
			continue
		}
		if elected, ok := f.Topics[objfmt.TopicID(other)]; ok && elected == other {
			names = append(names, other)
		}
	}

	// Copy before hashing. The codec returns the object as a slice into the
	// record, and the lane reader aliases its own buffer until the next read,
	// so the identity must be taken over bytes this handler owns.
	owned := make([]byte, len(object))
	copy(owned, object)

	contentID := objfmt.ContentID(owned)

	// Mark BEFORE the submit, for every topic the object lands on here. The
	// plane has now delivered this object; a client that re-submits the same
	// bytes to any of them must not push them back up the tunnel. Marking
	// afterwards leaves a window in which the echo arrives before the mark
	// lands.
	if f.Guard != nil {
		for _, n := range names {
			f.Guard.Mark(contentID, objfmt.TopicID(n), registry.Delivered)
		}
	}

	if f.Submit == nil {
		// Sink mode: terminate and count, submit nowhere. Without this a
		// well-formed delivery for an elected topic would dereference a nil
		// interface in the lane goroutine and take the whole bridge down.
		f.c.add(&f.c.sunk)
		return nil
	}
	if f.Workers > 0 {
		if !f.started.Load() {
			// Loud rather than silent: a queue nobody drains would fill and
			// shed as if the engine were slow, which is the wrong diagnosis.
			f.c.addShed(f.routeName(name))
			return fmt.Errorf("%w: workers not started", ErrShed)
		}
		q := f.queue()
		n := int64(len(owned))
		if b := f.queuedBytes.Add(n); f.QueueBytes > 0 && b > f.QueueBytes {
			f.queuedBytes.Add(-n)
			f.c.addShed(f.routeName(name))
			f.logReject("engine queue full (bytes), delivery shed", topicID, "bytes", len(owned), "queued_bytes", b-n)
			return ErrShed
		}
		select {
		case q <- job{names: names, object: owned, arrived: arrived}:
			return nil
		default:
			f.queuedBytes.Add(-n)
			f.c.addShed(f.routeName(name))
			f.logReject("engine queue full, delivery shed", topicID, "bytes", len(owned), "depth", cap(q))
			return ErrShed
		}
	}
	return f.process(ctx, job{names: names, object: owned, arrived: arrived})
}

// defaultSubmitRetries and defaultSubmitBackoff give up after roughly half a
// minute, which covers an ordinary service restart of the engine and stops
// well short of holding a worker through a real outage.
const (
	defaultSubmitRetries = 4
	defaultSubmitBackoff = 2 * time.Second
)

func (f *Feed) submitRetries() int {
	switch {
	case f.SubmitRetries < 0:
		return 0
	case f.SubmitRetries == 0:
		return defaultSubmitRetries
	default:
		return f.SubmitRetries
	}
}

func (f *Feed) submitBackoff() time.Duration {
	if f.SubmitBackoff <= 0 {
		return defaultSubmitBackoff
	}
	return f.SubmitBackoff
}

// submitOne submits one object and books the engine's answer, retrying a
// bounded number of times before giving up.
//
// A transport failure or a 5xx is retried, which is what "the engine is
// unreachable" or "restarting" looks like. A 4xx is not: it is the engine
// refusing the object on its merits, and re-submitting it would loop until
// the budget ran out and then book the same failure, having multiplied the
// load for nothing.
//
// marker, when not empty, is the SPV trust marker for this object.
func (f *Feed) submitOne(ctx context.Context, name string, object []byte, marker string, arrived time.Time) error {
	defer f.busy.enter(f.routeName(name), StageSubmit)()
	// The route's other local targets get the object whatever the primary
	// answers: each is a subscriber of its own behind the same lane.
	RouteFor(f.Routes, name).OfferAlso(name, object)
	retries := f.submitRetries()
	backoff := f.submitBackoff()
	engine, shadow := f.enginesFor(name)
	var err error
	for attempt := 0; ; attempt++ {
		var steak overlay.Steak
		if ms, ok := engine.(MarkedSubmitter); ok && marker != "" {
			steak, err = ms.SubmitMarked(ctx, name, object, marker)
		} else {
			steak, err = engine.Submit(ctx, name, object)
		}
		if err == nil {
			oc := outcomeOf(steak, name)
			f.c.add(&f.c.submitted)
			f.c.addSteak(name, oc)
			f.observeSubmit(oc, arrived)
			shadow.Offer(name, object, marker, steak[name])
			if attempt > 0 {
				f.c.add(&f.c.retried)
				if f.Log != nil {
					f.Log.Info("feed: engine submit recovered", "topic", name, "attempts", attempt+1)
				}
			}
			return nil
		}
		if Refused(err) {
			f.c.add(&f.c.engineRefused)
			f.observeSubmit(OutcomeRefused, arrived)
			shadow.Offer(name, object, marker, nil)
			if f.Log != nil {
				f.Log.Warn("feed: engine refused the object", "topic", name, "err", err)
			}
			return fmt.Errorf("feed: submit %s: %w", name, err)
		}
		if attempt >= retries || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * backoff):
		}
	}
	f.c.add(&f.c.engineError)
	f.observeSubmit(OutcomeEngineError, arrived)
	shadow.Offer(name, object, marker, nil)
	if f.Log != nil {
		f.Log.Warn("feed: engine submit failed, object DROPPED",
			"topic", name, "attempts", retries+1, "err", err)
	}
	return fmt.Errorf("feed: submit %s to engine after %d attempts: %w", name, retries+1, err)
}

// enginesFor picks the primary and the shadow for a topic: its route's, or
// the feed's own.
func (f *Feed) enginesFor(topic string) (Submitter, *Shadow) {
	if r := RouteFor(f.Routes, topic); r != nil && r.Submit != nil {
		return r.Submit, r.Shadow
	}
	return f.Submit, f.Shadow
}

// outcomeOf classifies the engine's answer for the topic we submitted.
func outcomeOf(steak overlay.Steak, topic string) string {
	ai, ok := steak[topic]
	if !ok || ai == nil {
		return OutcomeError
	}
	if len(ai.OutputsToAdmit) > 0 {
		return OutcomeAdmitted
	}
	return OutcomeEmpty
}

func (f *Feed) logReject(msg string, topicID [32]byte, kv ...any) {
	if f.Log == nil {
		return
	}
	args := append([]any{"topic_id", fmt.Sprintf("%x", topicID)}, kv...)
	f.Log.Warn("feed: "+msg, args...)
}
