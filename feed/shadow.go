package feed

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/shard-common/objfmt"
)

// Shadow is a second engine fed every object the primary is fed, so that two
// engines can be compared on real traffic before one replaces the other.
//
// Each topic submit the feed makes to the primary is offered to the shadow
// once the primary has answered: the same body, the same topic, the same
// SPV trust marker. The shadow's answer for that topic is compared with the
// primary's and the comparison counted by kind. Nothing the shadow does
// reaches the primary path: the offer is a non-blocking put on a bounded
// queue, drained by the shadow's own workers, and a full queue drops the
// offer and counts the drop. A shadow that is down, slow or wrong costs the
// primary nothing but the counter.
//
// Lookups are not teed. Whether the two engines answer questions alike is
// the application's own demo to run.
type Shadow struct {
	// Submit is the shadow engine's client. A MarkedSubmitter is handed the
	// marker the primary was handed.
	Submit Submitter
	// Workers drain the queue; zero takes a default of 2.
	Workers int
	// QueueDepth bounds offers waiting for a worker; zero takes a default
	// of 1024. A full queue drops.
	QueueDepth int
	// LogMismatches is how many mismatches are logged with their txid and
	// topic before logging stops; zero takes a default of 20. Counting
	// never stops.
	LogMismatches int
	Log           *slog.Logger
	// OnSubmitted is told each shadow submit's result (ok or error) and
	// its duration. It runs on a worker and must not block.
	OnSubmitted func(result string, d time.Duration)
	// Name labels this shadow in logs: the route it belongs to, or
	// DefaultRoute. Metrics carry it as the route label.
	Name string

	c       shadowCounters
	qmu     sync.Mutex
	q       chan shadowJob
	started atomic.Bool
	logged  atomic.Int64
}

// The comparison kinds, one per topic submit.
const (
	// ShadowMatch: both engines admitted the same outputs and retained the
	// same coins (both answering nothing is a match: the duplicate answer).
	ShadowMatch = "match"
	// ShadowPrimaryOnly: the primary admitted or retained something and the
	// shadow nothing.
	ShadowPrimaryOnly = "primary_only"
	// ShadowShadowOnly: the shadow admitted or retained something and the
	// primary nothing.
	ShadowShadowOnly = "shadow_only"
	// ShadowDiffer: both answered something, and not the same thing.
	ShadowDiffer = "differ"
	// ShadowError: the shadow could not be reached, refused the object,
	// failed, or answered with no entry for the topic. Nothing to compare.
	ShadowError = "shadow_error"
	// ShadowDropped: the shadow's queue was full, or its workers were not
	// running, so the object was never offered to it.
	ShadowDropped = "shadow_dropped"
)

// ShadowKinds lists every kind, for presetting series at zero.
var ShadowKinds = []string{ShadowMatch, ShadowPrimaryOnly, ShadowShadowOnly, ShadowDiffer, ShadowError, ShadowDropped}

// ShadowStats is a snapshot of the shadow's counters: Kinds by comparison
// kind, and Submitted, the submits that reached the shadow engine.
type ShadowStats struct {
	Kinds     map[string]uint64
	Submitted uint64
}

type shadowCounters struct {
	mu        sync.Mutex
	kinds     map[string]uint64
	submitted uint64
}

func (c *shadowCounters) add(kind string) {
	c.mu.Lock()
	if c.kinds == nil {
		c.kinds = make(map[string]uint64, len(ShadowKinds))
	}
	c.kinds[kind]++
	c.mu.Unlock()
}

func (c *shadowCounters) snapshot() ShadowStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := ShadowStats{Kinds: make(map[string]uint64, len(c.kinds)), Submitted: c.submitted}
	for k, v := range c.kinds {
		s.Kinds[k] = v
	}
	return s
}

// Stats returns a counter snapshot.
func (s *Shadow) Stats() ShadowStats { return s.c.snapshot() }

type shadowJob struct {
	topic  string
	object []byte
	marker string
	// primary is the primary's answer for the topic; nil when it gave none
	// (refused, unreachable, or answered without an entry).
	primary *overlay.AdmittanceInstructions
}

func (s *Shadow) workers() int {
	if s.Workers > 0 {
		return s.Workers
	}
	return 2
}

func (s *Shadow) queue() chan shadowJob {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if s.q == nil {
		depth := s.QueueDepth
		if depth <= 0 {
			depth = 1024
		}
		s.q = make(chan shadowJob, depth)
	}
	return s.q
}

// Start runs the workers until ctx is cancelled. Offers made before it runs
// are dropped and counted, never queued for a drain that is not coming.
func (s *Shadow) Start(ctx context.Context) error {
	q := s.queue()
	s.started.Store(true)
	defer s.started.Store(false)
	var wg sync.WaitGroup
	for i := 0; i < s.workers(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-q:
					s.process(ctx, j)
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// Offer queues one topic submit for the shadow: the object, the topic, the
// SPV marker the primary was handed (empty for none) and the primary's
// answer for the topic (nil when it gave none). It never blocks, and a nil
// Shadow accepts the call and does nothing, so callers need no guard.
func (s *Shadow) Offer(topic string, object []byte, marker string, primary *overlay.AdmittanceInstructions) {
	if s == nil || s.Submit == nil {
		return
	}
	if !s.started.Load() {
		s.c.add(ShadowDropped)
		return
	}
	select {
	case s.queue() <- shadowJob{topic: topic, object: object, marker: marker, primary: primary}:
	default:
		s.c.add(ShadowDropped)
	}
}

func (s *Shadow) process(ctx context.Context, j shadowJob) {
	start := time.Now()
	var (
		steak overlay.Steak
		err   error
	)
	if ms, ok := s.Submit.(MarkedSubmitter); ok && j.marker != "" {
		steak, err = ms.SubmitMarked(ctx, j.topic, j.object, j.marker)
	} else {
		steak, err = s.Submit.Submit(ctx, j.topic, j.object)
	}
	d := time.Since(start)
	if err != nil {
		s.c.add(ShadowError)
		s.observe("error", d)
		s.logMismatch(ShadowError, j, "err", err)
		return
	}
	s.c.mu.Lock()
	s.c.submitted++
	s.c.mu.Unlock()
	s.observe("ok", d)
	ai, ok := steak[j.topic]
	if !ok || ai == nil {
		s.c.add(ShadowError)
		s.logMismatch(ShadowError, j, "err", "no entry for the topic")
		return
	}
	kind := Classify(j.primary, ai)
	s.c.add(kind)
	if kind != ShadowMatch {
		s.logMismatch(kind, j, "primary", describe(j.primary), "shadow", describe(ai))
	}
}

func (s *Shadow) observe(result string, d time.Duration) {
	if s.OnSubmitted != nil {
		s.OnSubmitted(result, d)
	}
}

// logMismatch logs the first LogMismatches mismatches, each with the
// object's txid and topic. Past that, counting goes on and logging stops: a
// systematic disagreement would otherwise write one line per object.
func (s *Shadow) logMismatch(kind string, j shadowJob, kv ...any) {
	if s.Log == nil {
		return
	}
	limit := int64(s.LogMismatches)
	if limit <= 0 {
		limit = 20
	}
	if s.logged.Add(1) > limit {
		return
	}
	args := append([]any{"route", s.route(), "kind", kind, "txid", objectTxID(j.object), "topic", j.topic}, kv...)
	s.Log.Warn("feed: shadow engine MISMATCH", args...)
}

func (s *Shadow) route() string {
	if s.Name != "" {
		return s.Name
	}
	return DefaultRoute
}

// Classify compares the primary's answer for a topic with the shadow's. A
// nil answer is an engine that answered nothing for the topic.
func Classify(primary, shadow *overlay.AdmittanceInstructions) string {
	po, pc := sets(primary)
	so, sc := sets(shadow)
	if po == so && pc == sc {
		return ShadowMatch
	}
	pEmpty := po == "" && pc == ""
	sEmpty := so == "" && sc == ""
	switch {
	case pEmpty && !sEmpty:
		return ShadowShadowOnly
	case !pEmpty && sEmpty:
		return ShadowPrimaryOnly
	}
	return ShadowDiffer
}

// sets renders the outputs admitted and the coins retained as canonical
// strings, so two answers compare as sets whatever order they list.
func sets(ai *overlay.AdmittanceInstructions) (outputs, coins string) {
	if ai == nil {
		return "", ""
	}
	return canon(ai.OutputsToAdmit), canon(ai.CoinsToRetain)
}

func canon(v []uint32) string {
	if len(v) == 0 {
		return ""
	}
	c := append([]uint32(nil), v...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	out := make([]byte, 0, 4*len(c))
	for i, x := range c {
		if i > 0 {
			out = append(out, ',')
		}
		out = fmt.Appendf(out, "%d", x)
	}
	return string(out)
}

func describe(ai *overlay.AdmittanceInstructions) string {
	if ai == nil {
		return "none"
	}
	o, c := sets(ai)
	return "admit[" + o + "] retain[" + c + "]"
}

// objectTxID names an object in a log line: the subject txid of an Atomic
// BEEF, else the object's content identifier. Both are bounded.
func objectTxID(object []byte) string {
	if len(object) >= 36 && object[0] == 0x01 && object[1] == 0x01 && object[2] == 0x01 && object[3] == 0x01 {
		if h, err := chainhash.NewHash(object[4:36]); err == nil {
			return h.String()
		}
	}
	id := objfmt.ContentID(object)
	return "content:" + hex.EncodeToString(id[:])
}
