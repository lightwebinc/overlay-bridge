package feed

import "sync"

// SteakKey buckets an engine answer by topic and outcome.
type SteakKey struct{ Topic, Outcome string }

// Outcome values. These are the only three, and the distinction between them
// is what tells an operator whether the host is working.
const (
	// OutcomeAdmitted: the engine admitted at least one output.
	OutcomeAdmitted = "admitted"
	// OutcomeEmpty: the engine answered for the topic with nothing admitted.
	// This is the host's duplicate answer and the expected steady state, not
	// an alarm: the delivery pool replays its last written object on every
	// reconnect, and edge listeners restart on every converge, so duplicates
	// are routine. It is also the only duplicate signal either engine offers,
	// since neither exposes a metrics registry.
	OutcomeEmpty = "empty"
	// OutcomeError: the engine answered 200 with no entry for the topic we
	// submitted. That is a contract violation by the host, not a duplicate.
	OutcomeError = "error"
)

// Stats is a point-in-time snapshot of the feed's counters.
//
// These count what came off the plane. They are NOT a count of what the host
// holds: an engine can acquire objects through its own catch-up protocol that
// never traverse this feed. Do not write an acceptance check that reads engine
// state from these numbers; the oracle for what a host holds is its own lookup
// service.
type Stats struct {
	Submitted   uint64
	EngineError uint64
	// Retried counts objects that FAILED an engine submit at least once and
	// then succeeded. It is the difference between a transient engine restart
	// and data loss, and without it a recovered submit is indistinguishable
	// from one that never had trouble.
	Retried      uint64
	ParseError   uint64
	UnknownTopic uint64
	Rejected     uint64
	// Sunk counts deliveries terminated with no engine configured (sink mode).
	Sunk uint64
	// Shed counts deliveries refused because the engine queue was full. Each
	// is an object the host did NOT receive from the plane. Non-zero means the
	// engine cannot keep up with delivery and the host is relying on its own
	// catch-up to close the gap.
	Shed  uint64
	Steak map[SteakKey]uint64
}

type counters struct {
	mu                                                                              sync.Mutex
	submitted, engineError, parseError, unknownTopic, rejected, sunk, shed, retried uint64
	steak                                                                           map[SteakKey]uint64
}

func (c *counters) add(field *uint64) {
	c.mu.Lock()
	*field++
	c.mu.Unlock()
}

func (c *counters) addSteak(topic, outcome string) {
	c.mu.Lock()
	if c.steak == nil {
		c.steak = make(map[SteakKey]uint64)
	}
	c.steak[SteakKey{Topic: topic, Outcome: outcome}]++
	c.mu.Unlock()
}

func (c *counters) snapshot() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Stats{
		Submitted:    c.submitted,
		EngineError:  c.engineError,
		Retried:      c.retried,
		ParseError:   c.parseError,
		UnknownTopic: c.unknownTopic,
		Rejected:     c.rejected,
		Sunk:         c.sunk,
		Shed:         c.shed,
		Steak:        make(map[SteakKey]uint64, len(c.steak)),
	}
	for k, v := range c.steak {
		s.Steak[k] = v
	}
	return s
}
