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
	"fmt"
	"log/slog"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/guard"
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
	Log       *slog.Logger

	c counters
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
	topicID, object, n, err := objfmt.DecodeBEEFDelivery(record)
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

	// Copy before hashing. The codec returns the object as a slice into the
	// record, and the lane reader aliases its own buffer until the next read,
	// so the identity must be taken over bytes this handler owns.
	owned := make([]byte, len(object))
	copy(owned, object)

	contentID := objfmt.ContentID(owned)

	// Mark BEFORE the submit. The plane has now delivered this object on this
	// topic; a client that re-submits the same bytes must not push them back
	// up the tunnel. Marking afterwards leaves a window in which the echo
	// arrives before the mark lands.
	if f.Guard != nil {
		f.Guard.Mark(contentID, topicID, registry.Delivered)
	}

	steak, err := f.Submit.Submit(ctx, name, owned)
	if err != nil {
		f.c.add(&f.c.engineError)
		return fmt.Errorf("feed: submit %s to engine: %w", name, err)
	}
	f.c.add(&f.c.submitted)
	f.c.addSteak(name, outcomeOf(steak, name))
	return nil
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
