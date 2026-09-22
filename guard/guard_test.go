package guard

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// TestKeyMatchesIngressClaim pins the key byte for byte against the fabric
// ingress's own dedup key. If this drifts, the bridge and the plane stop
// agreeing on what "the same object" is and can double-count silently, which
// is exactly the failure the shared key exists to prevent.
func TestKeyMatchesIngressClaim(t *testing.T) {
	content := objfmt.ContentID([]byte("an object"))
	topic := objfmt.TopicID("tm_example")

	h := sha256.New()
	h.Write(content[:])
	h.Write(topic[:])
	var want registry.Key
	copy(want[:], h.Sum(nil))

	if got := Key(content, topic); got != want {
		t.Fatalf("Key = %x, want %x", got, want)
	}
}

// TestKeyPairsTopic is the property D4 deliberately kept: sibling emissions of
// ONE object to DIFFERENT topics must not collapse, or the second topic's
// publish is suppressed and that topic silently never hears the object.
func TestKeyPairsTopic(t *testing.T) {
	content := objfmt.ContentID([]byte("one object, two topics"))
	if Key(content, objfmt.TopicID("tm_a")) == Key(content, objfmt.TopicID("tm_b")) {
		t.Fatal("same object on two topics collapsed to one key")
	}
	// ...and the converse: two objects on one topic must not collide either.
	topic := objfmt.TopicID("tm_a")
	if Key(objfmt.ContentID([]byte("x")), topic) == Key(objfmt.ContentID([]byte("y")), topic) {
		t.Fatal("two objects on one topic collapsed to one key")
	}
}

func TestMarkAndLookup(t *testing.T) {
	g := New(time.Minute, 1024)
	content := objfmt.ContentID([]byte("delivered by the plane"))
	topic := objfmt.TopicID("tm_example")

	if _, known := g.Lookup(content, topic); known {
		t.Fatal("empty guard reported a hit")
	}

	// The feed marks Delivered before handing the object to the engine.
	if prev, known := g.Mark(content, topic, registry.Delivered); known {
		t.Fatalf("first Mark reported known (prev %v)", prev)
	}

	// A client handing the same bytes back to the facade is a loop: the facade
	// sees it is already Delivered and must not publish it.
	prev, known := g.Mark(content, topic, registry.Submitted)
	if !known || prev != registry.Delivered {
		t.Fatalf("re-publish: known=%v prev=%v, want true/Delivered", known, prev)
	}

	if dir, known := g.Lookup(content, topic); !known || dir == 0 {
		t.Fatalf("Lookup after Mark: dir=%v known=%v", dir, known)
	}
	// A different topic is a different key, so it is not a loop.
	if _, known := g.Lookup(content, objfmt.TopicID("tm_other")); known {
		t.Fatal("other topic reported a hit")
	}
}

// TestNewClampsToDefaults pins that a misconfigured guard is sized
// conservatively rather than disabled. A guard that admits everything would
// turn every re-submission into a second publish.
func TestNewClampsToDefaults(t *testing.T) {
	g := New(0, 0)
	content := objfmt.ContentID([]byte("x"))
	topic := objfmt.TopicID("tm_a")
	g.Mark(content, topic, registry.Delivered)
	if _, known := g.Lookup(content, topic); !known {
		t.Fatal("guard built with zero arguments did not retain an entry")
	}
	if g.Stats().Entries == 0 {
		t.Fatal("Stats reports no entries after a Mark")
	}
}
