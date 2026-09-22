package guard

import (
	"crypto/sha256"

	"github.com/lightwebinc/teranode-bridge/registry"
)

// Key is SHA-256(ContentID ‖ TopicID), byte for byte the same key the fabric's
// own BEEF ingress claims an object under. Matching it is deliberate, and two
// properties follow.
//
// The TopicID stays in the key. Keying the pair, never the bare ContentID, is
// what keeps a later re-submission of the same object to a NEW topic from
// being suppressed: sibling emissions of one object to different topics share
// a ContentID, so a ContentID-only key would collapse them and publish only
// the first. Ruling D4 replaced the object's identity (a parsed subject txid,
// which meant parsing BEEF to find it) with the delivered bytes' ContentID; it
// did not change the pairing.
//
// The bridge and the plane agree on what "the same object" is. A publish this
// guard lets through, and the ingress has already claimed, is dropped at
// ingress under this identical key, so the two layers cannot disagree about
// identity and quietly double-count.
func Key(contentID, topicID [32]byte) registry.Key {
	h := sha256.New()
	h.Write(contentID[:])
	h.Write(topicID[:])
	var out registry.Key
	copy(out[:], h.Sum(nil))
	return out
}
