package guard

import (
	"crypto/sha256"

	"github.com/lightwebinc/teranode-bridge/registry"
)

// Key is SHA-256(ContentID ‖ TopicID), the shape of the key the fabric's own
// BEEF ingress claims an object under. ContentID here is over the OBJECT
// bytes on both of the bridge's paths (the facade hashes what the client
// sent, the feed hashes what the delivery carried), which is what makes the
// two directions meet; the fabric hashes the whole submission record, so its
// key differs in value but not in shape. Two properties follow.
//
// The TopicID stays in the key. Keying the pair, never the bare ContentID, is
// what keeps a later re-submission of the same object to a NEW topic from
// being suppressed: sibling emissions of one object to different topics share
// a ContentID, so a ContentID-only key would collapse them and publish only
// the first. Ruling D4 replaced the object's identity (a parsed subject txid,
// which meant parsing BEEF to find it) with the delivered bytes' ContentID; it
// did not change the pairing.
//
// The bridge's two directions agree on what "the same object" is: a delivery
// marks the pair the facade would look up for the echoing client, and a
// publish marks the pair the feed would look up for the returning delivery.
func Key(contentID, topicID [32]byte) registry.Key {
	h := sha256.New()
	h.Write(contentID[:])
	h.Write(topicID[:])
	var out registry.Key
	copy(out[:], h.Sum(nil))
	return out
}
