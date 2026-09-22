package facade

import (
	"github.com/bsv-blockchain/go-sdk/overlay"
)

// admittance is the WIRE form of an admittance instruction.
//
// It exists because the SDK's own `overlay.AdmittanceInstructions` carries no
// JSON tags. Go's decoder is case-insensitive, so reading an engine's reply
// into it works; encoding it back out does NOT, because Go then emits its own
// field names. Relaying a reply through the SDK type turns
// `{"outputsToAdmit":[0]}` into `{"OutputsToAdmit":[0]}`, which every real
// client reads as an absent field: it sees no admitted outputs, with a 200 and
// no error anywhere. This was caught by relaying a real engine's reply, not by
// a test whose stub used the same Go types on both sides.
//
// Both engines agree on the names below. The Go engine's generated server
// spells the last one `ancillaryTxIDs`; the SDK and the TypeScript engine use
// `ancillaryTxids`. Only the first two are load-bearing for a client deciding
// what was admitted, and the two spellings of the last are emitted so neither
// client is surprised.
type admittance struct {
	OutputsToAdmit []uint32 `json:"outputsToAdmit"`
	CoinsToRetain  []uint32 `json:"coinsToRetain"`
	CoinsRemoved   []uint32 `json:"coinsRemoved,omitempty"`
	AncillaryTxids []string `json:"ancillaryTxids,omitempty"`
	AncillaryTxIDs []string `json:"ancillaryTxIDs,omitempty"`
}

// wireSteak converts a decoded STEAK to its wire form.
//
// Empty slices are emitted as [] rather than null. A client that tests the
// length of the field would crash on null, and an empty admittance is the
// engine's ordinary duplicate answer, so it is the single most common reply
// this facade relays.
func wireSteak(s overlay.Steak) map[string]admittance {
	out := make(map[string]admittance, len(s))
	for topic, ai := range s {
		w := admittance{OutputsToAdmit: []uint32{}, CoinsToRetain: []uint32{}}
		if ai == nil {
			out[topic] = w
			continue
		}
		if ai.OutputsToAdmit != nil {
			w.OutputsToAdmit = ai.OutputsToAdmit
		}
		if ai.CoinsToRetain != nil {
			w.CoinsToRetain = ai.CoinsToRetain
		}
		w.CoinsRemoved = ai.CoinsRemoved
		for _, h := range ai.AncillaryTxids {
			if h != nil {
				w.AncillaryTxids = append(w.AncillaryTxids, h.String())
			}
		}
		w.AncillaryTxIDs = w.AncillaryTxids
		out[topic] = w
	}
	return out
}
