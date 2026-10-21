package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/lightwebinc/overlay-bridge/pkg/spv"
)

// VerifyMode is how the feed uses its own SPV verifier.
type VerifyMode string

// The modes.
//
//   - off: no verification here; the engine verifies, as before.
//   - shadow: every object is verified here AND submitted exactly as before,
//     with no marker, so the engine verifies it too. When the engine answers,
//     the two verdicts are compared and a disagreement is counted. Nothing is
//     dropped and nothing is trusted: shadow can only add information.
//   - enforce: an object this verifier REFUSES is not submitted (counted and
//     logged with its txid). One that passes is submitted with the trust
//     marker, which an engine configured for it honours by skipping its own
//     script verification. One that could not be verified (ResultError) is
//     submitted with NO marker, so the engine verifies it in full: a tracker
//     outage never drops an object and never grants trust.
const (
	VerifyOff     VerifyMode = "off"
	VerifyShadow  VerifyMode = "shadow"
	VerifyEnforce VerifyMode = "enforce"
)

// ParseVerifyMode reads a mode name; empty is off.
func ParseVerifyMode(s string) (VerifyMode, error) {
	switch VerifyMode(s) {
	case "":
		return VerifyOff, nil
	case VerifyOff, VerifyShadow, VerifyEnforce:
		return VerifyMode(s), nil
	}
	return "", fmt.Errorf("verify mode %q is not off, shadow or enforce", s)
}

// Verifier verifies one BEEF object. *spv.Verifier is one.
type Verifier interface {
	Verify(ctx context.Context, beef []byte) spv.Verdict
}

// Mismatch kinds: the two ways the bridge and the engine can disagree.
const (
	// MismatchBridgeOKEngineRefused: the bridge passed an object the engine
	// refused (4xx). Under enforce, the engine would have been told to skip
	// scripts for it, so this is the kind that must stay at zero.
	MismatchBridgeOKEngineRefused = "bridge_ok_engine_refused"
	// MismatchBridgeRefusedEngineOK: the bridge refused an object the engine
	// accepted. Under enforce, the object would not have reached the engine.
	MismatchBridgeRefusedEngineOK = "bridge_refused_engine_ok"
)

// MismatchKinds lists every kind, for presetting series at zero.
var MismatchKinds = []string{MismatchBridgeOKEngineRefused, MismatchBridgeRefusedEngineOK}

// process verifies one object as the mode says, then submits it to each of
// its topics here, booking each answer; the first error is returned after
// every topic has been tried.
func (f *Feed) process(ctx context.Context, j job) error {
	var (
		vd     spv.Verdict
		marker string
	)
	verified := f.Verify != nil && f.VerifyMode != "" && f.VerifyMode != VerifyOff
	if verified {
		vd = f.Verify.Verify(ctx, j.object)
		f.c.addN(&f.c.verifyScripts, uint64(vd.Scripts))
		if f.OnVerified != nil {
			f.OnVerified(vd)
		}
		if f.VerifyMode == VerifyEnforce {
			switch vd.Result {
			case spv.ResultRefused:
				f.c.add(&f.c.spvRefused)
				if f.Log != nil {
					f.Log.Warn("feed: object refused by SPV, NOT submitted",
						"txid", vd.TxID.String(), "reason", string(vd.Reason), "topic", j.names[0], "err", vd.Err)
				}
				return fmt.Errorf("feed: SPV refused %s: %w", vd.TxID, vd.Err)
			case spv.ResultOK:
				if len(f.MarkerKey) >= spv.MinKeyBytes {
					marker = spv.Marker(f.MarkerKey, j.object)
				}
			}
		}
	}

	var first error
	for i, name := range j.names {
		err := f.submitOne(ctx, name, j.object, marker, j.arrived)
		if err != nil && first == nil {
			first = err
		}
		// Compare once per object, on its first topic: the engine verifies
		// an object once whatever topics it is offered under.
		if i == 0 && verified && f.VerifyMode == VerifyShadow {
			f.compare(vd, err)
		}
	}
	return first
}

// compare books a shadow disagreement. An engine that did not answer, or a
// verdict that is not one (ResultError), is not compared.
func (f *Feed) compare(vd spv.Verdict, engineErr error) {
	engineOK := engineErr == nil
	engineRefused := Refused(engineErr)
	var kind string
	switch {
	case vd.Result == spv.ResultOK && engineRefused:
		kind = MismatchBridgeOKEngineRefused
	case vd.Result == spv.ResultRefused && engineOK:
		kind = MismatchBridgeRefusedEngineOK
	default:
		return
	}
	f.c.addMismatch(kind)
	if f.Log != nil {
		f.Log.Warn("feed: SPV shadow MISMATCH", "kind", kind, "txid", vd.TxID.String(),
			"bridge_reason", string(vd.Reason), "bridge_err", vd.Err, "engine_err", engineErr)
	}
}

func (f *Feed) observeSubmit(outcome string, arrived time.Time) {
	if f.OnSubmitted != nil && !arrived.IsZero() {
		f.OnSubmitted(outcome, time.Since(arrived))
	}
}
