// Package secpverify verifies secp256k1 ECDSA signatures with decred's
// secp256k1 (github.com/decred/dcrd/dcrec/secp256k1/v4, the curve code under
// btcd's btcec/v2) through the two hooks go-sdk offers for an external
// verifier: primitives/ec InjectExternalVerifySignatureFn (every
// ec.Signature.Verify: field signatures, wallet and message checks,
// OP_CHECKMULTISIG) and script/interpreter InjectExternalVerifySignatureFn
// (OP_CHECKSIG and OP_CHECKSIGVERIFY). go-sdk's own verifier hands its
// KoblitzCurve to crypto/ecdsa, which takes the generic-curve path; decred's
// is a dedicated secp256k1 implementation, pure Go (CGO_ENABLED=0 holds).
//
// # Same verdicts
//
// Verify answers what go-sdk's built-in path answers for every input the
// hooks can hand it:
//
//   - The hash: crypto/ecdsa refuses an empty hash, and so does Verify; any
//     other length is read as the curve order's leading bytes by both. go-sdk
//     refuses a hash wider than the order before either verifier runs.
//   - The signature: strict DER, as both hooks hand it (the interpreter's
//     after its own BIP66 and parse checks, the ec hook re-encoded from R and
//     S). Decred's DER parser accepts exactly the BIP66 encodings with R and
//     S in [1, N-1] that go-sdk's strict parser accepts, with the same values.
//     High S verifies under both (ECDSA is symmetric in S; LOW_S is a script
//     rule the interpreter applies before the hook).
//   - The key: compressed, uncompressed or hybrid (parity checked), on the
//     curve, coordinates below the field prime. The one class that differs is
//     a compressed key whose X is at or above the field prime: go-sdk reduces
//     X and computes with that point, decred (and bitcoin-sv's libsecp256k1)
//     refuse the key. A signature that verifies under such a point needs the
//     discrete log of a point whose x is below 2^33, so none can be produced;
//     both answer false for every signature anyone can make.
//   - The arithmetic: u1 = 0, an infinite sum, the doubling case and an R
//     whose x lies between the group order and the field prime (r = x - N)
//     give the same answer.
//
// The package tests compare the two paths over random and adversarial cases
// at the function and at the script level. go-sdk v1.7.3's own test suites
// (49 packages, the interpreter's script and node vectors included) pass with
// Verify as the default verifier, exactly as they pass without it.
package secpverify

import (
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// Name is the verifier Install puts in place, for logs and build labels.
const Name = "decred-secp256k1-v4"

// Verify reports whether sig (strict DER, no hash type byte) is a valid
// ECDSA signature of hash under pub (33-byte compressed, 65-byte
// uncompressed or hybrid). It has the signature of go-sdk's external
// verifier hooks.
func Verify(hash, sig, pub []byte) bool {
	// crypto/ecdsa, go-sdk's built-in path, refuses an empty hash; decred
	// would read it as zero.
	if len(hash) == 0 {
		return false
	}
	s, err := ecdsa.ParseDERSignature(sig)
	if err != nil {
		return false
	}
	k, err := secp256k1.ParsePubKey(pub)
	if err != nil {
		return false
	}
	return s.Verify(hash, k)
}

// Install makes Verify go-sdk's ECDSA verifier for this process: both hooks,
// so OP_CHECKSIG and every ec.Signature.Verify use it. Call it once at
// startup, before any verification.
func Install() {
	ec.InjectExternalVerifySignatureFn(Verify)
	interpreter.InjectExternalVerifySignatureFn(Verify)
}

// Uninstall restores go-sdk's built-in verifier.
func Uninstall() {
	ec.InjectExternalVerifySignatureFn(nil)
	interpreter.InjectExternalVerifySignatureFn(nil)
}
