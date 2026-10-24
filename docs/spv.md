# SPV verification in the bridge

The bridge can verify every delivered object by SPV itself, with the Go SDK,
before the engine sees it. The point is speed: the engine's TypeScript SDK
takes several milliseconds per script check, and the Go SDK takes well under
one. Verification is the first piece of the overlay stack to move to Go. The
verifier lives in [`pkg/spv`](../pkg/spv), with no knowledge of lanes, topics
or HTTP, so that a Go overlay engine can reuse it.

## What is verified

For each object the bridge verifies:

- every merkle path, against the bridge's **own** header store, in process
  (the same store the engine reads over `/v1`, with no HTTP hop);
- every transaction without a merkle path: its scripts, amounts, inputs,
  finality and coinbase maturity, under the network's activation heights
  (`-verify-network`).

The bridge checks **every** unproven transaction in the object, not only the
one the engine treats as its subject. That way an engine told "this object
passed" can skip scripts for any transaction in it, without relying on the
two parsers agreeing on which transaction is the subject.

The Go checks are stricter than the TypeScript SDK's `verify`, which checks
paths, amounts and scripts only. Shadow mode exists to measure any
disagreement this causes before anything relies on it.

### Finality is a policy (`-verify-finality`, default `ignore`)

Committed-record carriers are unmined transactions that are non-final **by
design**: `nLockTime = 4102444800` (2100-01-01) with a non-final input, so
they can never be mined. An overlay admits a carrier when its scripts, proofs
and amounts are valid, and the TypeScript engine does exactly that.

go-sdk v1.7.2's `spv.Verify` applies bitcoin-sv's finality rule, and it has
no option to switch that rule off on its own. Under the owner ruling of
2026-10-21 ("own policy on v1.7.2"), `pkg/spv` therefore does not call
`spv.Verify`. It runs the same checks itself, built from go-sdk's
primitives: `MerklePath.Verify`, the script interpreter, and activation-height
flags. Finality is an explicit `FinalityPolicy`:

- `ignore`: the overlay plane's policy and the default. Finality is not
  checked.
- `require`: bitcoin-sv's rule, as v1.7.2 applies it.

Every other v1.7.2 check is kept: source-txid match, merkle paths before any
height is used, no unmined coinbase, inputs and outputs present, amounts
within `MAX_MONEY`, no P2SH output, no null outpoint, no duplicate input, no
confiscation transaction, every spent output present, coinbase maturity
(100), confiscation maturity (1000), outputs not exceeding inputs, and
scripts under the era flags (including P2SH-shaped coins being judged as
pre-Genesis). Under `require`, a time-based lock is always refused, as in
v1.7.2, because the header store does not report a median time past.

One v1.7.2 refinement is not reproduced. v1.7.2 skips re-verifying
transactions it has already verified (pointer-distinct copies that share a
txid and the same evidence). Here each Transaction value is verified once
per tip. That is a cost difference only. The checks are the same.

## Modes (`-verify`)

| Mode | Bridge verifies | Object submitted | Marker | Engine verifies scripts |
| --- | --- | --- | --- | --- |
| `off` (default) | no | always | never | yes |
| `shadow` | yes | always, as before | never | yes, and the two verdicts are compared |
| `enforce` | yes | only if not refused | on a pass | no, when it trusts the marker |

Each verification has one of three results:

- `ok`: every path proved and every unproven transaction passed.
- `refused`: the object fails on its merits: it does not parse, a path
  proves a root the chain does not commit to, or a transaction fails a
  script or structural check. In enforce mode the object is **not
  submitted**. It is counted (`overlay_bridge_feed_total{outcome="spv_refused"}`)
  and logged with its txid and a bounded reason.
- `error`: no verdict was reached. The tracker failed, a path names a height
  above the tip (its header has not arrived), or `-verify-timeout` ran out.
  The object is submitted **without** a marker, so the engine verifies it in
  full. A header outage therefore never drops an object and never grants
  trust.

In shadow mode the bridge compares its verdict with the engine's answer.
`overlay_bridge_verify_mismatch_total{kind}` counts two kinds of
disagreement:

- `bridge_ok_engine_refused`: the bridge passed an object that the engine
  refused (HTTP 4xx). Under enforce, the engine would have skipped the
  scripts for this object. **It must read zero before enforce.**
- `bridge_refused_engine_ok`: the bridge refused an object that the engine
  accepted. Under enforce, this object would not reach the engine.

A 4xx from the engine means the engine refused the object for any reason,
not only an SPV failure. A malformed body also produces a 4xx, but the
bridge has already parsed such an object, so in practice the 4xx is an SPV
refusal. Errors and engine outages are not compared.

## The trust boundary

In enforce mode the bridge marks each passing object with a header:

```text
X-Overlay-Bridge-Verified: v1=<hex HMAC-SHA256(key, "overlay-bridge-verified/v1\0" || body)>
```

Both engines the bridge feeds honor the marker the same way: the
TypeScript host (overlay-blueprints) and the Go one (overlay-engine). The
engine honors the marker only when **all** of these conditions hold:

1. It is configured to honor it. Trust is off by default
   (`OVERLAY_TRUST_BRIDGE_VERIFY=true` in both).
2. The request arrived on a loopback address.
3. It holds the same key (`OVERLAY_BRIDGE_MARKER_KEY_FILE`), and the HMAC over
   the body it actually read matches, compared in constant time.

Each check covers a weakness in the others. Anything on the machine can reach
loopback, so loopback alone is not enough. A key that leaks off the machine
must not let a remote submitter skip verification, so the key alone is not
enough. The HMAC binds the marker to one exact body, so a marker seen on the
wire cannot be moved to another object; replaying it only re-submits bytes
that already passed. The key is a file of at least 32 bytes
(`openssl rand -hex 32`), mode 0600, shared by the two processes and readable
only by root. Both sides refuse a key that is shorter or readable by group or
others.

If a marker fails any check, the engine **ignores** it rather than refusing
the object, and verifies the object in full. The engine counts each marker it
sees in `overlay_host_bridge_marker_total{result}` (`accepted`, `disabled`,
`not_loopback`, `bad_mac`).

The marker waives **script** verification only. With an accepted marker,
the engine still parses the object, checks every merkle path against its own
chain tracker, checks amounts and runs its topic managers. overlay-blueprints
does this through the SDK's script-verification backend seam: it trusts the
object's txids for the duration of that one submission. A Go engine runs
`pkg/spv` with `Verifier.SkipScripts`: every check of a full verification but
the interpreter loop. A txid commits to
the unlocking scripts and to every source txid, and therefore to every
locking script and amount the scripts are checked against. A verdict about a
txid therefore holds for any submission that carries it. The engine fork is
unchanged.

Objects that clients submit through the facade are never marked. The
engine verifies them as before.

## Metrics

| Series | Meaning |
| --- | --- |
| `overlay_bridge_verify_duration_seconds{result}` | One verification. The buckets match the engine's `overlay_host_verify_duration_seconds`, so the two can share a panel |
| `overlay_bridge_verify_scripts_total` | Input scripts run (inputs of unproven transactions) |
| `overlay_bridge_verify_object_{txs,inputs,scripts,proofs,ancestor_depth}` | Per-object work (R2) histograms; `_sum / _count` is the mean per object, and a CPU rate over `rate(..._count)` is CPU per object |
| `overlay_bridge_verify_mismatch_total{kind}` | Shadow disagreements; see above |
| `overlay_bridge_engine_submit_duration_seconds{outcome}` | Lane arrival to the engine's answer, per topic submit. It is language-neutral and covers the whole path |
| `overlay_bridge_feed_total{outcome="spv_refused"}` | Objects that enforce mode did not submit |
| `overlay_bridge_feed_total{outcome="engine_refused"}` | Engine 4xx answers. These are not retried |

On the TypeScript engine, a skipped check is recorded as
`overlay_host_verify_duration_seconds{path="bridge"}` (its duration is the
cost of the skip) and as `overlay_host_verify_skipped_total`. To read the
engine's real verify latency, filter on `path=~"worker|inline"`. The Go
engine counts every marker in `overlay_go_bridge_marker_total{result}`.

## The ECDSA verifier (`-sig-verifier`)

Most of a verification is signature checks: one per input of every unproven
transaction. go-sdk's own verifier hands its curve to `crypto/ecdsa`, which
takes the generic-curve path. go-sdk also offers two hooks for an external
verifier (`primitives/ec` for `Signature.Verify`, `script/interpreter` for
`OP_CHECKSIG`), and `pkg/secpverify` fills both with decred's secp256k1
(`github.com/decred/dcrd/dcrec/secp256k1/v4`, the curve code under btcd's
`btcec/v2`), pure Go. The default, `-sig-verifier decred`, installs it at
startup; `sdk` keeps go-sdk's built-in path.

The verdicts are the same. `pkg/secpverify` documents input by input why (the
hash, strict DER, high S, every key encoding, the edge cases of the
arithmetic), its tests compare the two paths over random and adversarial
cases at the function and the script level, and go-sdk v1.7.3's own test
suites pass with it as the default verifier. The one input class where the
two differ, a compressed key whose x is at or above the field prime, is one
no signature can be produced for; decred refuses the key, as bitcoin-sv
does.

On a 2020 desktop CPU (Ryzen 7 PRO 4750G, one core) a signature check costs
140 us against 182 us built in, and a P2PKH input's script 152 us against
213 us.

## Rollout

1. Run in `shadow` everywhere. Confirm that
   `overlay_bridge_verify_mismatch_total` stays at zero across real traffic,
   and compare `overlay_bridge_verify_duration_seconds` with the engine's
   histogram.
2. Create the key file. Set `OVERLAY_TRUST_BRIDGE_VERIFY=true` and
   `OVERLAY_BRIDGE_MARKER_KEY_FILE` on the engine, then switch the bridge to
   `-verify enforce -verify-marker-key <file>`.
3. Confirm that `overlay_host_bridge_marker_total{result="accepted"}` and
   `overlay_host_verify_skipped_total` climb, that
   `{result=~"bad_mac|not_loopback"}` stays at zero, and that admissions
   continue.
