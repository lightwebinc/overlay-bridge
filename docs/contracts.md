# Contracts and open items

Decisions this repository is held to, and the questions it has not settled.

## Submit request, the one form both engines accept

| Element | Value | Why it is not negotiable |
| --- | --- | --- |
| Method and path | `POST {base}/submit` | The base is a bare origin. The TypeScript host mounts on the app root and cannot be given a prefix |
| `Content-Type` | `application/octet-stream` | The TypeScript host's raw body parser is bound to exactly this type; anything else yields an empty body and a 400. The Go engine's size limiter likewise engages only on it |
| `x-topics` | one name, no spaces, one header | The Go engine splits on commas without trimming and reads only the first element, so a leading space fails the submit as an unknown topic. The TypeScript host requires a string, so a repeated header arrives as an array and is refused |
| Body | the object verbatim | |

## Submit response, two shapes

The Go engine answers `{"STEAK": {...}}`; the TypeScript engine answers the
bare map. Both are decoded. A client that understood only the wrapper would
read every successful TypeScript submit as an absent entry and book an error
on a success, which is why the bare branch is a requirement and not a hedge.

## The reply is relayed in the WIRE form

The SDK's admittance type carries no JSON tags. Go's decoder is
case-insensitive, so reading an engine's camelCase reply into it works, and
encoding it back out does not: the reply comes out under Go's own field names.
Relayed that way, `outputsToAdmit` becomes `OutputsToAdmit`, a real client
reads the field it knows, finds nothing, and concludes no output was admitted,
with a 200 and no error anywhere to explain it.

This was found by relaying a real engine's reply, in one request. Tests whose
stub produced and consumed the same Go types on both sides of the facade all
passed while it was live, which is the general lesson: a contract with another
implementation cannot be verified against your own types.

VERIFIED against a released `@bsv/overlay` v2.3.1 engine: the reply through
the facade is byte-identical to the engine's own.

## Outcome vocabulary

`admitted`, `empty`, `error`. `empty` is the engine's duplicate answer and the
expected steady state: the delivery pool replays its last written object on
every reconnect and edge listeners restart on every converge, so duplicates
are routine. Neither engine exposes a metrics registry, so this is the only
duplicate signal available from either.

## What the feed's counters are not

A count of what came off the plane, never a count of what the host holds. An
engine can acquire objects through its own catch-up protocol that never
traverse this feed. The oracle for what a host holds is its own lookup service.

## Bound before parse

`headers.GuardBUMPs` walks an object's BUMP section allocating nothing and
rejects any declared length the remaining bytes could not encode. It exists
because a reader that sizes a merkle path straight from the wire can be made to
demand an arbitrary allocation by a very small object, and that failure ends
the process rather than returning an error, so no recover sees it. This lane is
reachable by anyone who can put an object on the plane.

The bridge itself does not parse BEEF today: the feed's only structural claim
is the leading-marker gate, and the loop guard hashes bytes rather than parsing
them. The guard is exported for the sink mode and for any consumer of this
package, and the rule it encodes is the standing one: bound first, then parse,
and keep a recover as the third layer rather than the first. Pinning a fixed
parser version is not a substitute, because the pin fixes the known case and
the rule is general.

## Open items

**Unused Kafka client in the dependency tree: FIXED upstream.** Importing the
shared lane-termination library used to pull its observability package, which
carried Kafka instrumentation for a sibling bridge, so nine Kafka packages
linked into this binary though this software opens no Kafka connection: build
weight, a NOTICE entry for software we never call, and dependency-scanner
findings against code unreachable here. The instrumentation moved to its own
package in `teranode-bridge` v0.10.0, which only the Kafka user imports, and a
regression test there pins the boundary. This repository pins v0.10.0 and
links zero Kafka packages. Metric names were unchanged by the move, so no
dashboard or alert followed it.

**Publish-shed response code: SETTLED, 503 with `Retry-After`.** When the
bounded publish queue is full, or the plane is unreachable, the object is not
on the plane. Answering 200 would tell a client it published when it did not,
which is the one thing a publish interface must never do; the local admittance
is real but it is not what the client asked for. So the facade answers 503 and
the client requeues. No delivered egress is booked on that path, because a shed
object is not billed.

This is only safe because a failed publish is genuinely retryable, which cost
a defect to learn: the loop guard used to be marked BEFORE the publish attempt,
so a failure left the object marked, every retry was dropped as a loop, and the
object never reached the plane for the life of the guard entry while the client
saw a clean answer. The registry has no way to take a mark back, so the guard
is now marked only after a publish succeeds. The race that opens, two identical
submissions in flight at the same instant, is closed by the fabric ingress,
which claims the identical (ContentID, TopicID) key and drops the second.
