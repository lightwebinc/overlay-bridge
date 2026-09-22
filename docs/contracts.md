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

**Unused Kafka client in the dependency tree.** Importing the shared
lane-termination library pulls its observability package, which carries Kafka
instrumentation for a sibling bridge, so nine Kafka packages link into this
binary though this software opens no Kafka connection. Consequences: build
weight, a NOTICE entry for software we never call, and dependency-scanner
findings against code that is unreachable here. The fix belongs upstream, by
moving the Kafka instrumentation out of the package the lane terminator
imports; it would need a new library tag and a re-pin here. Recorded rather
than worked around: writing our own lane terminator to dodge it would be worse
than carrying the dependency.

**Publish-shed response code.** When the bounded publish queue is full the
object is not on the plane. Returning 200 tells a client it published;
returning 503 makes a client retry into a queue that is still full. Whichever
is chosen must not book delivered egress, because a shed object is not billed.
