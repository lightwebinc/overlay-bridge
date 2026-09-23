# Contracts and open items

Decisions this repository is held to, and the questions it has not settled.

## What has been proven against real upstream code

Run 2026-09-22 against a released `@bsv/overlay` v2.3.1 engine, constructed
directly with no advertiser (the reference-host posture), and against the real
`ChaintracksProvider` client from `@bsv/overlay-express` v2.6.1:

| Claim | How it was proven |
| --- | --- |
| A delivered record becomes an ordinary submit | A BRC-149 record written to the object lane reached the engine as `POST /submit`, `x-topics: tm_proof`, octet-stream, object verbatim, and the engine ADMITTED it |
| The submit request form is the one both engines accept | The real engine accepted it unchanged |
| The engine answers a BARE STEAK | Captured verbatim as `feed/testdata/steak_bare_real_engine.json`. The bare branch is a requirement, not a hedge |
| The facade relays a client-readable reply | The reply through the facade is byte-identical to the engine's own |
| `currentHeight()` | The real client returned the chained tip height |
| `findHeaderForHeight(h)` | The real client returned the header for a chained height |
| An unknown height is not an error | The real client returned `undefined` rather than throwing |
| `isValidRootForHeight` | The real client returned true for the correct root and false for a wrong one, which is what proves the display-hex rendering |
| The anchor height resolves through the fallback | Served from the header service, as a real deployment does |

A second run the same day put a real `go-overlay-services` v1.3.5 engine
behind its own HTTP server (a test-only in-memory store, not a storage
implementation) and probed it directly. It confirmed three things the bridge
relies on, and **corrected three claims that had been verified by reading the
source rather than by running it**:

| Claim | Real behaviour |
| --- | --- |
| The Go server mounts under `/api/v1` | CONFIRMED. `POST /submit` at the root is 404 |
| It wraps the answer as `{"STEAK":{...}}` | CONFIRMED, captured as `feed/testdata/steak_wrapped_real_engine.json` |
| A JSON-array `x-topics` fails there | CONFIRMED: the whole bracketed string is read as ONE topic name. But the status is **500**, not a 4xx, so it reads as a server fault rather than a bad request |
| "A leading space fails the submit as an unknown topic" | **FALSE.** ` tm_proof` is accepted and the reply names `tm_proof`: the header binder trims. No error is logged |
| "The binder reads only the first header value" | **FALSE.** Two `x-topics` headers produced an unknown-topic error naming the SECOND value, so repeated headers are combined rather than ignored |
| An unknown topic | 500, like every other submit failure on that server |

The bridge's own refusal of a topic containing a space or comma therefore
stands as belt and braces rather than as the necessity it was documented to
be. That is the right outcome, but the reason recorded for it was wrong.

Also worth carrying: the two engines disagree on empty collections. The Go
engine emits `coinsToRetain: null` where the TypeScript engine emits `[]`. A
decoder that assumes either shape is wrong about one of them.

A third run closed the last gap: a **stock `overlay-express` server** on
propagation route 2, with an advertiser supplied through
`configureEngineParams` and `slapTrackers: []`.

| Claim | Real behaviour |
| --- | --- |
| Route 2 does not propagate | CONFIRMED live. A submit that admitted an output produced `Error during propagation to other nodes: ... No competent mainnet hosts found by the SLAP trackers for lookup service: ls_ship` |
| The mechanism is a caught THROW, not a clean skip | CONFIRMED by the stack: `Engine.propagateSubmission` to `TopicBroadcaster.broadcast` to `findInterestedHosts` to `LookupResolver.query` to `competentHostsFor`, which throws. The broadcaster's own "no interested hosts" return is never reached |
| The submit still succeeds | CONFIRMED: 200 with a real STEAK admitting output 0 |
| An operator must not alert on that line | Follows from the above: it is written on every submit that admits |
| The bridge delivers into a stock host | CONFIRMED: records off the lane were submitted with no engine errors |

**Route 2 needs no wallet storage service**, which had been recorded as its
blocker. The blocker belongs to the DEFAULT advertiser, which hardcodes one and
blocks startup on a live round trip to it. Supplying an advertiser through
`configureEngineParams` avoids it entirely, and that is a documented
configuration seam, so the engine is still stock.

**And `configureEngine()` takes an argument that decides whether MongoDB is
required at all.** `configureEngine(false)` skips auto-configuring the SHIP and
SLAP discovery overlays, which is the only thing that calls `ensureMongo`. A
plane host does not want those topic managers anyway, so it needs no MongoDB
to accept traffic.

It does still need one to call itself ready. Measured on the Mongo-less host:
`/health/ready` and `/health` answer **503 for ever**, with the check list
reading `engine: ok`, `knex: ok`, `mongo: error`, all three critical and
`ready`-scoped; only `/health/live` is 200. So such a host is fully functional
and permanently unready by its own report, which fails a Kubernetes readiness
probe and the shipped container HEALTHCHECK. Run a MongoDB beside it for that
reason alone, or point the probe at `/health/live`.

**What this run did NOT prove, and must not be read as settled:** the
advertiser supplied was a STUB that advertises nothing. Route 2's PROPAGATION
half is proven; its ADVERTISEMENT half is not. The claim that a route-2 host
keeps its own advertisements still rests on reading the source, and confirming
it needs a real advertiser against reachable wallet storage.

Honest limit on all three runs: the hand-rolled probe objects collapse to one
identity inside both engines, so each host admits the first and answers `empty`
to the rest. Verified by submitting distinct objects DIRECTLY, bypassing the
bridge, and getting the same answer. The propagation behaviour is therefore
proven once rather than repeatedly, and a run with real, distinct BEEF is still
owed.

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

## Header read API, the two shapes

A consumer that verifies SPV against this bridge implements a chain tracker
over these routes. It does so WITHOUT importing this module: an HTTP contract
plus a vendored fixture is a cheaper coupling than a Go dependency, and it is
the only option for a consumer that is not written in Go.

So the response bodies are a contract with other repositories, not an internal
detail, and they are generated once here rather than described in prose and
re-derived per repo. `headers/testdata/*.json` holds them, pinned by
`TestHeaderAPIFixtures`; regenerate deliberately with
`go test ./headers -run TestHeaderAPIFixtures -regen`.

### Native shape

`GET /v1/tip`

```json
{"hash": "<64 hex>", "height": 101, "known": true}
```

`GET /v1/root/{height}`, height held:

```json
{"height": 101, "known": true, "merkleRoot": "<64 hex>"}
```

`GET /v1/root/{height}`, height not held, **404**:

```json
{"height": 999999}
```

The 404 is load-bearing. A height this bridge cannot answer for is "not yet",
not "something is broken", and a consumer must turn 404 into "this root is not
valid" while turning any OTHER non-ok status into a thrown error. Collapsing
the two makes a broken header service look like a failed proof, which is the
one reading an engine must never make.

`GET /v1/header/{hash}`, hash held:

```json
{"hash": "<64 hex>", "height": 101, "known": true, "merkleRoot": "<64 hex>"}
```

`GET /v1/header/{hash}`, hash not held, **404**:

```json
{"hash": "<64 hex>"}
```

This is the route an ANCHOR must serve, and the bridge serves it so that one
bridge can anchor another. A bridge calls it on its own anchor when a header
arrives whose parent it never saw, which happens on every restart and on any
host whose anchor sits below the chain tip. An anchor that does not serve it
does not fail loudly: the lane connects, headers arrive, every one is counted
`orphaned`, and the tip never moves.

`known` distinguishes a first-hand answer from a relayed one: true means this
host received the header on its lane, false means it holds it only because it
re-anchored through its own anchor. A malformed hash is **400**, which is a
different thing from "I do not hold it" and must not be collapsed into it.

### Chaintracks-compatible shape

`GET /chaintracks/v2/height` and `GET /chaintracks/v2/header/height/{height}`,
for a stock host that can only be pointed at a header service by
configuration. The prefix is a default and is overridable.

```json
{"height": 101}
{"hash": "<64 hex>", "height": 101, "merkleRoot": "<64 hex>"}
```

An unknown height here is a **bare 404 with no body**, deliberately: this route
once filled an unknown hash with the current tip's and answered 200, which
handed the client a fabricated header identity. A chaintracks client keys
headers by hash, so that is the worst failure available on this route.

### The one thing that silently fails

`merkleRoot` is the SDK's **display hex**. A byte-reversed or `0x`-prefixed
rendering fails every comparison in every consumer while this service goes on
answering 200. Verified the hard way: a consumer fed little-endian roots
refused every object with "invalid merkle path", and the service looked
healthy throughout.

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

## Reviewed 2026-09-22

A full-tree review found ten items; nine were fixed with a regression test
each, and all are recorded here because several were the kind that pass every
stub test and fail only in deployment:

- Every clean shutdown exited 1: the lane terminator returns nil on cancel and
  the task group treated that as a failure.
- The in-process tracker cached every root the lane delivered, so a competing
  header at a held height overwrote the canonical root and a reorganisation
  never corrected it. The tracker now holds no cache and asks the store.
- Provenance of root answers was counted on that tracker, which the engine
  never calls (it reads over HTTP), so the metric that exists to prove
  "verification is fed by the lane" read zero for ever. It is counted in the
  store now.
- The chaintracks route filled an unknown hash with the tip's. It now answers
  404 unless the fallback can name the block.
- Fallback roots were never cached, so every verification of an old output was
  a fresh round trip to the third-party header service on the engine's own
  verification path.
- The engine submit ran inline in the lane's read loop, so an engine stall
  held the delivery socket past the edge's write deadline. There is now a
  bounded worker pool with a visible shed.
- The loop guard was marked when the publish queue ACCEPTED a record, so an
  asynchronous send failure after the client's 200 stranded the object for
  the guard TTL. It is now marked from the queue's sent callback.
- Sink mode with topics set dereferenced a nil engine on the first delivery.
- The publish queue copied every record, doubling the allocation of a 64 MiB
  publish; it now takes ownership.

**Not changed, recorded as a follow-up:** a multi-topic submission is
forwarded to the engine once PER topic with the same object, so an N-topic
submit costs N full verifications and turns a failure on the last topic into a
502 after the earlier ones were admitted and published. Both engines accept a
multi-topic submit, so one engine call with the reply split per topic would be
cheaper. It is left as is for now because the two engines' multi-topic reply
shapes have not been run, only read. The same per-topic shape applies on the
feed side: a delivery whose payload names several topics this bridge elected
is submitted to each of them.

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
