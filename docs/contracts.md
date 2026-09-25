# Contracts

What this bridge relies on in the two released engines, each point checked
against a running engine rather than read out of its source, and the two
questions still open.

## The submit request

One form is accepted by both engines, and the bridge sends only that form:

| Element | Value | Why |
| --- | --- | --- |
| Method and path | `POST {base}/submit` | The base is a bare origin. The TypeScript host mounts on the app root and cannot be given a prefix; the Go server mounts under `/api/v1` and answers 404 at the root |
| `Content-Type` | `application/octet-stream` | The TypeScript host's raw body parser is bound to exactly this type; anything else yields an empty body and a 400. The Go engine's size limiter engages only on it |
| `x-topics` | one name, no spaces, sent once | The Go server reads a JSON array as one topic name and answers 500. It trims a leading space and combines repeated headers; the TypeScript host refuses a repeated header. One plain name in one header is the form neither can misread |
| Body | the object verbatim | |

The bridge refuses a topic containing a space or a comma before it sends
anything. Neither engine strictly needs that, but a name that could be split
or trimmed differently by two engines is a name to refuse.

## The submit response

The Go engine answers `{"STEAK": {...}}`; the TypeScript engine answers the
bare map. Both are decoded, and both are pinned by fixtures captured from the
real engines (`feed/testdata/`). A client that understood only the wrapper
would read every TypeScript admittance as an absent entry and book an error on
a success.

The two engines also disagree on empty collections: Go emits
`coinsToRetain: null` where TypeScript emits `[]`. A decoder that assumes either
is wrong about one of them.

**The facade relays the reply in the engine's wire form**, camelCase and byte
for byte. Go's decoder reads camelCase into the SDK's untagged type happily,
but encoding that type back out produces Go field names, and a client reading
`outputsToAdmit` would find nothing and conclude no output was admitted, with a
200 and no error anywhere. Verified byte-identical against `@bsv/overlay`
v2.3.1.

**A publish that does not happen is a 503.** When the publish queue is full or
the plane is unreachable, the object is not on the plane, so the facade answers
503 with `Retry-After` and the client requeues. The local admittance is real,
but it is not what the client asked for, and answering 200 is the one thing a
publish interface must never do. This is safe because a failed publish is
retryable: the loop guard is marked only after a publish succeeds, and the
fabric ingress drops a second identical submission in flight.

## Proven against the released engines

| Claim | Engine | How |
| --- | --- | --- |
| A delivered record becomes an ordinary submit, and is admitted | `@bsv/overlay` 2.3.1 | a BRC-149 record on the object lane reached the engine as `POST /submit` and admitted output 0 |
| The chain tracker contract: `currentHeight`, `findHeaderForHeight`, `isValidRootForHeight`, and `undefined` for an unknown height | `@bsv/overlay-express` 2.6.1 `ChaintracksProvider` | the real client against this bridge's header API; the root comparison is what proves the display-hex rendering |
| The wrapped answer and the `/api/v1` mount | `go-overlay-services` 1.3.5 | probed directly, fixture captured |
| A stock `overlay-express` host with an advertiser supplied and `slapTrackers: []` does not propagate, and still admits | `overlay-express` | its propagation step throws inside `LookupResolver.query` on every admitting submit, is caught, and the submit succeeds. Do not alert on that log line |
| `configureEngine(false)` removes the need for MongoDB to accept traffic, but not to report ready | `overlay-express` | `/health/ready` answers 503 for ever on a Mongo-less host; point a probe at `/health/live` or run a MongoDB beside it |

Two limits on the above. The stock-host run used an advertiser stub that
advertises nothing, so its propagation half is proven and its advertisement
half is not. And the probe objects collapsed to one identity inside both
engines, so propagation behaviour is proven once rather than repeatedly.

## The header read API

A consumer that verifies proofs against this bridge implements a chain
tracker over these routes, without importing this module: an HTTP contract and
a vendored fixture couple more cheaply than a Go dependency, and are the only
option for a consumer not written in Go. The bodies are generated here and
pinned by `TestHeaderAPIFixtures` in `headers/testdata/`; regenerate with
`go test ./headers -run TestHeaderAPIFixtures -regen`.

### Native shape

`GET /v1/tip`

```json
{"hash": "<64 hex>", "height": 101, "known": true}
```

`GET /v1/root/{height}`, held:

```json
{"height": 101, "known": true, "merkleRoot": "<64 hex>"}
```

`GET /v1/root/{height}`, not held, **404**:

```json
{"height": 999999}
```

The 404 is load-bearing. A height this bridge cannot answer for means "not
yet", not "broken". A consumer turns 404 into "this root is not valid" and any
other non-ok status into an error, because collapsing the two makes a broken
header service look like a failed proof.

`GET /v1/header/{hash}`, held:

```json
{"hash": "<64 hex>", "height": 101, "known": true, "merkleRoot": "<64 hex>"}
```

`GET /v1/header/{hash}`, not held, **404**:

```json
{"hash": "<64 hex>"}
```

This is the route an anchor must serve, and the bridge serves it so that one
bridge can anchor another. A bridge calls it on its own anchor when a header
arrives whose parent it never saw, which happens on every restart. An anchor
without it does not fail loudly: headers arrive, every one is counted
`orphaned`, and the tip never moves.

`known` distinguishes a first-hand answer (this host received the header on
its lane) from a relayed one (it holds it only because it re-anchored). A
malformed hash is **400**, which is not the same as "I do not hold it".

### Chaintracks-compatible shape

`GET /chaintracks/v2/height` and `GET /chaintracks/v2/header/height/{height}`,
for a stock host that can only be pointed at a header service by
configuration. The prefix is overridable.

```json
{"height": 101}
{"hash": "<64 hex>", "height": 101, "merkleRoot": "<64 hex>"}
```

An unknown height answers a bare 404 with no body. A chaintracks client keys
headers by hash, so a fabricated header is the worst failure available on this
route, and the route never fills one in.

### The rendering that fails silently

`merkleRoot` is the SDK's **display hex**. A byte-reversed or `0x`-prefixed
rendering fails every comparison in every consumer while the service goes on
answering 200.

## Outcome vocabulary

`admitted`, `empty`, `error`. `empty` is the engine's duplicate answer and the
expected steady state: the delivery pool replays its last object on every
reconnect, so duplicates are routine. Neither engine exposes a metrics
registry, so this is the only duplicate signal available.

The feed's counters count what came off the plane, never what the host holds.
An engine can acquire objects through its own catch-up that never traverse
this feed; the oracle for what a host holds is its own lookup service.

## Bound before parse

`headers.GuardBUMPs` walks an object's BUMP section allocating nothing and
rejects any declared length the remaining bytes could not encode. A reader that
sizes a merkle path straight from the wire can be made to demand an arbitrary
allocation by a very small object, and that failure ends the process rather
than returning an error. This lane is reachable by anyone who can put an object
on the plane. The rule is general: bound first, then parse, and keep a recover
as the third layer rather than the first.

## Open

- **A multi-topic submission is forwarded to the engine once per topic** with
  the same object, so an N-topic submit costs N verifications and a failure on
  the last topic becomes a 502 after the earlier ones were admitted and
  published. Both engines accept a multi-topic submit, so one call with the
  reply split per topic would be cheaper; the two engines' multi-topic reply
  shapes have been read but not run.
- **A run with real, distinct BEEF objects** through a stock host is still
  owed; see the limits above.
