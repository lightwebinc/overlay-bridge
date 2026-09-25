# Configuration

Every setting is a flag, so a unit file or a Helm `args` list is the whole
configuration. The one exception is the broker token, which may come from
`OVERLAY_BRIDGE_SUBSCRIPTION_TOKEN` so that it need not sit on a command line.

## Modes

| `-mode` | What runs | Use |
| --- | --- | --- |
| `sink` | lanes only: terminate, count, discard | burn-in a slot before an engine exists |
| `feed` | lanes plus engine submits | delivery only, no publishing |
| `all` | everything, including the facade and the up-tunnel | normal |

`sink` needs no engine, anchor or topics. The other modes need all three, and
the bridge refuses to start without them rather than discovering at the first
delivered object that it has nowhere to put it.

## Lanes and topics

| Flag | Default | Notes |
| --- | --- | --- |
| `-beef-lane` | `[::]:9171` | the edge dials this, so the bridge listens |
| `-header-lane` | `[::]:9172` | as above |
| `-topics` | none | the elected topic names; the bridge holds the only name map |
| `-max-object` | codec default (64 MiB) | keep at or below the engine's own body limit |
| `-subscription-url`, `-subscription-consumer` | none | when both are set, `-topics` is checked against the consumer's election at startup and a mismatch is logged as `TOPIC MISMATCH`. A broker that cannot be read is a warning, not a refusal |
| `-subscription-token` | none | bearer token for that read; prefer the environment variable above |

A delivered record for a topic not in `-topics` is counted `unknown_topic`
with the identifier in the log line. That counter is the only signal that a
subscription and this configuration have drifted apart, so alert on it.

## Engine

| Flag | Default | Notes |
| --- | --- | --- |
| `-engine` | none | a bare origin, no `/api/v1` prefix; the bridge refuses a prefixed value |
| `-engine-timeout` | `30s` | per-submit ceiling |
| `-engine-workers` | `4` | concurrent engine submits. The lane never waits on the engine, so an engine stall cannot hold the delivery socket past the edge's write deadline |
| `-engine-queue` | `256` | deliveries queued behind the workers. A full queue sheds: the delivery is counted, logged and refused, and the host misses that object until its own catch-up finds it |
| `-engine-retries` | `4` | retries for a failed submit before the object is dropped; `-1` disables |

Non-zero `shed` means the engine cannot keep up.

## Headers

| Flag | Default | Notes |
| --- | --- | --- |
| `-header-anchor` | none | the header service for the initial anchor, for roots below it, and to re-anchor across a gap |
| `-header-anchor-timeout` | `5s` | this call sits on the lane's hot path |
| `-header-min-bits` | `0x207fffff` | the compact-target floor. A header declaring an easier target is rejected before its hash is compared |
| `-header-window` | `0` (unbounded) | retained height window |

`-header-min-bits` is the security-relevant one. A header carries the target it
claims to meet, so checking it only against itself is a check the sender writes
the answer to. The default is the trivial floor, right for a lab and wrong for
anything else; `0x1d00ffff` on mainnet.

`-header-anchor` is the one component that reaches outside the bridge, and it
has no default because a default would send a host's verification questions to
a third party. **The anchor must serve all three native routes**: `/v1/tip`,
`/v1/root/{height}` and `/v1/header/{hash}`. Without the third, a header whose
parent this host never saw is counted `orphaned` and dropped, which is the
state of every host that has just restarted; the lane looks healthy while
`overlay_bridge_header_tip_height` never moves. Another `overlay-bridge` serves
all three.

Re-anchoring resolves one parent per gap, not one per header. A root that came
from the anchor is booked as `fallback` and reported `known: false`, so it
does not flatter `overlay_bridge_tracker_roots_total{source="lane"}`, the
metric that says whether the lane rather than a third party is feeding
verification.

## Publishing

| Flag | Default | Notes |
| --- | --- | --- |
| `-edge-ingress` | none | slot inners in failover order, side A then side B, never hot-hot |
| `-edge-beef-port` | `8725` | appended when an entry carries no port |
| `-publish-source` | none | the slot inner the facade sources from |
| `-publish-queue` | `1024` | bounded; a full queue sheds and answers 503 |
| `-publish-prefer-after` | `5m` | fail back to the first address after this long away; `0` is sticky |

`-publish-source` is checked at startup against the machine's own addresses.
Publishing from an address this host does not carry defeats the fabric's
own-traffic exclusion, so our own objects come back to us: billable egress and
a full verification per object before the engine's duplicate check discards
it. That failure is silent, hence a startup check.

## Listeners

| Flag | Default | Notes |
| --- | --- | --- |
| `-facade-listen` | `[::]:9175` | the client-facing submit listener; empty disables it |
| `-headers-listen` | `[::]:9178` | the chain-tracker read API |
| `-metrics-addr` | `[::]:9179` | `/metrics`, `/healthz`, `/readyz`; empty disables it |
| `-stats-every` | `1m` | interval of the stats log line; `0` is off |

`-facade-listen` without `-edge-ingress` is refused: a facade with nowhere to
publish would admit submissions locally and silently never put them on the
plane. `/readyz` reports ready once every lane's listener is bound.

## Loop guard

| Flag | Default |
| --- | --- |
| `-guard-ttl` | `30m` |
| `-guard-entries` | `1048576` |

Both are bounds. Non-positive values take the defaults rather than disabling
the guard, because a guard that admits everything turns every re-submission
into a second publication.
