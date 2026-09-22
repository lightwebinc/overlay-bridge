# Configuration

The bridge is configured entirely by flags. Nothing is read from the
environment and nothing is baked into the image, so a unit file or a Helm
`args` list is the whole configuration.

## Modes

| `-mode` | What runs | Use |
| --- | --- | --- |
| `sink` | lanes only: terminate, count, discard | burn-in a slot before an engine exists |
| `feed` | lanes plus engine submits | delivery only, no publishing |
| `all` | everything, including the facade and the up-tunnel | normal |

`sink` requires no engine, no anchor and no topics. Every other mode requires
all three, and the bridge refuses to start without them rather than discovering
at the first delivered object that it has nowhere to put it.

## Lanes

| Flag | Default | Notes |
| --- | --- | --- |
| `-beef-lane` | `[::]:9171` | the edge DIALS this, so the bridge listens |
| `-header-lane` | `[::]:9172` | as above |
| `-topics` | none | the elected topic names; the bridge holds the only name map |
| `-max-object` | codec default (64 MiB) | keep at or below the engine's own body limit |

A delivered record for a topic that is not in `-topics` is counted as
`unknown_topic` with the identifier in the log line. That counter is the only
signal that a subscription and this configuration have drifted apart, so it is
worth an alert.

## Engine

| Flag | Default | Notes |
| --- | --- | --- |
| `-engine` | none | a BARE origin. No `/api/v1` prefix: the TypeScript host mounts submit on the app root and cannot be given a base path. The bridge refuses a prefixed value |
| `-engine-timeout` | `30s` | per-submit ceiling |

## Headers

| Flag | Default | Notes |
| --- | --- | --- |
| `-header-anchor` | none | the header service used for the initial anchor, for roots below it, and to re-anchor across a gap |
| `-header-anchor-timeout` | `5s` | bounded; this call sits on the lane's hot path |
| `-header-min-bits` | `0x207fffff` | the compact-target FLOOR. A header declaring an easier target is rejected before its hash is compared |
| `-header-window` | `0` (unbounded) | retained height window |

`-header-min-bits` is the security-relevant one. A header carries the target it
claims to meet, so checking it only against itself is a check the sender writes
the answer to. Set this from the network the host is actually on. The default
is the trivial floor, which is right for a lab and wrong for anything else.

`-header-anchor` is the one component that reaches outside the bridge. It is
deliberately not defaulted: a default would quietly send a host's verification
questions to a third party.

## Publishing

| Flag | Default | Notes |
| --- | --- | --- |
| `-edge-ingress` | none | slot inners in FAILOVER order, side A then side B, never hot-hot |
| `-edge-beef-port` | `8725` | appended when an entry carries no port |
| `-publish-source` | none | the slot inner the facade sources from |
| `-publish-queue` | `1024` | bounded; a full queue sheds and says so |
| `-publish-prefer-after` | `5m` | fail back to the first address after this long away; `0` is sticky |

`-publish-source` is asserted at startup against the machine's own addresses.
Publishing from an address this host does not carry means the fabric's
own-traffic exclusion never matches, so our own objects are delivered back to
us: billable egress plus a full verify per object inside the engine before its
duplicate check discards it. That failure is silent, which is why it is a
startup check rather than something to notice later.

## Listeners

| Flag | Default | Notes |
| --- | --- | --- |
| `-facade-listen` | `[::]:9175` | ONE client-facing submit listener; empty disables it |
| `-headers-listen` | `[::]:9178` | the chain-tracker read API; comma-separated addresses |
| `-metrics-addr` | `[::]:9179` | `/metrics`, `/healthz`, `/readyz`; empty disables it |

Setting `-facade-listen` without `-edge-ingress` is refused: a facade with
nowhere to publish would accept submissions, admit them locally, and silently
never put them on the plane.

`/readyz` gates on every lane's listener being bound. Reporting ready before
then invites an edge to dial a socket that is not there.

## Loop guard

| Flag | Default |
| --- | --- |
| `-guard-ttl` | `30m` |
| `-guard-entries` | `1048576` |

Both are bounds rather than targets. Non-positive values take the defaults
instead of disabling the guard, because a guard that admits everything turns
every re-submission into a second publication.
