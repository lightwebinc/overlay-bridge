# The header feeder

The bridge terminates a header lane into a local, proof-of-work-checked chain
and serves that chain to an engine as its chain tracker. The result is
sovereign verification: every object the host admits is checked against headers
the host itself received.

## Two read shapes, one store

| Shape | Routes | Consumer |
| --- | --- | --- |
| native | `GET /v1/tip`, `GET /v1/root/{height}`, `GET /v1/header/{hash}` | this repository's Go client in `headers/chainclient`, the bridge's own tooling, and any bridge anchoring off this one |
| chaintracks | `GET /chaintracks/v2/height`, `GET /chaintracks/v2/header/height/{height}` | a stock TypeScript host, which is pointed at a header service with one configuration call |

The native shape ships first and is what the bridge's own tests use. The
chaintracks shape is what makes "point the engine at the bridge" a single
configuration line rather than a patch.

`GET /v1/header/{hash}` is the odd one out: it is not there for an engine, it
is there for the bridge's own `-header-anchor`. A header whose parent this host
never saw cannot be given a height, so it is orphaned and dropped. Resolving
that parent by hash is what lets a restarted host, or one anchored below the
tip, chain the lane's tail instead of discarding it. Serving the route as well
as calling it is what removes the need for a separate header service in front
of a second host.

Three details of the chaintracks shape are load-bearing:

1. **The prefix carries the `/chaintracks` segment.** The client's own default
   is `/chaintracks/v2`, not `/v2`. Serving only `/v2` satisfies nothing. An
   operator who wants a different prefix sets it on both sides.
2. **`merkleRoot` and `hash` are compared as STRINGS** by the caller, which
   fetches the header for a height and compares its root against the one it
   holds. The rendering must therefore be the display hex exactly: no byte
   reversal, no `0x` prefix. Getting this wrong returns 200 and fails every
   comparison, which is the worst failure available here.
3. **An unknown height answers 404.** The client turns 404 into "no header" and
   any other non-ok status into a thrown exception, so the difference is the
   difference between "this proof cannot be checked yet" and a host-side
   incident.

## The height has one writer

The tracker reports the best height this host has reached, and the lane reader
is the only thing that advances it. A reader that chains a header without
teaching the tracker leaves an engine asking how far the chain has got being
told zero. The two are wired together in the command for that reason.

A root answered from the fallback does NOT advance the height: the height is a
claim about this host's own view of the chain, and a backfilled root from
elsewhere is not evidence of that. Without that rule a host with a dead lane
would report a healthy tip.

## Provenance is counted

`overlay_bridge_tracker_roots_total` splits answers by `lane` and `fallback`.
The claim the bridge makes is that verification is fed by the lane, and a run
in which every root was served by the fallback has not demonstrated it. Check
the split before believing the claim.
