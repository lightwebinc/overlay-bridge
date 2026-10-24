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

`overlay_bridge_tracker_roots_total` books every root the store answers by
where the answer came from:

| `source` | Answered by | Heights |
| --- | --- | --- |
| `lane` | a header this host received on its own lane | any |
| `history` | the anchor (`-header-anchor`) | at or below the startup anchor, and any height pruned out of `-header-window` |
| `fallback` | the anchor | above the startup anchor: a height the lane did not deliver (a gap, a re-anchored parent, a lane that stopped) |
| `miss` | nobody | any |

The claim the bridge makes is that verification is fed by the lane, and only
`fallback` speaks against it. The lane is a live tail from the startup anchor,
so no height at or below the anchor could have come down this process's lane,
yet nearly every object carries an ancestor mined before the bridge started
and its proof names such a height. Those answers are `history`: expected on
every healthy host, and silent about the lane. A `fallback` answer is a height
the lane should have carried and did not, which is the posture change worth an
alert. Check `fallback` against `lane` before believing the claim.

The split is by height alone, decided in one comparison:

- A re-anchored header keeps the booking of its height. Above the startup
  anchor it stays `fallback` until the lane delivers that header itself, and
  from then on it is `lane`.
- A header the lane delivers is `lane` at any height, including one at or
  below the startup anchor that a reorg made canonical.
- A re-anchored header at or below the startup anchor is `history`. The lane
  could have carried one only in a reorg crossing the startup point, and
  `overlay_bridge_headers_total{outcome="reanchored"}` still counts it.
- The boundary is fixed at startup and only rises, with the retention
  window's floor. A restart moves it up to the new anchor, so heights the
  previous process received on its lane become `history` for the next one.

`known` on the read API does not follow this split: it is true only for a
header received on the lane, and both `history` and `fallback` answer
`known: false`.

## Not built: keeping the lane's headers across a restart

Every restart turns the lane's own record into anchor answers: the store
starts empty at the anchor's tip, and every height the previous process
received becomes `history` (and `known: false`). Keeping received headers in a
local state directory would stop that. The design, which is not implemented:

- **What is kept.** Only headers received on the lane, never re-anchored ones,
  as the raw 80-byte headers with their heights, appended as they chain and
  bounded like the store (the retention window, or the warm-up depth). Raw
  headers, not roots, so the next process checks them itself rather than
  trusting its own disk. A write is an append off the lane's hot path; losing
  the last few on a crash only means those heights are `history` once.
- **Proof of work on load.** Every kept header is checked against
  `-header-min-bits` and must link to its predecessor by hash. A file that
  fails either is discarded whole, counted and logged, and the bridge starts
  as it does now.
- **Linkage to the anchor.** Heights on disk are claims. The kept chain is
  accepted only up to a header the anchor confirms at the same height
  (`/v1/header/{hash}`): one anchor call at startup. Everything below a
  confirmed header is linked to it by hash, so the kept chain is the anchor's
  chain without trusting the anchor for more than the startup tip it is
  already trusted for.
- **A reorg while the bridge was down.** If the anchor does not confirm the
  kept tip, walk back to the highest kept header it does confirm, within a
  bound, and drop what lies above it: that branch lost. If none is confirmed
  within the bound (a deep reorg, or a chain reset), drop the file.
- **What it books.** A restored header is one this host received on its lane,
  so it answers as `lane` and `known: true`. The heights mined while the
  bridge was down stay `history`: the lane did not carry them to this host.
  The warm-up then only needs the heights the kept chain does not cover.
