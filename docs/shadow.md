# The shadow engine

The bridge can feed two engines from one lane: the primary (`-engine`),
whose answers are the bridge's answers, and a shadow (`-shadow-engine`),
which is handed every object the primary is handed and whose answers are
only compared. The point is to prove a second engine on real traffic before
it carries anything: an engine being replaced and its replacement see the
same objects, in the same order per lane, with the same trust marker, and
every disagreement is a counter and a log line rather than a surprise.

Delivery is the plane's: one lane, one delivery, two submits. There is no
synchronization path between the engines. An engine that misses an object
(it was down, or its submit was dropped) catches up the way it would after
any outage, from its own catch-up protocol; the bridge does not replay for
it.

## What the shadow is handed

For each topic submit the feed makes to the primary, once the primary has
answered (or failed through every retry, or refused), the same submit is
offered to the shadow:

- the same object bytes;
- the same topic in `x-topics`;
- the same SPV trust marker (`X-Overlay-Bridge-Verified`), when enforce
  mode produced one, so a shadow configured to honor it skips scripts too.
  An engine that does not know the header ignores it.

The shadow's submit is made once, with no retry, under the primary's
`-engine-timeout`. Objects clients submit through the facade are not
offered: they are not lane traffic. Lookups are not teed; whether the two
engines answer questions alike is the application's own check.

## The shadow can never slow the primary

The offer is a non-blocking put on a bounded queue (`-shadow-queue`),
drained by the shadow's own workers (`-shadow-workers`). The primary's
submit, its retries, its answer and its books are complete before the offer
is made, and the offer returns at once whether or not the queue has room.
A shadow that is down, slow or wrong therefore costs the primary nothing:

- a shadow that is unreachable, answers 4xx or 5xx, or answers without an
  entry for the topic counts `shadow_error`;
- a full queue counts `shadow_dropped` and the object is never offered; a
  queue whose workers are not running counts the same;
- a shadow that takes the whole timeout holds one of its own workers, not
  one of the primary's.

## The comparison

Per topic submit, the primary's admittance instructions for the topic are
compared with the shadow's as sets: the outputs admitted and the coins
retained, order ignored. `overlay_bridge_shadow_total{kind}` counts each:

| Kind | Meaning |
| --- | --- |
| `match` | the same outputs and the same coins. Both admitting nothing is a match: it is the duplicate answer, and the expected steady state on every reconnect and converge |
| `primary_only` | the primary admitted or retained something and the shadow nothing |
| `shadow_only` | the shadow admitted or retained something and the primary nothing |
| `differ` | both answered something, and not the same thing |
| `shadow_error` | the shadow gave no answer to compare (unreachable, refused, failed, or no entry for the topic) |
| `shadow_dropped` | never offered: the queue was full or not running |

A primary that gave no answer (refused, or unreachable through every retry)
is compared as having admitted nothing, so a shadow that admits the object
reads `shadow_only`. Every series is preset at zero when a shadow is
configured, so a shadow that has never matched shows a zero `match` series,
not no series.

The first twenty mismatches (every kind but `match`) are logged with the
object's txid (an Atomic BEEF's subject; otherwise the object's content
identifier), the topic, the kind and both answers. After that, counting goes
on and logging stops: a systematic disagreement would otherwise write one
line per object.

## Metrics

| Series | Meaning |
| --- | --- |
| `overlay_bridge_shadow_total{kind}` | Topic submits offered to the shadow, by comparison kind, as above |
| `overlay_bridge_shadow_submit_duration_seconds{result}` | One submit to the shadow, its whole submit included; `result` is `ok` (answered 200) or `error`. Comparable with `overlay_bridge_engine_submit_duration_seconds` for the primary, which also includes queueing and the bridge's verification |

A parity run is read from two ratios over the same window: `match` against
every kind but `shadow_error` and `shadow_dropped` (what the engines
disagreed on), and `shadow_error` plus `shadow_dropped` against the total
(what the shadow never saw). The second must be understood before the first
means anything.
