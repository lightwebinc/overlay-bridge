# overlay-bridge Prometheus Metrics Reference

Every series is `overlay_bridge_*`, served on `-metrics-addr` (default `[::]:9179`)
at `/metrics` from a dedicated registry, so no Go runtime or process collectors
are exported. All values are read from component counters at scrape time.

## Metric types

| Type | Description |
|------|-------------|
| Counter | Monotonic; only increases or resets to zero on restart. Always suffixed `_total`. |
| Gauge | A value that can go up and down: a level, a height, or a state. |

## Endpoints

| Path | Purpose |
|------|---------|
| `/metrics` | Prometheus text exposition |
| `/healthz` | Liveness |
| `/readyz` | Ready once every lane's listener is bound |

With `-pprof-listen` set, `net/http/pprof` is served on that loopback address
(not on `-metrics-addr`): `/debug/pprof/profile?seconds=N` (CPU), `allocs`,
`heap`, `goroutine`, `block`, `mutex`, `threadcreate`, `trace`. See
[configuration](../configuration.md#listeners).

## Metrics

| Name | Type | Labels | Meaning |
|------|------|--------|---------|
| `overlay_bridge_build_info` | Gauge | `version`, `shard_common`, `go_sdk`, `go_version` | Always 1. Labels carry what the running binary links, read from embedded build info. |
| `overlay_bridge_feed_total` | Counter | `outcome` = `submitted`, `engine_error`, `parse_error`, `unknown_topic`, `rejected`, `sunk`, `shed`, `retried`, `engine_refused` (engine 4xx, not retried), `spv_refused` (enforce mode: not submitted) | Delivery records handled by the feed. |
| `overlay_bridge_verify_duration_seconds` | Histogram | `result` = `ok`, `refused`, `error` | One SPV verification by the bridge (go-sdk). Buckets 0.0001 to 0.1 s, as the engine's `overlay_host_verify_duration_seconds`. Present only with `-verify` on. See [SPV](../spv.md) |
| `overlay_bridge_verify_scripts_total` | Counter | | Input scripts the verifier ran |
| `overlay_bridge_verify_object_txs`, `_inputs`, `_scripts`, `_proofs`, `_ancestor_depth` | Histogram | none | Per-object work (R2), observed once per verified object: transactions, inputs across all of them, input scripts run, transactions carrying a merkle path, and the longest chain of in-object parents (1 = child of a proven parent). `_sum / _count` is the mean per object; divide a CPU rate by `rate(..._count)` for CPU per object, or by `rate(..._scripts_sum)` for CPU per script |
| `overlay_bridge_verify_mismatch_total` | Counter | `kind` = `bridge_ok_engine_refused`, `bridge_refused_engine_ok` | Shadow-mode disagreements with the engine. `bridge_ok_engine_refused` must be zero before enforce |
| `overlay_bridge_engine_submit_duration_seconds` | Histogram | `outcome` = `admitted`, `empty`, `error`, `refused`, `engine_error` | Lane arrival to the engine's answer, per topic submit |
| `overlay_bridge_shadow_total` | Counter | `route` (`default` = `-shadow-engine`, else the `-engine-route` glob); `kind` = `match`, `primary_only`, `shadow_only`, `differ`, `shadow_error`, `shadow_dropped` | Topic submits offered to a shadow engine, by comparison with the primary's answer. Preset at zero. Present only with a shadow configured. See [the shadow engine](../shadow.md) |
| `overlay_bridge_shadow_submit_duration_seconds` | Histogram | `route`; `result` = `ok`, `error` | One submit to a shadow engine |
| `overlay_bridge_shadow_queue_depth` | Gauge | `route` | Topic submits queued for a shadow engine right now. At `overlay_bridge_shadow_queue_capacity` the next offer is dropped (`shadow_dropped`) |
| `overlay_bridge_shadow_queue_capacity` | Gauge | `route` | Size of a shadow's queue (`-shadow-queue`) |
| `overlay_bridge_engine_queue_depth` | Gauge | none | Deliveries queued behind the engine workers right now. At `overlay_bridge_engine_queue_capacity` the next delivery is shed |
| `overlay_bridge_engine_queue_capacity` | Gauge | none | Size of the engine queue (`-engine-queue`) |
| `overlay_bridge_engine_queue_wait_seconds` | Histogram | none | Lane arrival to a worker taking the object off the queue: how long a delivery waited for a free worker. Buckets 10 us to 30 s |
| `overlay_bridge_engine_workers` | Gauge | none | Engine workers configured (`-engine-workers`) |
| `overlay_bridge_engine_workers_busy` | Gauge | `route` (`default` = `-engine`, else the `-engine-route` glob); `stage` = `verify`, `submit` | Workers busy right now: running the bridge's SPV (`verify`, under the route of the topic the object was delivered under) or waiting on that route's engine (`submit`, retries and backoff included). `overlay_bridge_engine_workers` minus the sum is idle. Preset at zero for every route and stage |
| `overlay_bridge_feed_shed_total` | Counter | `route` | Deliveries shed because the engine queue was full, by the route of the topic the plane delivered them under. Sums to `overlay_bridge_feed_total{outcome="shed"}`. Preset at zero |
| `overlay_bridge_engine_submits_total` | Counter | `topic`, `outcome` = `admitted`, `empty`, `error` | Engine submits by topic and admittance outcome. Preset at zero for every elected topic. |
| `overlay_bridge_headers_total` | Counter | `outcome` = `observed`, `rejected`, `orphaned`, `reanchored`, `replaced`, `pruned` | Headers seen on the header lane. |
| `overlay_bridge_header_tip_height` | Gauge | none | Best chained block height. Flat with a live lane is the outage signature. |
| `overlay_bridge_tracker_roots_total` | Counter | `source` = `lane`, `history`, `fallback`, `miss` | Chain-tracker roots answered, by source: `lane` = a header received on this host's lane; `history` = the anchor's answer for a height the lane could not have carried to this process (at or below the startup anchor, or pruned out of `-header-window`); `fallback` = the anchor's answer for a height above the startup anchor that the lane did not deliver; `miss` = no answer. Every source is present from startup. See [provenance](../header-feeder.md#provenance-is-counted) |
| `overlay_bridge_header_warm_heights` | Gauge | `state` = `target`, `loaded`, `failed` | Startup warm-up (`-header-warm`): heights it set out to load from the anchor, loaded, and not answered (those load on first use). `loaded / target` is its progress. |
| `overlay_bridge_header_warm_duration_seconds` | Gauge | none | Wall time of the warm-up; 0 when it is off. |
| `overlay_bridge_config_info` | Gauge | `verify`, `verify_network`, `verify_finality`, `engine_workers`, `gomaxprocs`, `gogc`, `gomemlimit` | Always 1. The run settings a measurement is stamped with (R7). |
| `overlay_bridge_facade_total` | Counter | `result` = `ok`, `malformed`, `unknown_topic`, `loop_dropped`, `engine_error`, `publish_failed` | Submit-facade submissions. |
| `overlay_bridge_publish_total` | Counter | `result` = `enqueued`, `sent`, `resent`, `shed`, `failed` | Up-tunnel publications. `sent` is confirmed by the ingress (acknowledged, and the connection stayed up through the grace window), never just written locally. `resent` counts records written again after the connection they were on died. `failed` counts records not written to either address and records lost after their resends. A shed record is not on the plane. |
| `overlay_bridge_publish_queue_depth` | Gauge | none | Instantaneous publish backlog. |
| `overlay_bridge_publish_unconfirmed` | Gauge | none | Records written to the up-tunnel and not yet confirmed. |
| `overlay_bridge_uptunnel_peer_closed_total` | Counter | none | Up-tunnel connections the ingress closed or that failed (EOF, reset, keepalive or user timeout). |
| `overlay_bridge_guard_entries` | Gauge | none | Loop-guard entries retained. |
| `overlay_bridge_lane_objects_total` | Counter | `lane` = `beef`, `header`; `outcome` = `objects`, `errors`, `rejected`, `bytes` | Objects (and bytes) read off a lane. |
| `overlay_bridge_lane_connections_active` | Gauge | `lane` | Connections open on a lane. |
| `overlay_bridge_lane_bound` | Gauge | `lane` | 1 when the lane's listener is open. |

Feed, header, facade and publish series appear only in the modes that run those
components (`-mode sink` emits lane and build series only).

## Alerting notes

- `overlay_bridge_feed_total{outcome="unknown_topic"}` rising: a subscription and
  this bridge's `-topics` have drifted apart.
- `overlay_bridge_feed_total{outcome="shed"}` rising: the engine cannot keep up.
  `overlay_bridge_feed_shed_total{route}` says which engine, and
  `overlay_bridge_engine_workers_busy{stage="submit"}` at the worker count with
  `overlay_bridge_engine_queue_depth` at its capacity is the signature: every
  worker waiting on an engine and nothing left to queue into.
- `overlay_bridge_tracker_roots_total{source="lane"}` versus `{source="fallback"}`
  shows whether verification is sovereign or anchored on a third party.
  `{source="history"}` is expected on every healthy host (proofs naming blocks
  mined before the bridge started) and is not part of that question; a
  sustained `fallback` rate is.
