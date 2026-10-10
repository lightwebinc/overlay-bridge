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

## Metrics

| Name | Type | Labels | Meaning |
|------|------|--------|---------|
| `overlay_bridge_build_info` | Gauge | `version`, `shard_common`, `go_sdk`, `go_version` | Always 1. Labels carry what the running binary links, read from embedded build info. |
| `overlay_bridge_feed_total` | Counter | `outcome` = `submitted`, `engine_error`, `parse_error`, `unknown_topic`, `rejected`, `sunk`, `shed`, `retried` | Delivery records handled by the feed. |
| `overlay_bridge_engine_submits_total` | Counter | `topic`, `outcome` = `admitted`, `empty`, `error` | Engine submits by topic and admittance outcome. Preset at zero for every elected topic. |
| `overlay_bridge_headers_total` | Counter | `outcome` = `observed`, `rejected`, `orphaned`, `reanchored`, `replaced`, `pruned` | Headers seen on the header lane. |
| `overlay_bridge_header_tip_height` | Gauge | none | Best chained block height. Flat with a live lane is the outage signature. |
| `overlay_bridge_tracker_roots_total` | Counter | `source` = `lane`, `fallback`, `miss` | Chain-tracker roots answered, by source. |
| `overlay_bridge_facade_total` | Counter | `result` = `ok`, `malformed`, `unknown_topic`, `loop_dropped`, `engine_error`, `publish_failed` | Submit-facade submissions. |
| `overlay_bridge_publish_total` | Counter | `result` = `enqueued`, `sent`, `shed`, `failed` | Up-tunnel publications. A shed record is not on the plane. |
| `overlay_bridge_publish_queue_depth` | Gauge | none | Instantaneous publish backlog. |
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
- `overlay_bridge_tracker_roots_total{source="lane"}` versus `{source="fallback"}`
  shows whether verification is sovereign or anchored on a third party.
