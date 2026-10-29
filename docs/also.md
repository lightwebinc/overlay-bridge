# Also targets: a second local subscriber

One consumer slot per site, one crossing per object
([deliver once](deliver-once.md)). Sometimes a site runs two things that
want the same topic: an overlay host's engine that indexes a topic, and a
service beside it that keeps what the same topic carries. An also target
hands every object a route's primary is handed to that second local target,
from the same subscription.

```text
-engine-route 'tm_app_*=http://127.0.0.1:8280,also=http://127.0.0.1:8095'
```

The topics the glob names submit to the primary (`8280` above) as any route
does, and each object is also handed to `8095`'s `POST /submit`, in the
engine's own submit shape: the object as the body, `x-topics` naming the
topic. A route may name several also targets; each is independent.

## What a target is handed

- **On the feed:** every object the plane delivered under the route's topics
  and the bridge's SPV let through, whatever the primary answers. The target
  is a subscriber of its own; the primary refusing an object is not a reason
  to withhold it.
- **On the facade:** every submission the primary accepted, once. The plane
  does not deliver a host's own publications back to it (own-traffic
  exclusion), so this hand-off is the local target's delivery of what this
  host publishes. A submission the primary refused, or one the loop guard
  already knows, is not handed on.

## It never slows the primary

Each target has its own queue, bounded by count (`-also-queue`) and by bytes
(`-also-queue-bytes`, objects queued or in a worker's hands), drained by its
own workers (`-also-workers`). An offer is a non-blocking put: an object that
does not fit is shed and counted, never waited for. A target that answers a
4xx has refused the object on its merits and is not asked again; a 5xx or an
unreachable target is asked again a few times with a growing wait, then the
object is dropped and counted.

## It is not a shadow

A shadow (`shadow=`) is a second engine whose answers are compared with the
primary's, for a cutover. An also target is compared with nothing: it is a
consumer. Use a shadow to prove one engine against another, and an also
target to feed a second service from one subscription.

## Metrics

| Series | Labels | Meaning |
| --- | --- | --- |
| `overlay_bridge_also_total` | `route`, `target`, `result` = `ok`, `refused`, `error`, `shed` | Objects handed to an also target, by result. Preset at zero |
| `overlay_bridge_also_queue_bytes` | `route`, `target` | Bytes queued for, or in the hands of, the target's workers |
| `overlay_bridge_also_queue_bytes_capacity` | `route`, `target` | The byte bound (`-also-queue-bytes`) |
