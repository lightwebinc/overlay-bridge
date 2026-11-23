# Configuration

Every setting is a flag, so a unit file or a Helm `args` list is the whole
configuration. The one exception is the subscription source token, which may
come from `OVERLAY_BRIDGE_SUBSCRIPTION_TOKEN` so that it need not sit on a
command line. `-version` prints the version and exits.

## Modes

| `-mode` | What runs | Use |
| --- | --- | --- |
| `sink` | lanes only: terminate, count, discard | burn-in a slot before an engine exists |
| `feed` | lanes plus engine submits | delivery only, no publishing |
| `all` | everything, including the facade and the up-tunnel | normal; the default |

`sink` needs no engine, anchor or topics. The other modes need all three, and
the bridge refuses to start without them rather than discovering at the first
delivered object that it has nowhere to put it. A sink given `-topics` counts
a delivery on one of them `sunk`; without `-topics` every delivery is counted
`unknown_topic` and logged, and the lane's own object and byte counters count
it either way.

## Lanes and topics

| Flag | Default | Notes |
| --- | --- | --- |
| `-beef-lane` | `[::]:9171` | the edge dials this, so the bridge listens |
| `-header-lane` | `[::]:9172` | as above |
| `-topics` | none | the elected topic names; the bridge holds the only name map. With `-topics-from engine`, only the names that stand in for an engine's list until it answers |
| `-topics-from` | empty | `engine`: carry the topics the engines carry, see below. Empty: the `-topics` list |
| `-topics-every` | `5s` | with `-topics-from engine`, how often each engine is asked (1 s at least) |
| `-topics-wait` | `30s` | with `-topics-from engine`, how long the bridge waits at start for its engines to answer before it opens its lanes |
| `-max-object` | codec default (64 MiB) | keep at or below the engine's own body limit |
| `-subscription-url`, `-subscription-consumer` | none | the base URL of a [subscription source](contracts.md#the-subscription-source) and the consumer's id there. When both are set, `-topics` is checked against that consumer's subscription at startup, and again at every change of topics, and a mismatch is logged as `TOPIC MISMATCH`. A source that cannot be read is a warning, not a refusal |
| `-subscription-token` | none | bearer token for that read; prefer the environment variable above |

A delivered record for a topic not in `-topics` is counted `unknown_topic`
with the identifier in the log line. That counter is the only signal that a
subscription and this configuration have drifted apart, so alert on it.

### Topics from the engine

With `-topics-from engine` the bridge carries what its engines carry, and a
topic added to or dropped from an engine needs no restart of the bridge and
no second list to keep in step. Every `-topics-every` it asks each engine it
submits to, `-engine` and the primary of every `-engine-route`, for its topic
managers: `GET /listTopicManagers`, the route overlay-express and the Go
engine both serve. A topic is carried when the engine its route sends it to
lists it; a topic one engine lists but routes elsewhere is not taken from
that engine.

A change is applied in order: the feed names deliveries of a new topic and
the facade takes its submissions before the edge is asked for it (topic
interest, above), and the edge is told of a topic gone, so it stops
delivering it. Nothing else restarts: the lanes and the edge's other topics
are not touched.

| Case | What the bridge does |
| --- | --- |
| an engine has not answered yet | at start, waits up to `-topics-wait`, then opens its lanes with the `-topics` names routed to that engine and logs `TOPICS NOT YET FROM THE ENGINE`; the engine's list replaces them when it answers |
| an engine stops answering | keeps the list it last gave, logs `TOPICS NOT FOLLOWED` once and exports `overlay_bridge_topics_engine_up` 0. A topic is dropped only when an engine that answers no longer lists it, never because an engine is down |
| a primary answers 404, 405 or 501 | it does not list its topics (a storage provider): its topics are the `-topics` names routed to it and every topic a listing engine lists that is routed to it, so a host whose engine carries a storage topic routed to its provider names it once. With neither, `TOPICS NOT FOLLOWED` says nothing is delivered for it |
| an answer that is not a JSON object of topic names to metadata objects, or names none | a failure, as above; never an empty list. A 404 from an engine that has listed before is a failure too, never a provider |

The bridge keeps nothing on disk for this: what each engine listed is asked
again every `-topics-every`, with the ETag the engine gave, so an unchanged
list costs a `304`. `overlay_bridge_topics` is the number of topics carried
now and `overlay_bridge_topics_changes_total` counts the changes.

## Topic interest

Optional. With `-interest-upstream` the bridge tells its delivery edge which
topics `-topics` needs, and logs the edge's answer for each topic by name.
The protocol is a draft and its wire may still change.

| Flag | Default | Meaning |
|---|---|---|
| `-interest-upstream` | none | where to announce: the delivery edges the consumer's tunnel reaches, or a relay, as a comma list of `[host]:port`. Every one is told every topic. Empty turns the announcer off |
| `-interest-bind` | `[::]:0` | the local address to announce from. The edge identifies the consumer by it, so on a host with several addresses name the consumer's own tunnel address. A named upstream resolves in this address's family, and one of the other family is refused at startup |
| `-interest-label` | `overlay-bridge` | distinguishes this bridge from any other announcer on the same host. Two bridges on one host need different labels |
| `-interest-lifetime` | `0` | the lifetime to ask for, in whole seconds. `0` asks for the edge's default |
| `-interest-withdraw-on-exit` | `false` | withdraw on exit, so the edge stops delivering announced topics at once. Leave it off for a bridge that restarts: what it announced stays for its lifetime |
| `-interest-deliver-to` | none | an `address:port` named with every topic as where its delivery records should arrive: this bridge's `-beef-lane`, as the hop above reaches it. For a bridge behind a distributing relay (below). Needs `-interest-upstream`; a port `-beef-lane` does not listen on is reported at startup |

What the edge answers, and what the bridge does with it:

| Answer | Meaning | In the log |
|---|---|---|
| served | the topic is in the consumer's election, or the account's interest quota covers it | `topic served by the edge` |
| outside the bound | not in the election, and the account has no interest quota | `TOPIC NOT DELIVERED`, with the fix |
| over the cap | the interest quota is used up | `TOPIC NOT DELIVERED`; asked again at every refresh |
| not served | the consumer's tunnel does not deliver the topic's group; a relay that knows no path it announces to delivers it says `unserved` | `TOPIC NOT DELIVERED` |
| address not honored | the topic is held, but the hop above ignores `-interest-deliver-to` and delivers as provisioned. An edge answers so, and so does a relay that does not distribute | `topic served by the edge`, and once `TOPIC INTEREST: the hop above does not honor -interest-deliver-to` |
| bad address | a distributing relay refused the address `-interest-deliver-to` names: it is neither the address the bridge announces from nor inside the prefix the relay allows it to announce from. The topic is not held | `TOPIC NOT DELIVERED`, with the fix |

### Behind a distributing relay

A host deep inside a consumer's network has no lane of its own at the edge.
The consumer's lane can end at a relay that distributes: it writes each
topic's records on to the hosts that asked for it, at the address each
names. The bridge asks for its records with
`-interest-deliver-to` set to its own lane, and announces to that relay:

```sh
overlay-bridge -topics tm_example \
  -beef-lane '[::]:9171' \
  -interest-upstream '[2001:db8:c:1::1]:8729' \
  -interest-bind '[2001:db8:c:1::10]:0' \
  -interest-deliver-to '[2001:db8:c:1::10]:9171'
```

The relay honors the address only if it is the address the bridge
announces from or lies inside the `-allow` prefix the bridge announces
from, so name an address of this host. Delivery from the relay is at most
once, as on the lane: a bridge that falls behind or restarts loses what the
relay shed meanwhile.

The announcer is not on the data path. When it is in trouble the bridge
keeps delivering, and says so:

| Trouble | In the log |
|---|---|
| its socket cannot be opened, for example because `-interest-bind` is not yet an address of the host, or the socket fails | `TOPIC INTEREST STOPPED`, then tried again |
| sends fail | `TOPIC INTEREST: cannot send to the edge`, at most once a minute with the number that failed |
| no edge answers | `TOPIC INTEREST: no edge answers`, noticed at the next exchange and at the latest at the next refresh; `topic interest: the edge answers` when one does again. Meanwhile each topic keeps the last answer it had |
| one edge is silent while another answers | one line at Info, not a warning. A standby edge stays silent until delivery moves to it, and the edge that delivered goes silent once delivery has moved away, so this is expected whenever `-interest-upstream` names both edges |

This is the same fact the startup reconciliation reports, from the edge
itself and for as long as the bridge runs: a topic the election stops
covering is named in the log when it happens, not at the next restart. The
two checks are independent and either can be used alone.

Metrics: `overlay_bridge_interest_topics{status}`,
`overlay_bridge_interest_upstream{upstream,state}` and
`overlay_bridge_interest_events_total{upstream,event}`. A non-zero
`overlay_bridge_interest_topics` for any status but `ok` and `pending` is a
topic whose objects are not arriving. An address not honored counts as
`ok`, and `bad_address` is exported only by a bridge that names an address.

## Engine

| Flag | Default | Notes |
| --- | --- | --- |
| `-engine` | none | a bare origin, no `/api/v1` prefix; the bridge refuses a prefixed value |
| `-engine-timeout` | `30s` | per-submit ceiling |
| `-engine-workers` | `4` | concurrent engine submits. The lane never waits on the engine, so an engine stall cannot hold the delivery socket past the edge's write deadline |
| `-engine-queue` | `256` | deliveries queued behind the workers. A full queue sheds: the delivery is counted, logged and refused, and the host misses that object until its own catch-up finds it |
| `-engine-queue-bytes` | `0` | also bound the queue by the bytes of the objects in it (0: by `-engine-queue` alone). Memory is what a queue costs, so a deep `-engine-queue` with a byte bound rides out a burst of small objects and stays bounded for large ones |
| `-stop-drain` | `30s` | at stop (SIGTERM), how long the workers keep submitting the objects already queued before the bridge exits. The lane delivered them, so nothing upstream sends them again; a restart without the drain dropped every one of them. The service's stop timeout must exceed it |
| `-engine-retries` | `4` | retries for a failed submit before the object is dropped; `-1` disables, `0` takes the default |

Every engine client (the primary, routes, shadows, the facade) shares one
connection pool that keeps up to 256 idle connections per engine, so the
workers reuse their connections however many there are (Go's default of 2
closed every connection above the second after each answer).

Non-zero `shed` means the engine cannot keep up. An engine that answers 4xx
refused the object on its merits; that is counted as `engine_refused` and is
not retried.

## Shadow engine

| Flag | Default | Notes |
| --- | --- | --- |
| `-shadow-engine` | (none) | a second submit base, a bare origin like `-engine` and not the same one, handed every object the primary is handed and compared per topic. See [the shadow engine](shadow.md) |
| `-shadow-workers` | `2` | concurrent shadow submits |
| `-shadow-queue` | `1024` | objects queued for the shadow. A full queue drops the offer (`shadow_dropped`); it never blocks the primary |
| `-shadow-facade` | off | also offer the facade's accepted submissions to the topic's shadow, with the local engine's answer to compare against, so a host that publishes through its own facade counts parity on what it publishes. A submission the engine failed is not offered |

The shadow uses `-engine-timeout` for its submits and makes no retries.

## Engine routes

| Flag | Default | Notes |
| --- | --- | --- |
| `-engine-route` | (none) | `GLOB=PRIMARY[,shadow=SHADOW][,also=TARGET]...`: the topics the glob names submit to `PRIMARY`, with `SHADOW` as their shadow, instead of `-engine` and `-shadow-engine`, and each object is also handed to every `TARGET`. Repeatable; the first matching route wins. All are bare origins like `-engine`; the shadow may not be the primary, and an also target may be neither. See [routes](shadow.md#routes) and [also targets](also.md) |
| `-also-workers` | `2` | concurrent submits to each also target |
| `-also-queue` | `1024` | objects queued for each also target. A full queue sheds the object (`shed`); it never blocks the primary |
| `-also-queue-bytes` | `268435456` | bytes queued for, or in the hands of, each also target's workers. Past it the next object is shed |

A route moves one application's topics to another engine while every other
topic stays where it is: the shape of a per-application cutover, with the
engine being replaced shadowing its replacement on those topics. A route
with no `shadow=` has no shadow for its topics (the default shadow is not
offered them). Each route's shadow has its own queue and workers, sized by
`-shadow-workers` and `-shadow-queue`, and its own `route` label on the
shadow metrics. The feed and the facade route alike.

## SPV verification

| Flag | Default | Notes |
| --- | --- | --- |
| `-verify` | `off` | `off`, `shadow` (verify and compare, submit as before) or `enforce` (do not submit a refused object; mark a passing one). See [SPV verification](spv.md) |
| `-verify-network` | `main` | activation heights scripts are judged under: `main`, `test`, or `all` (every rule from block 0, for a regtest-style chain) |
| `-verify-finality` | `ignore` | finality policy for unmined transactions: `ignore` (the overlay plane; committed-record carriers are non-final by design) or `require` (bitcoin-sv's rule). See [SPV verification](spv.md) |
| `-verify-marker-key` | (none) | file holding the trust-marker key, mode 0600, at least 32 bytes; **required** for `enforce`, and startup fails without it |
| `-verify-timeout` | `5s` | ceiling on one verification; past it the object goes to the engine unmarked |
| `-sig-verifier` | `decred` | the secp256k1 ECDSA verifier behind go-sdk's hooks: `decred` (`pkg/secpverify`, decred's secp256k1, the same verdicts, about a quarter less per signature) or `sdk` (go-sdk's built-in path). See [SPV verification](spv.md), "The ECDSA verifier" |

## Headers

| Flag | Default | Notes |
| --- | --- | --- |
| `-header-anchor` | none | the header service for the initial anchor, for roots below it, and to re-anchor across a gap |
| `-header-anchor-timeout` | `5s` | this call sits on the lane's hot path |
| `-header-min-bits` | `0x207fffff` | the compact-target floor. A header declaring an easier target is rejected before its hash is compared |
| `-header-window` | `0` (unbounded) | retained height window |
| `-header-warm` | `2016` | heights ending at the anchor whose roots are loaded from the anchor at startup, before the lanes open; `0` = off |
| `-header-warm-workers` | `8` | concurrent anchor reads during the warm-up |
| `-header-warm-timeout` | `1m` | ceiling on the warm-up; the bridge then starts with what it loaded |

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

**Warm-up.** The lane is a live tail, so every height at or below the anchor
is answered by the anchor. Without warm-up the first verification naming such
a height paid that round trip on the verify path (live: verify p99 21 ms
against a p50 of 0.66 ms). `-header-warm` loads the most recent heights first,
in parallel, before the lanes and the read API open; readiness follows the
lanes, so `/readyz` turns ready only after it. It never fails startup: a height
not loaded in time, or not answered, is fetched on first use. Warmed roots are
the anchor's answers for heights the lane could not have carried, and are
booked as `history` when read. 2016 heights is two weeks of mainnet blocks; a
proof older than the warmed range costs one round trip on first use, then is
cached.

Re-anchoring resolves one parent per gap, not one per header. A root that came
from the anchor is reported `known: false` and booked as `history` at or below
the startup anchor, `fallback` above it, so it does not flatter
`overlay_bridge_tracker_roots_total{source="lane"}`, the metric that says
whether the lane rather than a third party is feeding verification. Only
`fallback` means the lane missed a height it should have carried; see
[provenance](header-feeder.md#provenance-is-counted).

## Publishing

| Flag | Default | Notes |
| --- | --- | --- |
| `-edge-ingress` | none | slot inners in failover order, side A then side B, never hot-hot |
| `-edge-beef-port` | `8725` | appended when an entry carries no port |
| `-publish-source` | none | the slot inner the facade sources from |
| `-publish-lane` | none | `SOURCE=HOSTS`, repeatable: one more lane, for a consumer with several tunnels. `SOURCE` is that tunnel's slot inner and `HOSTS` its ingress hosts in failover order. Needs `-edge-ingress` and `-publish-source`, which are the first lane |
| `-publish-queue` | `1024` | records queued for the up-tunnel. A full queue holds the submission for `-publish-wait`, then sheds it and answers 503 |
| `-publish-queue-bytes` | `268435456` | also bound the queue by the bytes of the records in it (0: by `-publish-queue` alone). Memory is what a queue costs: by count alone, a thousand 4 MiB pieces sit in it. A record is admitted when it fits under the bound or when the queue is empty, so one larger than the bound is carried alone rather than refused for its size |
| `-publish-wait` | `2s` | how long a submission waits for room in a full queue before it is shed and answered 503; `0` sheds at once. A drain that falls behind for a moment then shows as `POST /submit` latency rather than as refusals a client treats as failed puts. Keep it under the client's own request timeout |
| `-publish-prefer-after` | `5m` | fail back to the first address after this long away; `0` is sticky |

`-publish-source` is checked at startup against the machine's own addresses.
Publishing from an address this host does not carry defeats the fabric's
own-traffic exclusion, so our own objects come back to us: delivery bandwidth and
a full verification per object before the engine's duplicate check discards
it. That failure is silent, hence a startup check.

`overlay_bridge_publish_queue_bytes` against its `_capacity`, and
`overlay_bridge_publish_total{result="waited"}`, show a queue at its bound
before anything is shed.

### Several lanes

One lane is one connection, and the fabric numbers the records of one source
address in one group as one flow: one sequence space, carried and read as one
stream at every hop. A consumer with several tunnels has several inner
addresses, and with a `-publish-lane` per tunnel the facade publishes over all
of them:

```
  -edge-ingress   '2001:db8:59::a,2001:db8:59::b' -publish-source '2001:db8:59::10' \
  -publish-lane   '2001:db8:5a::10=2001:db8:5a::a,2001:db8:5a::b'
```

- **Each lane is its own flow.** Every lane has a source address of its own
  (two lanes on one address are refused at startup: two connections from one
  address interleave one flow's sequence numbers, which reads downstream as
  loss).
- **A record goes whole to one lane**, the one with the fewest bytes queued,
  the lanes taking turns when they are level. A lane that drains slower is
  offered less, and a lane with no room for the record hands it to another
  that has: a publisher waits, and a record is shed, only when no lane in
  service has room.
- **A lane that fails is passed over.** After a lane fails a record (its
  ingress unreachable, a record lost after its resends) it gets no more for two
  seconds while another lane is healthy, so a dead tunnel sheds its share onto
  the others.
- **Each lane has the queue bounds above**, so the queue series are the lanes
  summed. `overlay_bridge_publish_lane_sent_total{lane}` and
  `overlay_bridge_publish_lane_queue_bytes{lane}` show each lane, named by its
  source address.

Records published to one topic are independent objects and the plane promises
no order between them, with one lane or several. Lanes add flows, not
capacity on a link they share: where two lanes' flows meet on one port again
downstream, that port is still one port.

## Listeners

| Flag | Default | Notes |
| --- | --- | --- |
| `-facade-listen` | `[::]:9175` | the client-facing submit listener; empty disables it |
| `-facade-trusted` | (none) | `GLOB=CIDR[,CIDR]`: a submission on a topic the glob names, from a source in one of the CIDRs, is published without the local route (no engine, no shadow, no also target, no check) and answered as an admitted object. For a storage node's own gateway, whose provider already holds every piece in process. Repeatable; needs `-mode all` and `-facade-listen`, and the bridge refuses to start otherwise. List only this host's own addresses |
| `-headers-listen` | `[::]:9178` | the chain-tracker read API, on one address or a comma list of them; empty disables it |
| `-metrics-addr` | `[::]:9179` | `/metrics`, `/healthz`, `/readyz`; empty disables it |
| `-stats-every` | `1m` | interval of the stats log line; `0` is off |
| `-pprof-listen` | (none) | a loopback `host:port` (`127.0.0.1`, `::1` or `localhost`) serving `net/http/pprof` under `/debug/pprof/`; empty is off |

`-facade-listen` without `-edge-ingress` is refused: a facade with nowhere to
publish would admit submissions locally and silently never put them on the
plane. `/readyz` reports ready once every lane's listener is bound.

`-pprof-listen` is refused on any non-loopback address: profiles carry code
paths and memory contents and the endpoint has no authentication, so whatever
collects them (a continuous profiler's agent, or `go tool pprof` over ssh)
runs on the same machine. While it is set, block profiling records every
blocking event of 10 us or more and a shorter one in proportion to its
length, and mutex profiling records one contention event in 10: the
low-overhead rates continuous profilers use. Unset, the runtime records
neither. A listener that cannot open is logged and the bridge carries on.

## Loop guard

| Flag | Default |
| --- | --- |
| `-guard-ttl` | `30m` |
| `-guard-entries` | `1048576` |

Both are bounds. Non-positive values take the defaults rather than disabling
the guard, because a guard that admits everything turns every re-submission
into a second publication.

## Examples

```bash
# Sink: terminate the lanes, count, discard. Burn in a delivery slot before
# an engine exists.
./overlay-bridge -mode sink

# Feed: deliver into a released engine, with headers served from the lane.
./overlay-bridge -mode feed \
  -engine          'http://127.0.0.1:8080' \
  -topics          'tm_example' \
  -header-anchor   'http://192.0.2.10:9178' \
  -header-min-bits 0x1d00ffff

# All: also serve the submit facade and publish once onto the plane.
./overlay-bridge -mode all \
  -engine          'http://127.0.0.1:8080' \
  -topics          'tm_example,tm_other' \
  -header-anchor   'http://192.0.2.10:9178' \
  -header-min-bits 0x1d00ffff \
  -edge-ingress    '2001:db8:59::a,2001:db8:59::b' \
  -publish-source  '2001:db8:59::10'
```

`-header-min-bits` is the compact-target floor of the network the host is on
(`0x1d00ffff` on mainnet); the default is the trivial floor, right for a lab
and wrong for anything else. `-edge-ingress` lists the slot's inner addresses
in failover order, and `-publish-source` is checked at startup against the
machine's own addresses. The sections above give the reasons behind each check.


## Default ports

| Port | Direction | Carries |
| --- | --- | --- |
| `9171` | in | object lane (BRC-149 delivery records); the edge dials the bridge |
| `9172` | in | header lane (bare BRC-135 headers) |
| `9175` | in | submit facade (`POST /submit`, the engine's own interface) |
| `9178` | in | chain-tracker read API (`/v1/tip`, `/v1/root/{height}`, `/v1/header/{hash}`) |
| `9179` | in | `/metrics`, `/healthz`, `/readyz` |
| `8725` | out | one publication per accepted submission, to the fabric's BEEF ingress |

Lane ports follow the fabric-wide
[lane numbering](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/lane-numbering.md);
one delivery slot serves every bridge at a site
([deliver once](deliver-once.md)).
