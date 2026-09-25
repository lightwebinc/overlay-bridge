# overlay-bridge

[![CI](https://github.com/lightwebinc/overlay-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/overlay-bridge/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/overlay-bridge/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/overlay-bridge/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/overlay-bridge)](https://github.com/lightwebinc/overlay-bridge/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/overlay-bridge.svg)](https://pkg.go.dev/github.com/lightwebinc/overlay-bridge)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project. See the main repository for the full architecture, design docs, and BRC specifications.

A landing-tier bridge that runs an **unmodified overlay services engine with a
multicast delivery fabric as its transport.** Objects delivered by the BEEF
object plane enter the engine through the submit interface it already serves,
publication goes out once and the network fans it out to every subscribed
host, and the block-header lane feeds the chain tracker the engine verifies
against. The design is Paper 008, _The overlay bridge_ (link under Papers).

```text
   delivery (down)
   fabric ══push══▶ overlay-bridge
       object lane ─▶ feed ─▶ POST /submit (x-topics: matched) ─▶ engine
       header lane ─▶ header store ◀── chain tracker reads ──── engine

   publication (up)
   client ─▶ facade (POST /submit) ─┬─▶ engine  (local admittance, real STEAK)
                                    └─▶ up-tunnel ══▶ fabric ══▶ every subscribed host

   engine: propagation switched off in configuration, so it admits and
           indexes but never re-submits to another host
```

It is the application-tier sibling of
[arcade-bridge](https://github.com/lightwebinc/arcade-bridge) and
[teranode-bridge](https://github.com/lightwebinc/teranode-bridge). Where those
bridges front the broadcaster tier and a mining Teranode cluster, this one
fronts an overlay host: a delivery consumer of the application tier, never a
miner, and nothing here carries miner entitlements. The shared landing-tier
machinery (lane termination, byte-order discipline, redial and failover across
the tunnel's primary and standby paths) is imported from teranode-bridge's
public packages, never forked.

## Why a bridge

An overlay host does two jobs when an object arrives. It **admits** it: verify
the BEEF, run the topic manager, index the outputs. Then it **propagates** it:
one HTTPS submit to every other host of the topic. The second job is the
expensive one. Every host that admits re-submits to every other, so between N
hosts one object crosses the network on the order of N squared times, fully
verified at every arrival before a duplicate check discards it, and a host
that was unreachable simply misses it.

This bridge replaces that second fan-out with one publication. Delivery arrives
as an ordinary submit on the interface the engine already serves; publication
goes out once and the network fans it out to every subscribed host. Every host
of the topic then hears the same objects at the same moment, each one carrying
its own proof and verified against headers that host received itself.

**No fork required.** The bridge imports no engine module and speaks only the
engine's published HTTP interfaces, in TypeScript or Go. Stop it, give the
engine back its propagation peers and its stock chain tracker, and you have a
stock overlay host again. The bridge holds no state of record.

## Planes

| Plane | Direction | What the bridge does |
| --- | --- | --- |
| Object lane (BRC-149) | down | splits delivery records, maps the matched topic identifier back to its name, submits to the engine |
| Header lane (BRC-135) | down | proof-of-work checks and chains bare headers, serves them to the engine as its chain tracker |
| Submit facade (BRC-22) | up | forwards to the local engine first, then publishes once onto the object plane |

Sovereign verification follows from the header lane: every object the host
admits is checked against headers the host itself received, from the same
network that delivered the object, with no third-party header service in the
loop and no rate limit to be subject to.

## Documentation

- [Architecture](docs/architecture.md): the feed, the header store, the facade and the loop guard, and what each holds
- [Configuration](docs/configuration.md): every flag, defaults, the modes, and the startup checks that refuse a half-configured host
- [The header feeder](docs/header-feeder.md): the header lane as a chain tracker, the two read shapes, anchoring and re-anchoring
- [Deliver once](docs/deliver-once.md): one delivery slot per landing site, and how a site that runs several bridges fans out locally
- [Contracts and open items](docs/contracts.md): what has been proven against the released engines, and what is not yet settled
- [BRC-148](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0148.md) and [BRC-149](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0149.md): the BEEF object plane and the delivery record the object lane carries
- [BRC-135](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0135.md): the block-header frame the header lane carries
- [BRC-22](https://github.com/bsv-blockchain/BRCs/blob/master/overlays/0022.md) and [BRC-24](https://github.com/bsv-blockchain/BRCs/blob/master/overlays/0024.md): the engine's submit and lookup interfaces

## Requirements

- Go 1.26 or later
- A released overlay services engine reachable as a bare origin
  ([ts-stack](https://github.com/bsv-blockchain/ts-stack) or
  [go-overlay-services](https://github.com/bsv-blockchain/go-overlay-services)),
  with its propagation switched off in configuration (no advertiser, or an
  empty SLAP tracker list) and its chain tracker pointed at the bridge
- A provisioned delivery slot carrying the object lane and the header lane
  (`-mode sink` needs only the slot)
- A header anchor serving `/v1/tip`, `/v1/root/{height}` and
  `/v1/header/{hash}`; another `overlay-bridge` serves all three, so a second
  host can anchor on the first

## Build

```bash
go build ./cmd/overlay-bridge
make verify        # what CI runs: formatting, vet, tests under -race, licence freshness
```

## Run

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
machine's own addresses. See [docs/configuration.md](docs/configuration.md)
for the full flag reference and the reasons behind each check.

## Default ports

| Port | Direction | Carries |
| --- | --- | --- |
| `9171` | in | object lane (BRC-149 delivery records); the edge dials the bridge |
| `9172` | in | header lane (bare BRC-135 headers) |
| `9175` | in | submit facade (`POST /submit`, the engine's own interface) |
| `9178` | in | chain-tracker read API (`/v1/tip`, `/v1/root/{height}`, `/v1/header/{hash}`) |
| `9179` | in | `/metrics`, `/healthz`, `/readyz` |
| `8725` | out | one publication per accepted submission, to the fabric's BEEF ingress |

The object and header lanes do not collide with the settlement lanes the
sibling bridges take, so a site that runs an overlay host beside another
bridge provisions one delivery slot and elects every lane it needs on it; see
[docs/deliver-once.md](docs/deliver-once.md).

## Observability

Prometheus series are `overlay_bridge_*` on `-metrics-addr` (default
`[::]:9179`), covering the lanes, the feed and its engine submits, the header
store, the facade, the publish queue and the loop guard. Two to alert on:
`unknown_topic` on the feed, the only signal that a subscription and this
configuration have drifted apart, and `shed` on the engine queue, which means
the engine cannot keep up. `overlay_bridge_tracker_roots_total{source="lane"}`
against `{source="fallback"}` is what shows verification is sovereign rather
than anchored on a third party. `/readyz` reports ready once every lane's
listener is bound.

## Layout

```
.
├── cmd/overlay-bridge/  # entrypoint: flags, modes, wiring, the startup checks
├── feed/                # object lane -> delivery records -> engine submits
├── headers/             # header lane -> proof-of-work-checked chain -> chain-tracker API
├── facade/              # the engine's submit interface, forwarded then published
├── uptunnel/            # one publication per submission to the fabric ingress, with failover
├── guard/               # the bounded loop guard on object identity
├── topics/              # elected topic names and their identifiers
├── docs/                # architecture, configuration, contracts, deliver-once, header-feeder
├── Dockerfile
├── Makefile
└── .github/workflows/{ci,codeql,image-publish,release,vuln}.yml
```

## Dependencies

- [`github.com/lightwebinc/teranode-bridge`](https://github.com/lightwebinc/teranode-bridge): the shared landing-tier packages (lane termination, failover)
- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common): the push object-frame codecs, including the BRC-149 delivery record
- [`github.com/bsv-blockchain/go-sdk`](https://github.com/bsv-blockchain/go-sdk): BEEF parsing and block-header hashing
- [`github.com/prometheus/client_golang`](https://github.com/prometheus/client_golang): metrics

The bridge links no overlay engine module. The two contracts it needs, the
submit request and its admittance response in both engines' wire forms, are
pinned by fixtures captured from the real engines.

## Status

Released (see the [releases](https://github.com/lightwebinc/overlay-bridge/releases))
and running on devnet: the bridge lands objects into a released engine, serves
headers from its own BRC-135 lane, and both reference hosts admit
object-for-object. What has been proven against the released engines, and what
remains open, is in [docs/contracts.md](docs/contracts.md).

## Papers

- _The overlay bridge: an unmodified engine on the object plane_ (Lightweb Inc., Paper 008): <https://1bsv.net/papers/overlay-bridge.pdf>
- _The overlay object plane: publish BEEF once, and every overlay hears_ (Lightweb Inc., Paper 006): <https://1bsv.net/papers/overlay-object-plane.pdf>
- _Arcade v2 on a multicast delivery fabric_ (Lightweb Inc., Paper 007), for the sibling bridge: <https://1bsv.net/papers/arcade-on-the-fabric.pdf>

## Licence

Apache-2.0 (see `LICENSE`). Third-party dependencies and the attribution their
licences require are in `NOTICE`. There is no licence check in this software,
no call home, and no metering of any kind inside it.
