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

## Documentation

- [Architecture](docs/architecture.md): the feed, the header store, the facade and the loop guard, and what each holds
- [Configuration](docs/configuration.md): every flag, defaults, the modes, and the startup checks that refuse a half-configured host
- [The header feeder](docs/header-feeder.md): the header lane as a chain tracker, the two read shapes, anchoring and re-anchoring
- [Deliver once](docs/deliver-once.md): one delivery slot per landing site, and how a site that runs several bridges fans out locally
- [Metrics reference](docs/references/prometheusMetrics.md): every `overlay_bridge_*` series, its labels and meaning
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

## Quick start

```bash
# Sink: terminate the lanes, count, discard. Burn in a delivery slot before
# an engine exists.
./overlay-bridge -mode sink
```

Feed and full (facade plus publish) examples, every flag and the default ports
are in [docs/configuration.md](docs/configuration.md).

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
├── docs/                # architecture, configuration, contracts, deliver-once, header-feeder, references/
├── Dockerfile
├── Makefile
└── .github/workflows/{ci,codeql,image-publish,release,vuln}.yml
```

## Observability

Prometheus series are `overlay_bridge_*` on `-metrics-addr` (default `[::]:9179`);
see the [metrics reference](docs/references/prometheusMetrics.md).

## Status

Released (see the [releases](https://github.com/lightwebinc/overlay-bridge/releases))
and running in a lab deployment: the bridge lands objects into a released engine, serves
headers from its own BRC-135 lane, and both reference hosts admit
object-for-object. What has been proven against the released engines, and what
remains open, is in [docs/contracts.md](docs/contracts.md).

Releases and notes live on [GitHub Releases](https://github.com/lightwebinc/overlay-bridge/releases); there is no CHANGELOG.

## Papers

- _The overlay bridge: an unmodified engine on the object plane_ (Lightweb Inc., Paper 008): <https://1bsv.net/papers/overlay-bridge.pdf>
- _The overlay object plane: publish BEEF once, and every overlay hears_ (Lightweb Inc., Paper 006): <https://1bsv.net/papers/overlay-object-plane.pdf>
- _Arcade v2 on a multicast delivery fabric_ (Lightweb Inc., Paper 007), for the sibling bridge: <https://1bsv.net/papers/arcade-on-the-fabric.pdf>

## Licence

Apache-2.0 (see `LICENSE`). Third-party dependencies and the attribution their
licences require are in `NOTICE`. There is no licence check in this software,
no call home, and no metering of any kind inside it.
