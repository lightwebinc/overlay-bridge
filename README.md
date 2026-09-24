# overlay-bridge

Run an **unmodified overlay services engine** with a multicast delivery fabric
as its transport.

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

## Why a bridge

An overlay host does two jobs when an object arrives. It **admits** it: verify
the BEEF, run the topic manager, index the outputs. Then it **propagates** it:
one HTTPS submit to every other host of the topic. The second job is the
expensive one, and only an operator sees it. A publisher already submits to
every host it can find, and every host that admits re-submits to every other,
so between N hosts one object crosses the network on the order of N squared
times and is fully verified at every arrival before a duplicate check discards
it. A host that was unreachable simply misses it, with no signal that it did.

This bridge replaces that second fan-out with one publication. Delivery arrives
as an ordinary submit on the interface the engine already serves; publication
goes out once and the network fans it out to every subscribed host.

**Nothing is forked.** The bridge imports no engine module and speaks only the
engine's published HTTP interfaces. Stop it, give the engine back its
propagation peers and its stock chain tracker, and you have a stock overlay
host again. The bridge holds no state of record.

That claim is about THIS repository and is meant literally rather than as a
claim about whichever engine you point it at: the bridge works against a stock
engine and asks nothing of it beyond the published interfaces. Our own
reference host happens to run a small fork of the TypeScript engine, for one
observability hook unrelated to the bridge, and the bridge neither knows nor
cares.

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

## Status

Running on devnet. All packages are built; the bridge lands objects into a
released engine, serves headers from its own BRC-135 lane, and both reference
hosts admit object-for-object. See `docs/` for the build plan and the wire
contracts, and the repository tags for releases.

## Licence

Apache-2.0 (see `LICENSE`). Third-party dependencies and the attribution their
licences require are in `NOTICE`. There is no licence check in this software,
no call home, and no metering of any kind inside it.

## Papers

- _The overlay bridge: an unmodified engine on the object plane_
- _The overlay object plane: publish BEEF once, and every overlay hears_:
  <https://1bsv.net/papers/overlay-object-plane.pdf>
