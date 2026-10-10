# Architecture

Three independent parts on one landing machine, and no state of record.

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


## feed (down)

The object lane delivers one length-delimited BRC-149 delivery record per
object: the identifier of the elected topic that matched, the payload's
length, and the payload verbatim, which is the publisher's submission record
(every topic name it wrote, then the BEEF object) or, from an older edge, the
bare object. The feed splits the stream on the explicit length, unwraps the
payload, maps the matched identifier back to the name the host elected (the
identifier is the hash of the name, so the map is computed from the election
and never fetched), and submits the object to the engine under that name and
under every other name in the record the host also elected. The plane
delivers an object once per subscriber however many of its topics matched,
so that second step is what keeps every elected topic fed; names the host
did not elect are labels and are left alone.

The plane is open to any publisher, so the bytes on this lane are whatever some
publisher chose to send and the network never parses past the leading marker.
Every length an object declares is therefore checked against the bytes actually
present before anything of that size is allocated. A parser failure is a
rejected object, not an incident.

## headers (down)

Bare 80-byte headers arrive on their own lane, are proof-of-work checked and
chained on arrival, and are held in a local store that is served to the engine
as its chain tracker. The lane is a live tail: on start, or after a gap longer
than the network's repair window, the store re-anchors from a header source the
operator trusts and then follows the lane.

## facade (up)

One listener serves the engine's own submit interface. An accepted submission
is forwarded to the local engine first, so the client receives the engine's
real admittance instructions, and then published once onto the plane.

## guard

A bounded seen-set keyed on SHA-256(ContentID ‖ TopicID), the same key the
fabric's own ingress claims an object under. Both sides mark before they act.

**This is sound only while the engine does not propagate.** An engine
re-serialises what it propagates, so an HTTPS propagation echo of our own
object would arrive with different bytes, a different ContentID, and this guard
would not catch it. The posture that switches propagation off is what makes a
bytes-derived identity safe here. Re-enabling propagation re-opens that
decision; it is not a configuration change.

## Dependencies

- [`github.com/lightwebinc/teranode-bridge`](https://github.com/lightwebinc/teranode-bridge): the shared landing-tier packages (lane termination, failover)
- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common): the push object-frame codecs, including the BRC-149 delivery record
- [`github.com/bsv-blockchain/go-sdk`](https://github.com/bsv-blockchain/go-sdk): BEEF parsing and block-header hashing
- [`github.com/prometheus/client_golang`](https://github.com/prometheus/client_golang): metrics

The bridge links no overlay engine module. The two contracts it needs, the
submit request and its admittance response in both engines' wire forms, are
pinned by fixtures captured from the real engines.


## What stays where it is

The plane replaces live propagation and only that. Discovery, lookup, history
and catch-up remain the overlay's own protocols. The bridge does not touch the
engine's synchronization configuration.
