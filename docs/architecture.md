# Architecture

Three independent parts on one landing machine, and no state of record.

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

## What stays where it is

The plane replaces live propagation and only that. Discovery, lookup, history
and catch-up remain the overlay's own protocols. The bridge does not touch the
engine's synchronization configuration.
