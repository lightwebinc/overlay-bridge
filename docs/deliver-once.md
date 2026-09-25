# Deliver once per landing site

The scarce resource is the tunnel, not the landing machine.

A site that runs several bridges, an overlay host beside a broadcaster stack
for instance, provisions ONE delivery slot and elects every lane it needs on
it. Each bridge terminates its own lanes, and the object and header lanes do
not collide with the settlement lanes the other bridges take: one decade per
bridge, and this one takes the 917x block.

| Port | Role |
| --- | --- |
| 9171 | object lane (the edge dials it) |
| 9172 | header lane (the edge dials it) |
| 9175 | submit facade |
| 9178 | header read API |
| 9179 | metrics and health |

9176 and 9177 are unused: the bridge has one facade listener and no resolver,
so nothing binds them.

## Two bridges on one machine

The lanes are single-class and a lane's port is claimed by whichever bridge
binds it, so two bridges on one host cannot both terminate the same lane. That
collision is deliberate: it stops a site pulling the same bytes twice over two
slots and paying for both.

When two bridges on one machine genuinely need the same lane, a small
tier-neutral tee fans it out locally, where the copy costs nothing. That tee is
follow-up work and arrives with the first site that needs it.

## A grammar owns its connection

The fabric ingress decides a connection's grammar from its first four bytes,
once, for the life of the connection. The submission-record stream this bridge
publishes therefore needs its own socket and must never share one with
extended-format transactions: the first four bytes have already decided how
every later byte is read, so interleaving corrupts one of the two grammars
rather than erroring. The bridge enforces this structurally, one client per
grammar, and asserts it in a test.
