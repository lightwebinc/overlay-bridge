# Deliver once per landing site

One delivery slot per landing site, local fan-out, and one grammar per
connection are specified in
[bsv-multicast: deliver once](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/deliver-once.md).
This bridge takes the 917x block of the
[lane numbering](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/lane-numbering.md):

| Port | Role |
| --- | --- |
| 9171 | object lane (the edge dials it) |
| 9172 | header lane (the edge dials it) |
| 9175 | submit facade |
| 9178 | header read API |
| 9179 | metrics and health |

9176 and 9177 are unused: the bridge has one facade listener and no resolver,
so nothing binds them.
