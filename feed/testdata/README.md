# Contract fixtures

`steak_bare_real_engine.json` is not hand-written. It is the verbatim reply of
a released `@bsv/overlay` v2.3.1 engine, constructed directly with no
advertiser, to a submit carried across the object lane by this bridge. It is
kept because a hand-written fixture agreed with our own Go types on both sides
and therefore could not catch the field-naming defect that this reply exposed
in one request.

`steak_wrapped_real_engine.json` is likewise verbatim, from a real
`go-overlay-services` v1.3.5 server behind its own HTTP layer. Note its nulls:
that engine emits `coinsToRetain: null` where the TypeScript engine emits an
array, so a decoder that assumes either shape is wrong about one of them.

`steak_wrapped.json`, `steak_bare.json` and `steak_bare_empty.json` are
hand-written shapes kept for the decoder's table. The two `_real_engine`
fixtures are the ones that carry authority.
