# Contract fixtures

`steak_bare_real_engine.json` is not hand-written. It is the verbatim reply of
a released `@bsv/overlay` v2.3.1 engine, constructed directly with no
advertiser, to a submit carried across the object lane by this bridge. It is
kept because a hand-written fixture agreed with our own Go types on both sides
and therefore could not catch the field-naming defect that this reply exposed
in one request.

`steak_wrapped.json` is the Go engine's wrapper form. `steak_bare.json` and
`steak_bare_empty.json` are the shapes those two engines produce for an
admitted and a duplicate submission.
