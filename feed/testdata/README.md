# Contract fixtures

`steak_bare_real_engine.json` is the verbatim reply of a released
`@bsv/overlay` v2.3.1 engine, constructed with no advertiser, to a submit
carried across the object lane by this bridge. `steak_wrapped_real_engine.json`
is the verbatim reply of a real `go-overlay-services` v1.3.5 server behind its
own HTTP layer. Note its nulls: that engine emits `coinsToRetain: null` where
the TypeScript engine emits an array, so a decoder that assumes either shape is
wrong about one of them.

These two carry the authority. A fixture written by hand agrees with our own
Go types on both sides and cannot catch a field-naming defect; a captured
reply can. `steak_wrapped.json`, `steak_bare.json` and `steak_bare_empty.json`
are hand-written shapes kept for the decoder's table.

There is no separate duplicate fixture: `steak_bare_real_engine.json` is a
real engine's answer to an object it already held, and its empty
`outputsToAdmit` is the duplicate case.

## The two submit wire forms

`submit_request.txt` and `submit_request_jsonarray.txt` are the same
submission in the two `x-topics` spellings a client may send:

- **Comma list, no spaces.** Accepted by both engines. The TypeScript host
  trims each element; the Go server trims a leading space too, so the strict
  form is a courtesy there rather than a necessity.
- **JSON array.** What go-sdk's own facilitator emits and what the TypeScript
  host accepts. The Go server does not parse it: the whole bracketed string
  becomes one topic name and the submit fails with a 500.

So a comma list with no spaces, in one header, is the form to send when the
engine behind the door is unknown, and it is the only form this bridge sends.
Both forms were confirmed against a running TypeScript host, which answered a
bare STEAK to each. The body is described rather than embedded because it is
binary and not what these fixtures are about.
