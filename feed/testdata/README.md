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

## The two submit wire forms

`submit_request.txt` and `submit_request_jsonarray.txt` are the SAME
submission in the two `x-topics` spellings the two engines accept. Paper 008
review item R3 makes carrying both a release requirement rather than a
follow-up, because the difference is silent:

- **Comma list, NO SPACES.** The Go server's binder splits on the comma and
  does not trim, so one leading space becomes a topic name nobody mounts and
  fails the whole submission with an unknown-topic error. Repeating the header
  does not help: the generated wrapper reads only the first value.
- **JSON array.** What go-sdk's own facilitator emits, and what the TypeScript
  host answers to. The Go binder does not parse it at all: it would split the
  raw JSON on commas and invent topic names out of the brackets and quotes.

The TypeScript host accepts BOTH and trims each element, so a comma list with
spaces is safe there and fatal on the Go server. That makes the comma list
with no spaces the strictest common form, and it is what a client should send
when it does not know which engine is behind the door.

Confirmed against a RUNNING TypeScript host 2026-09-22, not read out of the
package source: both forms were submitted and both were accepted, and the host
answered a bare STEAK to each. The header bytes here are the ones
`txmint/internal/overlay`'s own tests pin (`TestCommaFormHasNoSpaces`,
`TestJSONArrayForm`); the body is described rather than embedded because it is
binary and its content is not what these fixtures are about.

There is no separate `steak_duplicate.json`: the duplicate case IS
`steak_bare_real_engine.json`, whose empty `outputsToAdmit` is a real engine's
answer to an object it already held.
