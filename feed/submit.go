package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
)

// Submitter posts one BEEF object to one topic on a BRC-22 engine.
type Submitter interface {
	Submit(ctx context.Context, topic string, object []byte) (overlay.Steak, error)
}

// Answer is an engine's reply plus the one detail of its shape that a caller
// relaying the reply has to preserve.
type Answer struct {
	Steak overlay.Steak
	// Wrapped reports whether the engine wrapped its answer as
	// {"STEAK": {...}} rather than answering the bare map. A facade relaying
	// this to a client must re-emit the same form: a client written against
	// one engine and pointed at the facade would otherwise stop being able to
	// read the reply, which defeats the point of serving the engine's own
	// interface.
	Wrapped bool
}

// MarkedSubmitter is a Submitter that can attach the SPV trust marker. The
// marker is a header value (see pkg/spv); the empty string sends none.
type MarkedSubmitter interface {
	SubmitMarked(ctx context.Context, topic string, object []byte, marker string) (overlay.Steak, error)
}

// StatusError is an engine that ANSWERED with a status other than 200. A 4xx
// is the engine refusing the object on its merits (a malformed or invalid
// BEEF); a 5xx is the engine failing.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("engine returned %d %s: %s", e.Code, http.StatusText(e.Code), e.Body)
}

// Refused reports whether err is the engine refusing the object (a 4xx).
func Refused(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code >= 400 && se.Code < 500
}

// DetailSubmitter is a Submitter that also reports the answer's wrapper form.
type DetailSubmitter interface {
	Submitter
	SubmitDetail(ctx context.Context, topic string, object []byte) (Answer, error)
}

// Client is the BRC-22 submit client.
//
// It is hand-rolled on purpose. One client has to speak to two engines whose
// /submit responses differ, and the SDK's own HTTPS facilitator speaks to
// neither reliably: it decodes straight into a bare STEAK, which is right for
// the TypeScript engine and wrong for the Go one (which wraps the answer, so
// the facilitator returns no error and a one-entry map keyed literally
// "STEAK" with every instruction slice empty), and it sends the topic header
// as a JSON array, which the Go engine's generated binder splits on commas and
// then rejects. Both failures are silent in the shape that matters: a success
// that reads as an empty admittance.
//
// The request form below is the single form BOTH engines accept, so the feed
// needs no per-host dialect.
type Client struct {
	// Base is the engine's submit origin, mounted at ROOT: no /api/v1 prefix.
	// The TypeScript host mounts POST /submit on the Express app root and
	// offers no way to set a base path, so a prefix here is simply wrong for
	// the round-one target.
	Base string
	// HTTP is the client to use. Nil builds one with Timeout.
	HTTP *http.Client
	// Timeout is the per-submit ceiling when HTTP is nil. Default 30s.
	Timeout time.Duration
	Log     *slog.Logger
}

var (
	_ Submitter       = (*Client)(nil)
	_ DetailSubmitter = (*Client)(nil)
	_ MarkedSubmitter = (*Client)(nil)
)

// submitResponse is the Go engine's wrapper. The TypeScript engine answers the
// bare map, so a nil STEAK here means "try the other shape", not "failure".
type submitResponse struct {
	STEAK overlay.Steak `json:"STEAK"`
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	t := c.Timeout
	if t == 0 {
		t = 30 * time.Second
	}
	return &http.Client{Timeout: t, Transport: engineTransport}
}

// IdleConnsPerEngine is how many idle connections the submit clients keep
// to one engine. Go's default transport keeps 2, so with more concurrent
// submits than that (the feed's workers, the shadows', the facade's) a
// connection closed after every answer above the second and the next
// submit dialled anew: a TCP setup and an engine-side connection per
// object at load. The pool only grows to the concurrency actually used.
const IdleConnsPerEngine = 256

// engineTransport is the one transport every Client without its own HTTP
// shares, so connections to one engine pool together whichever route or
// shadow uses them.
var engineTransport = func() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = IdleConnsPerEngine
	tr.MaxIdleConns = 4 * IdleConnsPerEngine
	return tr
}()

// Submit posts one object to one topic and returns the engine's admittance
// instructions.
//
// Three details of the request are load-bearing rather than polite:
//
//   - Content-Type must be application/octet-stream. The TypeScript host
//     parses the body with a raw parser bound to exactly that type, so any
//     other value leaves the body empty and the submit fails with a missing
//     BEEF body; the Go engine's size limiter likewise only engages on it.
//   - x-topics carries ONE topic name, with no spaces, sent ONCE. The Go
//     server reads a JSON array as a single topic name and answers 500, and
//     the TypeScript host refuses a repeated header; one plain name in one
//     header is the form neither engine can misread. A delivery record
//     carries exactly one topic identifier, so there is never cause to send
//     more.
func (c *Client) Submit(ctx context.Context, topic string, object []byte) (overlay.Steak, error) {
	a, err := c.SubmitDetail(ctx, topic, object)
	return a.Steak, err
}

// SubmitMarked is Submit with the SPV trust marker attached, when marker is
// not empty. Only the feed sends one, and only for an object its own verifier
// passed; the facade never does.
func (c *Client) SubmitMarked(ctx context.Context, topic string, object []byte, marker string) (overlay.Steak, error) {
	a, err := c.submit(ctx, topic, object, marker)
	return a.Steak, err
}

// SubmitDetail is Submit, and also reports the answer's wrapper form.
func (c *Client) SubmitDetail(ctx context.Context, topic string, object []byte) (Answer, error) {
	return c.submit(ctx, topic, object, "")
}

func (c *Client) submit(ctx context.Context, topic string, object []byte, marker string) (Answer, error) {
	if c.Base == "" {
		return Answer{}, fmt.Errorf("feed: no engine base configured")
	}
	if strings.ContainsAny(topic, " ,") {
		// Refuse locally: a name two engines could split or trim differently
		// is a name to refuse before it is sent.
		return Answer{}, fmt.Errorf("feed: topic %q contains a space or comma", topic)
	}
	url := strings.TrimRight(c.Base, "/") + "/submit"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(object))
	if err != nil {
		return Answer{}, fmt.Errorf("feed: build submit request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", topic)
	if marker != "" {
		req.Header.Set(spv.MarkerHeader, marker)
	}
	req.ContentLength = int64(len(object))

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return Answer{}, fmt.Errorf("feed: submit to %s: %w", url, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Answer{}, fmt.Errorf("feed: read submit response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return Answer{}, fmt.Errorf("feed: %w", &StatusError{Code: resp.StatusCode, Body: snippet(body)})
	}
	return decodeAnswer(body)
}

// decodeSteak accepts both engines' answers: the wrapped {"STEAK":{...}} of
// the Go engine and the bare map of the TypeScript one.
//
// The bare branch is not a hedge. The TypeScript host answers with the STEAK
// unwrapped, and it is the round-one target, so a client that understood only
// the wrapper would read every successful submit as an absent entry and book
// an error on a success.
func decodeAnswer(body []byte) (Answer, error) {
	var wrapped submitResponse
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.STEAK != nil {
		return Answer{Steak: wrapped.STEAK, Wrapped: true}, nil
	}
	var bare overlay.Steak
	if err := json.Unmarshal(body, &bare); err != nil {
		return Answer{}, fmt.Errorf("feed: decode submit response: %w: %s", err, snippet(body))
	}
	return Answer{Steak: bare}, nil
}

// decodeSteak is the shape-agnostic form, kept for callers that do not relay.
func decodeSteak(body []byte) (overlay.Steak, error) {
	a, err := decodeAnswer(body)
	return a.Steak, err
}

func snippet(b []byte) string {
	const max = 200
	s := strings.TrimSpace(string(b))
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
