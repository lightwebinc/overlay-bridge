package facade

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lightwebinc/overlay-bridge/feed"
)

// TestSubmitReadsXTopicsInAnyCase pins what reading x-topics by its canonical
// name keeps from Header.Values: a client that writes the name in lower case
// on the wire is read, and a header sent twice is refused rather than half
// read.
func TestSubmitReadsXTopicsInAnyCase(t *testing.T) {
	f, _ := newFacade(t, &stubEngine{}, &stubPublisher{}, "tm_example")
	srv := httptest.NewServer(f.Handler())
	defer srv.Close()

	post := func(h http.Header) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/submit", bytes.NewReader(beefObj))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range h {
			req.Header[k] = v // set as given: the client writes such a key verbatim
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := post(http.Header{"x-topics": {"tm_example"}}); code != http.StatusOK {
		t.Fatalf("x-topics in lower case on the wire: code %d, want 200", code)
	}
	if code := post(http.Header{"X-Topics": {"tm_example", "tm_example"}}); code != http.StatusBadRequest {
		t.Fatalf("x-topics sent twice: code %d, want 400", code)
	}
	if s := f.Stats(); s.OK != 1 || s.Malformed != 1 {
		t.Fatalf("ok %d malformed %d, want 1 and 1", s.OK, s.Malformed)
	}
}

// TestFacadeLogsNoRequestCredentials follows the paths from the request to a
// log line that names a topic: x-topics through a route's also target to its
// shed warning, and through a failing engine to the facade's own warning,
// with credentials in the request beside it. The topic is logged; the
// Authorization and Cookie values never are.
func TestFacadeLogsNoRequestCredentials(t *testing.T) {
	const token, session = "request-token-not-for-logs", "cookie-not-for-logs"
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, nil))

	// A one-byte bound sheds every object, which is the warning to read.
	a := &feed.Also{Submit: &alsoTarget{}, Name: "tm_bstore_*", Target: "http://provider", Workers: 1, QueueBytes: 1, Log: log}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Start(ctx) }()
	waitFor(t, "also workers", a.Running)

	f, _ := newFacade(t, &stubEngine{}, &stubPublisher{}, "tm_bstore_lab", "tm_borg_lab")
	f.cfg.Log = log
	f.cfg.Routes = []*feed.Route{
		{Pattern: "tm_bstore_*", Submit: &stubEngine{wrapped: true}, Also: []*feed.Also{a}},
		{Pattern: "tm_borg_*", Submit: &stubEngine{err: errors.New("engine down")}},
	}
	for _, topic := range []string{"tm_bstore_lab", "tm_borg_lab"} {
		req := httptest.NewRequest(http.MethodPost, "/submit", bytes.NewReader(beefObj))
		req.Header.Set("x-topics", topic)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Cookie", "session="+session)
		f.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}

	got := out.String()
	for _, want := range []string{"also: object shed", "topic=tm_bstore_lab", "facade: engine submit failed", "topic=tm_borg_lab"} {
		if !strings.Contains(got, want) {
			t.Errorf("the log lacks %q: %s", want, got)
		}
	}
	if strings.Contains(got, token) || strings.Contains(got, session) {
		t.Errorf("a request credential reached the log: %s", got)
	}
}
