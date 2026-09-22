package feed

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return b
}

// TestDecodeSteakBothEngines is the release requirement: one decoder must
// cover both engines. The Go engine wraps its answer, the TypeScript engine
// does not, and the TypeScript engine is the round-one target. A client that
// understood only the wrapper would read every successful TypeScript submit as
// an absent entry and book an error on a success.
func TestDecodeSteakBothEngines(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		admit   int
	}{
		{"go engine wraps the answer", "steak_wrapped.json", 1},
		// Captured verbatim from a real go-overlay-services v1.3.5 server.
		{"a real Go engine's reply", "steak_wrapped_real_engine.json", 1},
		{"typescript engine answers bare", "steak_bare.json", 1},
		{"bare with nothing admitted", "steak_bare_empty.json", 0},
		// Captured verbatim from a released engine rather than written by hand.
		{"a real engine's reply", "steak_bare_real_engine.json", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			steak, err := decodeSteak(readFixture(t, tc.fixture))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			topic := "tm_example"
			if strings.HasSuffix(tc.fixture, "_real_engine.json") {
				topic = "tm_proof"
			}
			ai, ok := steak[topic]
			if !ok || ai == nil {
				t.Fatalf("no entry for %s in %v", topic, steak)
			}
			if got := len(ai.OutputsToAdmit); got != tc.admit {
				t.Fatalf("outputsToAdmit = %d, want %d", got, tc.admit)
			}
		})
	}
}

// TestSubmitRequestForm pins the three load-bearing request details. Each one
// fails silently or confusingly on at least one of the two engines if it
// drifts, so they are asserted rather than commented.
func TestSubmitRequestForm(t *testing.T) {
	var (
		gotPath   string
		gotType   string
		gotTopics []string
		gotBody   []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotType = r.Header.Get("Content-Type")
		gotTopics = r.Header.Values("x-topics")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(readFixture(t, "steak_bare.json"))
	}))
	defer srv.Close()

	c := &Client{Base: srv.URL}
	object := []byte{0x01, 0x01, 0x01, 0x01, 0xAA}
	if _, err := c.Submit(context.Background(), "tm_example", object); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if gotPath != "/submit" {
		t.Errorf("path = %q, want /submit (no /api/v1 prefix)", gotPath)
	}
	if gotType != "application/octet-stream" {
		t.Errorf("Content-Type = %q: the TypeScript host's raw parser is bound to octet-stream", gotType)
	}
	if len(gotTopics) != 1 {
		t.Errorf("x-topics sent %d times, want exactly 1: a repeated header arrives as an array and is refused", len(gotTopics))
	}
	if len(gotTopics) == 1 && gotTopics[0] != "tm_example" {
		t.Errorf("x-topics = %q, want the bare name with no spaces", gotTopics[0])
	}
	if string(gotBody) != string(object) {
		t.Errorf("body = %x, want the object verbatim %x", gotBody, object)
	}
}

// TestSubmitRefusesUnsendableTopic pins that a space or comma is caught here
// rather than silently misread by the engine.
func TestSubmitRefusesUnsendableTopic(t *testing.T) {
	c := &Client{Base: "http://127.0.0.1:1"}
	for _, topic := range []string{" tm_example", "tm_a,tm_b", "tm a"} {
		if _, err := c.Submit(context.Background(), topic, []byte{0x01}); err == nil {
			t.Errorf("topic %q was accepted", topic)
		}
	}
}

func TestSubmitNonOKIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Missing or empty BEEF body", http.StatusBadRequest)
	}))
	defer srv.Close()
	c := &Client{Base: srv.URL}
	if _, err := c.Submit(context.Background(), "tm_example", []byte{0x01}); err == nil {
		t.Fatal("a 400 was reported as success")
	}
}
