package topics

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func idOf(name string) string {
	id := ID(name)
	return hex.EncodeToString(id[:])
}

// The direction that loses data: the consumer subscribed to a topic the bridge
// does not carry, so its objects arrive and are discarded.
func TestMissingIsTheDirectionThatLosesData(t *testing.T) {
	d := Compare([]string{"tm_a"}, []string{idOf("tm_a"), idOf("tm_b")}, false)
	if d.OK() {
		t.Fatal("a subscribed topic the bridge does not carry must not read as OK")
	}
	if len(d.Missing) != 1 || d.Missing[0] != idOf("tm_b") {
		t.Fatalf("missing = %v", d.Missing)
	}
	if !strings.Contains(d.Describe(), "discarded") {
		t.Errorf("the description should say the objects are discarded: %s", d.Describe())
	}
}

// The other direction is harmless and must NOT be reported as a fault: nothing
// is delivered for a topic nobody elected, so a wider flag loses nothing.
func TestExtraIsHarmless(t *testing.T) {
	d := Compare([]string{"tm_a", "tm_stale"}, []string{idOf("tm_a")}, false)
	if !d.OK() {
		t.Fatal("a topic in -topics that nobody elected is not a fault")
	}
	if len(d.Extra) != 1 || d.Extra[0] != "tm_stale" {
		t.Fatalf("extra = %v", d.Extra)
	}
	if !strings.Contains(d.Describe(), "harmless") {
		t.Errorf("the description should say so: %s", d.Describe())
	}
}

func TestMatchingIsQuiet(t *testing.T) {
	d := Compare([]string{"tm_b", "tm_a"}, []string{idOf("tm_a"), idOf("tm_b")}, false)
	if !d.OK() || len(d.Missing) != 0 || len(d.Extra) != 0 {
		t.Fatalf("a matching pair should be clean: %+v", d)
	}
	if !strings.Contains(d.Describe(), "match") {
		t.Errorf("describe = %s", d.Describe())
	}
}

// An aggregator receives every topic on the plane, so no finite -topics list
// can match it. Reporting that as "matching" because the two lists happen to
// be equal today would be the worst possible answer.
func TestAggregatorCanNeverMatch(t *testing.T) {
	d := Compare([]string{"tm_a"}, []string{idOf("tm_a")}, true)
	if d.OK() {
		t.Fatal("an aggregator posture cannot be satisfied by a finite topic list")
	}
	if !strings.Contains(d.Describe(), "AGGREGATOR") {
		t.Errorf("describe = %s", d.Describe())
	}
}

func TestCompareIgnoresBlanksAndCase(t *testing.T) {
	d := Compare([]string{"  tm_a  ", "", "   "}, []string{strings.ToUpper(idOf("tm_a"))}, false)
	if !d.OK() {
		t.Fatalf("a padded name and an upper-case id should still match: %+v", d)
	}
}

func TestFetchReadsTheConsumersElection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/state" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"consumers":[
			{"id":"consumer-b","subscription":{"beefTopics":["tm_z"]}},
			{"id":"consumer-a","subscription":{"beefTopics":["tm_a","tm_b"],"allBeefTopics":false}}
		]}`))
	}))
	defer srv.Close()

	sub, err := Fetch(context.Background(), srv.URL, "consumer-a", "tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Names in, identifiers out: the comparison is on what the edge filters
	// on, not on what the source displays.
	if len(sub.TopicIDs) != 2 || sub.TopicIDs[0] != idOf("tm_a") {
		t.Fatalf("topic ids = %v", sub.TopicIDs)
	}
	if sub.AllTopics {
		t.Error("allTopics leaked from another consumer")
	}
}

// Every failure is reported and none is fatal: a bridge that refused to start
// because the source blinked would turn a control-plane blip into a data-plane
// outage.
func TestFetchFailuresAreErrorsNotPanics(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	if _, err := Fetch(context.Background(), down.URL, "consumer-a", "", nil); err == nil {
		t.Fatal("a 503 should be an error")
	}

	absent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"consumers":[]}`))
	}))
	defer absent.Close()
	if _, err := Fetch(context.Background(), absent.URL, "consumer-a", "", nil); err == nil {
		t.Fatal("an absent consumer should be an error")
	}
}

// TestFetchWireContract pins the request docs/contracts.md documents for a
// subscription source, field by field, so a service written to that page
// keeps answering this client: GET {base}/v1/state with any trailing slash
// trimmed and a path in the base kept, Accept: application/json, the token as
// a bearer credential only when one is set, and no body. On the answer it
// pins what a source may rely on: unknown fields and other consumers are
// ignored, the first matching entry wins, and allBeefTopics is read.
func TestFetchWireContract(t *testing.T) {
	type seen struct {
		method, path, accept, auth string
		hasAuth                    bool
		bodyLen                    int64
	}
	reqs := make(chan seen, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hasAuth := r.Header["Authorization"]
		reqs <- seen{r.Method, r.URL.Path, r.Header.Get("Accept"), r.Header.Get("Authorization"), hasAuth, r.ContentLength}
		_, _ = w.Write([]byte(`{"generation":7,"consumers":[
			{"id":"consumer-b","subscription":{"beefTopics":["tm_z"]}},
			{"id":"consumer-a","name":"a wider document","subscription":{"beefTopics":["tm_a"],"allBeefTopics":true,"other":1}},
			{"id":"consumer-a","subscription":{"beefTopics":["tm_late"]}}
		]}`))
	}))
	defer srv.Close()

	sub, err := Fetch(context.Background(), srv.URL+"/api/", "consumer-a", "tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := seen{http.MethodGet, "/api/v1/state", "application/json", "Bearer tok", true, 0}
	if got := <-reqs; got != want {
		t.Fatalf("request = %+v, want %+v", got, want)
	}
	if len(sub.TopicIDs) != 1 || sub.TopicIDs[0] != idOf("tm_a") || !sub.AllTopics {
		t.Fatalf("subscription = %+v, want the first consumer-a entry: tm_a, all topics", sub)
	}

	if _, err := Fetch(context.Background(), srv.URL, "consumer-b", "", nil); err != nil {
		t.Fatal(err)
	}
	if got := <-reqs; got.path != "/v1/state" || got.hasAuth {
		t.Fatalf("without a token: path %q, Authorization sent %v; want /v1/state and no header", got.path, got.hasAuth)
	}
}

// The errors name the subscription source, the one name the contract gives
// the service at the other end, whatever service implements it.
func TestFetchErrorsNameTheSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
	}{
		{"status", http.StatusServiceUnavailable, ""},
		{"not json", http.StatusOK, "not json"},
		{"absent consumer", http.StatusOK, `{"consumers":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			_, err := Fetch(context.Background(), srv.URL, "consumer-a", "", nil)
			if err == nil || !strings.HasPrefix(err.Error(), "subscription source: ") {
				t.Fatalf("err = %v, want one that names the subscription source", err)
			}
		})
	}
}
