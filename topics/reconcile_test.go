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

// The direction that loses data: the customer joined a topic the bridge does
// not carry, so its objects arrive and are discarded.
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
			{"id":"cn_other","subscription":{"beefTopics":["tm_z"]}},
			{"id":"cn_me","subscription":{"beefTopics":["tm_a","tm_b"],"allBeefTopics":false}}
		]}`))
	}))
	defer srv.Close()

	sub, err := Fetch(context.Background(), srv.URL, "cn_me", "tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Names in, IDS out: the comparison is on what the broker projects, not on
	// what it displays.
	if len(sub.TopicIDs) != 2 || sub.TopicIDs[0] != idOf("tm_a") {
		t.Fatalf("topic ids = %v", sub.TopicIDs)
	}
	if sub.AllTopics {
		t.Error("allTopics leaked from another consumer")
	}
}

// Every failure is reported and none is fatal: a bridge that refused to start
// because the broker blinked would turn a control-plane blip into a data-plane
// outage.
func TestFetchFailuresAreErrorsNotPanics(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	if _, err := Fetch(context.Background(), down.URL, "cn_me", "", nil); err == nil {
		t.Fatal("a 503 should be an error")
	}

	absent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"consumers":[]}`))
	}))
	defer absent.Close()
	if _, err := Fetch(context.Background(), absent.URL, "cn_me", "", nil); err == nil {
		t.Fatal("an absent consumer should be an error")
	}
}
