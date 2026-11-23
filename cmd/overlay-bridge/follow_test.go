package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/shard-common/interest"
	"github.com/lightwebinc/shard-common/objfmt"
)

// fakeEngine answers GET /listTopicManagers as an overlay host does: a JSON
// object of topic name to metadata, with an ETag, and 304 to a matching
// If-None-Match. fail makes it answer that status instead.
type fakeEngine struct {
	mu       sync.Mutex
	topics   []string
	fail     int
	asked    int
	notModif int
}

func (e *fakeEngine) set(topics ...string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.topics = topics
}

func (e *fakeEngine) failWith(status int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fail = status
}

func (e *fakeEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.asked++
	if r.Method != http.MethodGet || r.URL.Path != listPath {
		http.NotFound(w, r)
		return
	}
	if e.fail != 0 {
		w.WriteHeader(e.fail)
		return
	}
	m := map[string]map[string]string{}
	for _, t := range e.topics {
		m[t] = map[string]string{"name": t, "shortDescription": "test"}
	}
	body, _ := json.Marshal(m)
	sum := sha256.Sum256(body)
	tag := `"` + hex.EncodeToString(sum[:8]) + `"`
	w.Header().Set("ETag", tag)
	if r.Header.Get("If-None-Match") == tag {
		e.notModif++
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func startEngine(t *testing.T, topics ...string) (*fakeEngine, string) {
	t.Helper()
	e := &fakeEngine{topics: topics}
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	return e, srv.URL
}

func followConfig(engine string) config {
	return config{mode: "all", engine: engine, topicsFrom: "engine", topicsEvery: 20 * time.Millisecond}
}

// runFollower starts fo.run and returns each set it applies.
func runFollower(t *testing.T, fo *follower) <-chan []string {
	t.Helper()
	got := make(chan []string, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = fo.run(ctx, func(names []string) { got <- names })
	}()
	t.Cleanup(func() { cancel(); <-done })
	return got
}

func nextSet(t *testing.T, got <-chan []string) []string {
	t.Helper()
	select {
	case s := <-got:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no change of topics was applied")
		return nil
	}
}

func followMetrics(t *testing.T, fo *follower) map[string]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(&collector{follow: fo})
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range m.GetLabel() {
				key += "/" + l.GetValue()
			}
			out[key] = value(m)
		}
	}
	return out
}

// The bridge starts with the engine's topics, not -topics, and follows the
// engine's list as it changes, asking again with the ETag it was given.
func TestFollowerFollowsTheEngine(t *testing.T) {
	eng, url := startEngine(t, "tm_b", "tm_a")
	c := followConfig(url)
	c.topics = "tm_seed"
	fo, err := newFollower(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if got := fo.prime(context.Background(), 2*time.Second); !slices.Equal(got, []string{"tm_a", "tm_b"}) {
		t.Fatalf("started with %v, want the engine's [tm_a tm_b]", got)
	}
	got := runFollower(t, fo)

	eng.set("tm_a", "tm_c")
	if s := nextSet(t, got); !slices.Equal(s, []string{"tm_a", "tm_c"}) {
		t.Fatalf("applied %v, want [tm_a tm_c]", s)
	}
	// Unchanged lists are answered 304 and apply nothing.
	deadline := time.Now().Add(5 * time.Second)
	for {
		eng.mu.Lock()
		n := eng.notModif
		eng.mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the follower never asked with the ETag it was given")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case s := <-got:
		t.Fatalf("applied %v with no change", s)
	default:
	}
	m := followMetrics(t, fo)
	for key, want := range map[string]float64{
		"overlay_bridge_topics":                  2,
		"overlay_bridge_topics_engine_up/" + url: 1,
		"overlay_bridge_topics_changes_total":    1,
	} {
		if m[key] != want {
			t.Errorf("%s = %v, want %v (all: %v)", key, m[key], want, m)
		}
	}
}

// An engine that fails keeps the list it last gave: nothing is withdrawn
// because an engine is down. It is said once, and so is its return.
func TestFollowerKeepsWhatAnEngineLastListed(t *testing.T) {
	eng, url := startEngine(t, "tm_a", "tm_b")
	var logs syncBuffer
	fo, err := newFollower(followConfig(url), slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	fo.prime(context.Background(), time.Second)
	got := runFollower(t, fo)

	eng.failWith(http.StatusInternalServerError)
	waitFor(t, &logs, "TOPICS NOT FOLLOWED")
	time.Sleep(100 * time.Millisecond) // several failed asks
	if n := strings.Count(logs.String(), "TOPICS NOT FOLLOWED"); n != 1 {
		t.Errorf("the failure was logged %d times, want once:\n%s", n, logs.String())
	}
	if m := followMetrics(t, fo); m["overlay_bridge_topics_engine_up/"+url] != 0 || m["overlay_bridge_topics"] != 2 {
		t.Errorf("while failing: %v, want engine_up 0 and both topics kept", m)
	}
	select {
	case s := <-got:
		t.Fatalf("applied %v while the engine failed", s)
	default:
	}

	// Back, with one topic fewer: now it is dropped.
	eng.set("tm_a")
	eng.failWith(0)
	if s := nextSet(t, got); !slices.Equal(s, []string{"tm_a"}) {
		t.Fatalf("applied %v, want [tm_a]", s)
	}
	waitFor(t, &logs, "answers for its topics again")
}

// A topic is taken from the engine its route sends it to, and from no other.
func TestFollowerTakesEachTopicFromItsRoute(t *testing.T) {
	_, def := startEngine(t, "tm_a", "tm_bx")
	_, other := startEngine(t, "tm_bx", "tm_by", "tm_a")
	c := followConfig(def)
	c.engineRoutes = routeFlags{"tm_b*=" + other}
	fo, err := newFollower(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if got := fo.prime(context.Background(), 2*time.Second); !slices.Equal(got, []string{"tm_a", "tm_bx", "tm_by"}) {
		t.Fatalf("carried %v, want [tm_a tm_bx tm_by]", got)
	}
}

// A primary that does not list its topics (a storage provider) keeps the
// -topics names routed to it; one with none routed to it is a warning.
func TestFollowerStaticPrimary(t *testing.T) {
	_, def := startEngine(t, "tm_a")
	provider := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(provider.Close)
	c := followConfig(def)
	c.engineRoutes = routeFlags{"tm_bstoreblob_*=" + provider.URL}
	c.topics = "tm_bstoreblob_1,tm_unlisted"
	var logs syncBuffer
	fo, err := newFollower(c, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// tm_unlisted is routed to the engine, which lists only tm_a: once the
	// engine answers, its list is the engine's.
	if got := fo.prime(context.Background(), 2*time.Second); !slices.Equal(got, []string{"tm_a", "tm_bstoreblob_1"}) {
		t.Fatalf("carried %v, want [tm_a tm_bstoreblob_1]", got)
	}
	if !strings.Contains(logs.String(), "does not list its topics") || strings.Contains(logs.String(), "TOPICS NOT FOLLOWED") {
		t.Errorf("a provider with -topics names should be said once at info:\n%s", logs.String())
	}

	// The engine lists a blob topic it routes to the provider: carried,
	// without -topics naming it.
	_, def2 := startEngine(t, "tm_a", "tm_bstoreblob_2")
	c3 := followConfig(def2)
	c3.engineRoutes = routeFlags{"tm_bstoreblob_*=" + provider.URL}
	fo3, err := newFollower(c3, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if got := fo3.prime(context.Background(), 2*time.Second); !slices.Equal(got, []string{"tm_a", "tm_bstoreblob_2"}) {
		t.Fatalf("carried %v, want [tm_a tm_bstoreblob_2]", got)
	}

	c.topics = ""
	var logs2 syncBuffer
	fo2, err := newFollower(c, slog.New(slog.NewTextHandler(&logs2, nil)))
	if err != nil {
		t.Fatal(err)
	}
	fo2.prime(context.Background(), time.Second)
	if !strings.Contains(logs2.String(), "TOPICS NOT FOLLOWED: an engine does not list its topics and -topics names none") {
		t.Errorf("a provider with no -topics names was not warned about:\n%s", logs2.String())
	}
}

// At start the bridge waits for an engine that is coming up, and starts with
// -topics, saying so, when the engine does not answer in time.
func TestFollowerPrimeWaitsForTheEngine(t *testing.T) {
	eng, url := startEngine(t, "tm_a")
	eng.failWith(http.StatusServiceUnavailable)
	go func() {
		time.Sleep(300 * time.Millisecond)
		eng.failWith(0)
	}()
	fo, err := newFollower(followConfig(url), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if got := fo.prime(context.Background(), 5*time.Second); !slices.Equal(got, []string{"tm_a"}) {
		t.Fatalf("started with %v, want the engine's [tm_a]", got)
	}

	eng2, url2 := startEngine(t, "tm_z")
	eng2.failWith(http.StatusServiceUnavailable)
	c := followConfig(url2)
	c.topics = "tm_seed"
	var logs syncBuffer
	fo2, err := newFollower(c, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if got := fo2.prime(context.Background(), 0); !slices.Equal(got, []string{"tm_seed"}) {
		t.Fatalf("started with %v, want -topics [tm_seed]", got)
	}
	if !strings.Contains(logs.String(), "TOPICS NOT YET FROM THE ENGINE") {
		t.Errorf("starting without the engine's list was not said:\n%s", logs.String())
	}
	got := runFollower(t, fo2)
	eng2.failWith(0)
	if s := nextSet(t, got); !slices.Equal(s, []string{"tm_z"}) {
		t.Fatalf("applied %v, want the engine's [tm_z]", s)
	}
	waitFor(t, &logs, "answers for its topics again")
}

// An answer that is not a list of topics is a failure, never an empty list.
func TestFollowerRefusesAnAnswerThatIsNotAList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`["tm_a"]`))
	}))
	t.Cleanup(srv.Close)
	c := followConfig(srv.URL)
	c.topics = "tm_seed"
	fo, err := newFollower(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if got := fo.prime(context.Background(), 0); !slices.Equal(got, []string{"tm_seed"}) {
		t.Fatalf("carried %v, want -topics kept", got)
	}
}

// The announcer follows a change of topics: a new topic is announced and
// served, a dropped one withdrawn from the edge.
func TestAnnouncerFollowsTopicChanges(t *testing.T) {
	pol := &edgePolicy{listed: map[interest.TopicID]bool{
		interest.TopicID(objfmt.TopicID("tm_a")): true,
		interest.TopicID(objfmt.TopicID("tm_b")): true,
	}}
	rcv, edge, _ := startEdge(t, pol)
	a, err := newAnnouncer(config{interestUpstream: edge, interestBind: "[::1]:0", interestLabel: "follow-test"},
		[]string{"tm_a"}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	}()

	held := func(want ...string) {
		t.Helper()
		ids := make([]interest.TopicID, 0, len(want))
		for _, n := range want {
			ids = append(ids, interest.TopicID(objfmt.TopicID(n)))
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			live := rcv.Live("consumer")
			ok := len(live) == len(ids)
			for _, id := range ids {
				ok = ok && slices.Contains(live, id)
			}
			if ok && gather(t, a)["overlay_bridge_interest_topics/ok"] == float64(len(want)) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the edge holds %d topics, want %v; metrics %v", len(live), want, gather(t, a))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	held("tm_a")
	a.update([]string{"tm_a", "tm_b"})
	held("tm_a", "tm_b")
	a.update([]string{"tm_b"})
	held("tm_b")
}
