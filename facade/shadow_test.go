package facade

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/feed"
)

// shadowEngine is a fake shadow: it records the topics it was handed and
// answers a fixed STEAK or error.
type shadowEngine struct {
	mu    sync.Mutex
	calls []string
	steak overlay.Steak
	err   error
}

func (s *shadowEngine) Submit(_ context.Context, topic string, _ []byte) (overlay.Steak, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, topic)
	return s.steak, s.err
}

func (s *shadowEngine) n() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func total(s feed.ShadowStats) uint64 {
	var n uint64
	for _, v := range s.Kinds {
		n += v
	}
	return n
}

func admit(topic string) overlay.Steak {
	return overlay.Steak{topic: &overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{0}}}
}

// newShadow returns a started shadow whose probe offer has been counted, and
// the comparisons counted so far (the probe's, and any dropped offer before
// the workers ran), which a test subtracts.
func newShadow(t *testing.T, eng *shadowEngine, name string) (*feed.Shadow, uint64) {
	t.Helper()
	sh := &feed.Shadow{Submit: eng, Workers: 1, Name: name}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = sh.Start(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		// The feed's own tests read the started flag; here the only window
		// is the counters, so an offer is made until one is not dropped.
		before := sh.Stats().Kinds[feed.ShadowDropped]
		sh.Offer("tm_probe", []byte{1, 1, 1, 1}, "", nil)
		if sh.Stats().Kinds[feed.ShadowDropped] == before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shadow workers did not start")
		}
		time.Sleep(time.Millisecond)
	}
	waitFor(t, "the probe", func() bool { return eng.n() == 1 })
	// The probe's own comparison, plus any offer dropped before the workers
	// ran, is the count a test subtracts.
	waitFor(t, "the probe's comparison", func() bool {
		st := sh.Stats()
		return total(st) == st.Kinds[feed.ShadowDropped]+1
	})
	return sh, total(sh.Stats())
}

// TestShadowFacadeOffersAcceptedSubmissions: with the option on, what the
// facade accepts reaches the topic's shadow with the engine's answer to
// compare against, so the publishing host counts parity too.
func TestShadowFacadeOffersAcceptedSubmissions(t *testing.T) {
	eng := &stubEngine{}
	shEng := &shadowEngine{steak: admit("tm_example")}
	sh, probe := newShadow(t, shEng, feed.DefaultRoute)
	f, _ := newFacade(t, eng, &stubPublisher{}, "tm_example")
	f.cfg.Shadow, f.cfg.ShadowFacade = sh, true

	rec := submit(t, f, "tm_example", beefObj)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	waitFor(t, "the shadow offer", func() bool { return shEng.n() == 2 })
	if shEng.calls[1] != "tm_example" {
		t.Fatalf("shadow got %v", shEng.calls)
	}
	waitFor(t, "the comparison", func() bool { return total(sh.Stats()) == probe+1 })
	if st := sh.Stats(); st.Kinds[feed.ShadowMatch] != 1 {
		t.Fatalf("shadow stats = %+v", st)
	}
}

func TestShadowFacadeOffByDefault(t *testing.T) {
	shEng := &shadowEngine{steak: admit("tm_example")}
	sh, _ := newShadow(t, shEng, feed.DefaultRoute)
	f, _ := newFacade(t, &stubEngine{}, &stubPublisher{}, "tm_example")
	f.cfg.Shadow = sh
	if rec := submit(t, f, "tm_example", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	time.Sleep(20 * time.Millisecond)
	if shEng.n() != 1 {
		t.Fatalf("the shadow was offered a facade submission with the option off: %v", shEng.calls)
	}
}

// TestFacadeRoutesATopicToItsEngine: a routed topic is admitted by the
// route's primary and offered to the route's shadow; an unrouted one by the
// default pair.
func TestFacadeRoutesATopicToItsEngine(t *testing.T) {
	defEng := &stubEngine{}
	routed := &stubEngine{wrapped: true}
	defShEng := &shadowEngine{steak: admit("tm_borg_lab")}
	defSh, _ := newShadow(t, defShEng, feed.DefaultRoute)
	rtShEng := &shadowEngine{steak: admit("tm_bsecret_lab")}
	rtSh, _ := newShadow(t, rtShEng, "tm_bsecret_*")
	f, _ := newFacade(t, defEng, &stubPublisher{}, "tm_borg_lab", "tm_bsecret_lab")
	f.cfg.Shadow, f.cfg.ShadowFacade = defSh, true
	f.cfg.Routes = []*feed.Route{{Pattern: "tm_bsecret_*", Submit: routed, Shadow: rtSh}}

	rec := submit(t, f, "tm_bsecret_lab", beefObj)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	// The routed engine's wrapper form is the one relayed.
	if body := rec.Body.String(); len(body) == 0 || body[:9] != `{"STEAK":` {
		t.Fatalf("routed answer not relayed in the engine's form: %s", body)
	}
	if routed.calls != 1 || defEng.calls != 0 {
		t.Fatalf("engines: routed %d, default %d", routed.calls, defEng.calls)
	}
	waitFor(t, "the routed shadow", func() bool { return rtShEng.n() == 2 })
	if rtShEng.calls[1] != "tm_bsecret_lab" || defShEng.n() != 1 {
		t.Fatalf("shadows: routed %v, default %v", rtShEng.calls, defShEng.calls)
	}

	if rec := submit(t, f, "tm_borg_lab", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	if routed.calls != 1 || defEng.calls != 1 {
		t.Fatalf("engines after the unrouted topic: routed %d, default %d", routed.calls, defEng.calls)
	}
	waitFor(t, "the default shadow", func() bool { return defShEng.n() == 2 })
}

// TestShadowDownNeverFailsTheFacade: a shadow that refuses everything costs
// the client nothing; the engine's answer and the publish are untouched.
func TestShadowDownNeverFailsTheFacade(t *testing.T) {
	shEng := &shadowEngine{err: errors.New("connection refused")}
	sh, _ := newShadow(t, shEng, feed.DefaultRoute)
	pub := &stubPublisher{}
	f, _ := newFacade(t, &stubEngine{}, pub, "tm_example")
	f.cfg.Shadow, f.cfg.ShadowFacade = sh, true
	rec := submit(t, f, "tm_example", beefObj)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	if len(pub.records) != 1 {
		t.Fatalf("published %d records, want 1", len(pub.records))
	}
	// The probe was the first error; this submission is the second.
	waitFor(t, "the shadow error", func() bool { return sh.Stats().Kinds[feed.ShadowError] == 2 })
	if st := f.Stats(); st.OK != 1 || st.EngineError != 0 {
		t.Fatalf("facade stats = %+v", st)
	}
}

// An engine failure is answered 502 and offered to nobody: there is no
// answer to compare and the client's retry brings the object again.
func TestShadowFacadeSkipsAnEngineFailure(t *testing.T) {
	shEng := &shadowEngine{steak: admit("tm_example")}
	sh, _ := newShadow(t, shEng, feed.DefaultRoute)
	f, _ := newFacade(t, &stubEngine{err: errors.New("down")}, &stubPublisher{}, "tm_example")
	f.cfg.Shadow, f.cfg.ShadowFacade = sh, true
	if rec := submit(t, f, "tm_example", beefObj); rec.Code != http.StatusBadGateway {
		t.Fatalf("code = %d", rec.Code)
	}
	time.Sleep(20 * time.Millisecond)
	if shEng.n() != 1 {
		t.Fatalf("the shadow was offered a failed submission: %v", shEng.calls)
	}
}
