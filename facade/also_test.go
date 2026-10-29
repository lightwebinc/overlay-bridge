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

type alsoTarget struct {
	mu    sync.Mutex
	calls []string
}

func (a *alsoTarget) Submit(_ context.Context, topic string, _ []byte) (overlay.Steak, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, topic)
	return overlay.Steak{topic: &overlay.AdmittanceInstructions{}}, nil
}

func (a *alsoTarget) n() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

func startedAlso(t *testing.T, sub feed.Submitter) *feed.Also {
	t.Helper()
	a := &feed.Also{Submit: sub, Name: "tm_bstore_*", Target: "http://provider", Workers: 1}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = a.Start(ctx) }()
	waitFor(t, "also workers", a.Running)
	return a
}

// A submission the route's primary accepts is published and handed to the
// route's also target, which is how a provider beside the host gets what the
// plane will not deliver back here. A repeat the guard knows is neither.
func TestFacadeHandsAcceptedSubmissionsToAlso(t *testing.T) {
	target := &alsoTarget{}
	a := startedAlso(t, target)
	pub := &stubPublisher{}
	f, _ := newFacade(t, &stubEngine{}, pub, "tm_bstore_lab", "tm_borg_lab")
	f.cfg.Routes = []*feed.Route{{Pattern: "tm_bstore_*", Submit: &stubEngine{wrapped: true}, Also: []*feed.Also{a}}}

	if rec := submit(t, f, "tm_bstore_lab", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	waitFor(t, "the also target", func() bool { return a.Stats().Results[feed.AlsoOK] == 1 })
	if len(pub.records) != 1 {
		t.Fatalf("published %d records, want 1", len(pub.records))
	}
	// The same bytes again: the guard knows them now, so nothing is
	// published or handed on a second time.
	if rec := submit(t, f, "tm_bstore_lab", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("repeat code = %d", rec.Code)
	}
	// An unrouted topic has no also target.
	if rec := submit(t, f, "tm_borg_lab", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("unrouted code = %d", rec.Code)
	}
	time.Sleep(20 * time.Millisecond)
	if n := target.n(); n != 1 {
		t.Fatalf("also target called %d times, want 1", n)
	}
}

// A submission the route's primary refuses is answered as before, and the
// also target never sees it: nothing the primary turned away is handed on.
func TestFacadeHandsNothingRefusedToAlso(t *testing.T) {
	target := &alsoTarget{}
	a := startedAlso(t, target)
	pub := &stubPublisher{}
	f, _ := newFacade(t, &stubEngine{}, pub, "tm_bstoreblob_lab")
	refusing := &stubEngine{err: &feed.StatusError{Code: http.StatusBadRequest, Body: "piece: bad signature"}}
	f.cfg.Routes = []*feed.Route{{Pattern: "tm_bstoreblob_*", Submit: refusing, Also: []*feed.Also{a}}}

	if rec := submit(t, f, "tm_bstoreblob_lab", beefObj); rec.Code != http.StatusBadGateway {
		t.Fatalf("code = %d, want 502", rec.Code)
	}
	time.Sleep(20 * time.Millisecond)
	if target.n() != 0 || len(pub.records) != 0 {
		t.Fatalf("refused object handed on (%d) or published (%d)", target.n(), len(pub.records))
	}
	if errors.Is(nil, nil) && refusing.calls != 1 {
		t.Fatalf("primary calls = %d", refusing.calls)
	}
}
