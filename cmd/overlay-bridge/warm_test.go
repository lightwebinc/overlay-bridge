package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/overlay-bridge/pkg/spv"
)

// fakeAnchor serves the anchor's native routes: a tip and a root per height.
func fakeAnchor(t *testing.T, tip uint32, rootCalls *atomic.Int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tip", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"height":%d,"hash":"%064x"}`, tip, tip)
	})
	mux.HandleFunc("GET /v1/root/{height}", func(w http.ResponseWriter, r *http.Request) {
		rootCalls.Add(1)
		_, _ = fmt.Fprintf(w, `{"height":%s,"merkleRoot":"%064x"}`, r.PathValue("height"), 0xabc)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestStartupWarmsThroughTheAnchorAndPublishesIt runs the startup path the
// binary runs: anchor, warm, then the first verification-path reads of warmed
// heights must not reach the anchor, and the warm-up must be on /metrics.
func TestStartupWarmsThroughTheAnchorAndPublishesIt(t *testing.T) {
	var calls atomic.Int64
	srv := fakeAnchor(t, 5000, &calls)
	c := baseConfig()
	c.headerAnchor = srv.URL
	c.headerAnchorTimeout = 2 * time.Second
	c.headerWarm, c.headerWarmWorkers, c.headerWarmTimeout = 300, 8, 10*time.Second
	c.verifyMode, c.verifyNetwork, c.verifyFinality, c.engineWorkers = "shadow", "all", "ignore", 4
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := buildStore(t.Context(), c, log)
	if err != nil {
		t.Fatal(err)
	}
	warmStore(t.Context(), c, store, log)
	if got := calls.Load(); got != 300 {
		t.Fatalf("warm-up made %d root reads, want 300", got)
	}
	for h := uint32(4701); h <= 5000; h++ {
		if _, err := store.RootAt(t.Context(), h); err != nil {
			t.Fatalf("height %d: %v", h, err)
		}
	}
	if got := calls.Load(); got != 300 {
		t.Fatalf("reads of warmed heights reached the anchor: %d calls, want 300", got)
	}

	tim := newTimings()
	tim.onVerified(spv.Verdict{Result: spv.ResultOK, Transactions: 3, Inputs: 4, Scripts: 2, Proofs: 1, Depth: 2})
	col := &collector{store: store, timings: tim, config: configLabels(c)}
	rec := httptest.NewRecorder()
	col.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`overlay_bridge_header_warm_heights{state="target"} 300`,
		`overlay_bridge_header_warm_heights{state="loaded"} 300`,
		`overlay_bridge_header_warm_heights{state="failed"} 0`,
		`overlay_bridge_header_warm_duration_seconds `,
		// The 300 reads of warmed heights are at or below the startup anchor:
		// history, not a lane gap. Every source is present, at zero if unused,
		// so a rule or panel never reads an absent series as zero.
		`overlay_bridge_tracker_roots_total{source="history"} 300`,
		`overlay_bridge_tracker_roots_total{source="fallback"} 0`,
		`overlay_bridge_tracker_roots_total{source="lane"} 0`,
		`overlay_bridge_tracker_roots_total{source="miss"} 0`,
		`overlay_bridge_verify_object_inputs_sum 4`,
		`overlay_bridge_verify_object_ancestor_depth_sum 2`,
		`overlay_bridge_verify_object_txs_count 1`,
		`overlay_bridge_verify_object_scripts_sum 2`,
		`overlay_bridge_verify_object_proofs_sum 1`,
		`engine_workers="4"`,
		`verify="shadow"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

// TestWarmOffMakesNoReads: -header-warm 0 restores the old startup exactly.
func TestWarmOffMakesNoReads(t *testing.T) {
	var calls atomic.Int64
	srv := fakeAnchor(t, 5000, &calls)
	c := baseConfig()
	c.headerAnchor, c.headerAnchorTimeout, c.headerWarm = srv.URL, 2*time.Second, 0
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := buildStore(t.Context(), c, log)
	if err != nil {
		t.Fatal(err)
	}
	warmStore(t.Context(), c, store, log)
	if calls.Load() != 0 {
		t.Fatalf("%d root reads with warm-up off", calls.Load())
	}
}
