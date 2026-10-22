package main

import (
	"time"

	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/prometheus/client_golang/prometheus"
)

// verifyBuckets match the engine's overlay_host_verify_duration_seconds, so
// the two histograms can be read on one panel.
var verifyBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1}

// submitBuckets cover lane arrival to the engine's answer: sub-millisecond on
// an idle host up to the submit timeout.
var submitBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// workBuckets cover per-object work counts (R2): a simple object is 1-2, a
// deep or wide one tens, and a pathological one hundreds.
var workBuckets = []float64{0, 1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 64, 128, 256}

// timings are the bridge's histograms. They are observed, not derived from
// Stats, so they are real prometheus vectors beside the collector.
type timings struct {
	verify *prometheus.HistogramVec
	submit *prometheus.HistogramVec
	// work is the R2 per-object work: one histogram per counter, observed
	// once per verified object, so _sum/_count is the mean work per object
	// and rate(_sum) is the work rate a CPU rate is divided by.
	work map[string]prometheus.Histogram
}

// workCounters are the R2 series, by name suffix.
var workCounters = []struct{ name, help string }{
	{"txs", "Transactions in the object."},
	{"inputs", "Inputs across every transaction in the object."},
	{"scripts", "Input scripts the verifier ran (inputs of unproven transactions). Fewer than the unproven inputs when a refusal stopped it early."},
	{"proofs", "Transactions in the object carrying a merkle path."},
	{"ancestor_depth", "Longest chain of in-object parents: 1 for a child of a proven parent, N for N unproven generations."},
}

func newTimings() *timings {
	t := &timings{
		verify: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "overlay_bridge_verify_duration_seconds",
			Help:    "One SPV verification of a delivered object by the bridge (go-sdk): every merkle path against the bridge's own header store, and every unproven transaction's scripts. result: ok | refused (invalid on its merits) | error (no verdict: tracker failure, header not yet arrived, deadline).",
			Buckets: verifyBuckets,
		}, []string{"result"}),
		submit: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "overlay_bridge_engine_submit_duration_seconds",
			Help:    "Lane arrival to the engine's answer, per topic submit: queueing, the bridge's verification and the engine's whole submit (its own verification included). outcome: admitted | empty | error | refused (engine 4xx) | engine_error (unreachable or 5xx after retries).",
			Buckets: submitBuckets,
		}, []string{"outcome"}),
	}
	t.work = make(map[string]prometheus.Histogram, len(workCounters))
	for _, w := range workCounters {
		t.work[w.name] = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "overlay_bridge_verify_object_" + w.name,
			Help:    "Per verified object (R2 work counter): " + w.help,
			Buckets: workBuckets,
		})
	}
	for _, r := range []spv.Result{spv.ResultOK, spv.ResultRefused, spv.ResultError} {
		t.verify.WithLabelValues(string(r))
	}
	for _, o := range []string{feed.OutcomeAdmitted, feed.OutcomeEmpty, feed.OutcomeError, feed.OutcomeRefused, feed.OutcomeEngineError} {
		t.submit.WithLabelValues(o)
	}
	return t
}

func (t *timings) onVerified(vd spv.Verdict) {
	t.verify.WithLabelValues(string(vd.Result)).Observe(vd.Duration.Seconds())
	for name, v := range map[string]int{
		"txs": vd.Transactions, "inputs": vd.Inputs, "scripts": vd.Scripts,
		"proofs": vd.Proofs, "ancestor_depth": vd.Depth,
	} {
		t.work[name].Observe(float64(v))
	}
}

// collectors lists every vector for registration.
func (t *timings) collectors() []prometheus.Collector {
	cs := []prometheus.Collector{t.verify, t.submit}
	for _, w := range workCounters {
		cs = append(cs, t.work[w.name])
	}
	return cs
}

func (t *timings) onSubmitted(outcome string, d time.Duration) {
	t.submit.WithLabelValues(outcome).Observe(d.Seconds())
}
