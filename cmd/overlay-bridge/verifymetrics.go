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

// timings are the bridge's two histograms. They are observed, not derived
// from Stats, so they are real prometheus vectors beside the collector.
type timings struct {
	verify *prometheus.HistogramVec
	submit *prometheus.HistogramVec
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
}

func (t *timings) onSubmitted(outcome string, d time.Duration) {
	t.submit.WithLabelValues(outcome).Observe(d.Seconds())
}
