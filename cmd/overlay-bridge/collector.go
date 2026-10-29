package main

import (
	"net/http"
	"runtime"
	"runtime/debug"

	"github.com/lightwebinc/overlay-bridge/facade"
	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/overlay-bridge/headers"
	"github.com/lightwebinc/overlay-bridge/uptunnel"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// collector publishes every package's snapshot counters.
//
// It is a prometheus.Collector over Stats() rather than a set of counters the
// packages increment directly, so the packages stay free of a metrics
// dependency and a counter cannot drift from the value its own package
// reports.
//
// Neither overlay engine exposes a metrics registry, so what this publishes is
// the only quantitative view of the bridge's side of the seam.
type collector struct {
	feed    *feed.Feed
	guard   *guard.Guard
	store   *headers.Store
	tracker *headers.Tracker
	facade  *facade.Facade
	queue   *uptunnel.Queue
	lanes   []*lanes.Lane
	timings *timings
	verify  bool
	// config is the overlay_bridge_config_info label set, in descConfig's
	// order; nil omits the series.
	config []string
}

var (
	descFeed = prometheus.NewDesc("overlay_bridge_feed_total",
		"Delivery records handled, by outcome.", []string{"outcome"}, nil)
	descSteak = prometheus.NewDesc("overlay_bridge_engine_submits_total",
		"Engine submits, by topic and admittance outcome.", []string{"topic", "outcome"}, nil)
	descHeaders = prometheus.NewDesc("overlay_bridge_headers_total",
		"Headers seen on the lane, by outcome.", []string{"outcome"}, nil)
	descTip = prometheus.NewDesc("overlay_bridge_header_tip_height",
		"Best chained block height. A flat value with a live lane is the outage signature.", nil, nil)
	descRoots = prometheus.NewDesc("overlay_bridge_tracker_roots_total",
		"Roots answered, by where the answer came from: lane = a header this host received on its header lane; history = the anchor's answer for a height the lane could not have carried to this process (at or below the startup anchor, or pruned out of -header-window); fallback = the anchor's answer for a height the lane should have carried (a gap, a re-anchored parent, a lane that stopped); miss = no answer. Only fallback means verification has moved off the lane.",
		[]string{"source"}, nil)
	descFacade = prometheus.NewDesc("overlay_bridge_facade_total",
		"Facade submissions, by result.", []string{"result"}, nil)
	descQueue = prometheus.NewDesc("overlay_bridge_publish_total",
		"Up-tunnel publications, by result. sent = confirmed by the ingress, never just written locally; resent = written again after the connection it was on died; failed = not written, or lost after its resends. A shed record is NOT on the plane and is not billed.",
		[]string{"result"}, nil)
	descUnconfirmed = prometheus.NewDesc("overlay_bridge_publish_unconfirmed",
		"Records written to the up-tunnel and not yet confirmed by the ingress.", nil, nil)
	descPeerClosed = prometheus.NewDesc("overlay_bridge_uptunnel_peer_closed_total",
		"Up-tunnel connections the ingress closed or that failed under the bridge (EOF, reset, keepalive or user timeout).", nil, nil)
	descQueueDepth = prometheus.NewDesc("overlay_bridge_publish_queue_depth",
		"Instantaneous publish backlog.", nil, nil)
	descGuard = prometheus.NewDesc("overlay_bridge_guard_entries",
		"Loop-guard entries retained.", nil, nil)
	descLane = prometheus.NewDesc("overlay_bridge_lane_objects_total",
		"Objects read off a lane, by lane and outcome.", []string{"lane", "outcome"}, nil)
	descLaneConns = prometheus.NewDesc("overlay_bridge_lane_connections_active",
		"Connections open on a lane right now.", []string{"lane"}, nil)
	descBuild = prometheus.NewDesc("overlay_bridge_build_info",
		"Always 1. Labels carry what this BINARY links, read from the embedded build info, not from go.mod. A deployed binary keeps the dependency it was built with for ever and nothing else on the host says which.",
		[]string{"version", "shard_common", "go_sdk", "go_version"}, nil)
	descVerifyScripts = prometheus.NewDesc("overlay_bridge_verify_scripts_total",
		"Input scripts the bridge's SPV verifier ran (inputs of unproven transactions in delivered objects).", nil, nil)
	descMismatch = prometheus.NewDesc("overlay_bridge_verify_mismatch_total",
		"Shadow-mode disagreements between the bridge's SPV verdict and the engine's answer, by kind. bridge_ok_engine_refused must stay zero before enforce.",
		[]string{"kind"}, nil)
	descShadow = prometheus.NewDesc("overlay_bridge_shadow_total",
		"Topic submits offered to a shadow engine, by route (default = -shadow-engine, else the -engine-route glob) and by comparison with the primary's answer: match | primary_only | shadow_only | differ | shadow_error (no answer to compare) | shadow_dropped (queue full; never offered). Present only with a shadow configured.",
		[]string{"route", "kind"}, nil)
	descLaneBound = prometheus.NewDesc("overlay_bridge_lane_bound",
		"1 when a lane's listener is open.", []string{"lane"}, nil)
	descWarm = prometheus.NewDesc("overlay_bridge_header_warm_heights",
		"Startup header warm-up: heights it set out to load (target), loaded from the anchor (loaded) and not answered (failed; fetched on first use). loaded/target is its progress.",
		[]string{"state"}, nil)
	descWarmDuration = prometheus.NewDesc("overlay_bridge_header_warm_duration_seconds",
		"Wall time of the startup header warm-up; 0 until it has finished or when it is off.", nil, nil)
	descEngineQueue = prometheus.NewDesc("overlay_bridge_engine_queue_depth",
		"Deliveries queued behind the engine workers right now. At overlay_bridge_engine_queue_capacity the next delivery is shed.", nil, nil)
	descEngineQueueCap = prometheus.NewDesc("overlay_bridge_engine_queue_capacity",
		"Size of the engine queue (-engine-queue).", nil, nil)
	descEngineQueueBytes = prometheus.NewDesc("overlay_bridge_engine_queue_bytes",
		"Bytes of the deliveries queued behind the engine workers right now.", nil, nil)
	descEngineQueueBytesCap = prometheus.NewDesc("overlay_bridge_engine_queue_bytes_capacity",
		"The engine queue's byte bound (-engine-queue-bytes); 0 is none. Past it the next delivery is shed.", nil, nil)
	descWorkers = prometheus.NewDesc("overlay_bridge_engine_workers",
		"Engine workers configured (-engine-workers).", nil, nil)
	descWorkersBusy = prometheus.NewDesc("overlay_bridge_engine_workers_busy",
		"Engine workers busy right now, by route (default = -engine, else the -engine-route glob) and stage: verify (the bridge's SPV, counted under the route of the topic the object was delivered under) | submit (waiting on that route's engine, retries and backoff included). Workers minus the sum is idle.",
		[]string{"route", "stage"}, nil)
	descShedRoute = prometheus.NewDesc("overlay_bridge_feed_shed_total",
		"Deliveries shed because the engine queue was full, by the route of the topic the plane delivered them under. Sums to overlay_bridge_feed_total{outcome=\"shed\"}.",
		[]string{"route"}, nil)
	descShadowQueue = prometheus.NewDesc("overlay_bridge_shadow_queue_depth",
		"Topic submits queued for a shadow engine right now, by route. At overlay_bridge_shadow_queue_capacity the next offer is dropped (shadow_dropped).",
		[]string{"route"}, nil)
	descShadowQueueCap = prometheus.NewDesc("overlay_bridge_shadow_queue_capacity",
		"Size of a shadow engine's queue (-shadow-queue), by route.", []string{"route"}, nil)
	descAlso = prometheus.NewDesc("overlay_bridge_also_total",
		"Objects handed to a route's also= target (the site's other local subscriber), by route glob, target origin and result: ok | refused (a 4xx; not asked again) | error (failed or unreachable on every try) | shed (queue full by count or bytes; never offered).",
		[]string{"route", "target", "result"}, nil)
	descAlsoQueue = prometheus.NewDesc("overlay_bridge_also_queue_bytes",
		"Bytes queued for, or in the hands of, an also= target's workers right now. Past overlay_bridge_also_queue_bytes_capacity the next object is shed.",
		[]string{"route", "target"}, nil)
	descAlsoQueueCap = prometheus.NewDesc("overlay_bridge_also_queue_bytes_capacity",
		"An also= target's byte bound (-also-queue-bytes).", []string{"route", "target"}, nil)
	descConfig = prometheus.NewDesc("overlay_bridge_config_info",
		"Always 1. Labels carry the run settings a measurement must be stamped with (R7): verify mode, network and finality, engine workers, GOMAXPROCS, GOGC, GOMEMLIMIT and the ECDSA verifier (-sig-verifier).",
		[]string{"verify", "verify_network", "verify_finality", "engine_workers", "gomaxprocs", "gogc", "gomemlimit", "sig_verifier"}, nil)
)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descFeed, descSteak, descHeaders, descTip, descRoots, descFacade,
		descQueue, descQueueDepth, descUnconfirmed, descPeerClosed, descGuard, descLane, descLaneBound, descLaneConns,
		descBuild, descVerifyScripts, descMismatch, descShadow, descWarm, descWarmDuration, descConfig,
		descEngineQueue, descEngineQueueCap, descEngineQueueBytes, descEngineQueueBytesCap, descWorkers, descWorkersBusy, descShedRoute, descShadowQueue, descShadowQueueCap,
		descAlso, descAlsoQueue, descAlsoQueueCap,
	} {
		ch <- d
	}
}

func counter(ch chan<- prometheus.Metric, d *prometheus.Desc, v uint64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
}

func gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

// buildLabels reads what this BINARY actually links, which is not what any
// go.mod says.
//
// This exists because of a real outage. On 2026-09-23 both bridges refused
// 100% of delivered objects for about ninety minutes: the deployed binary
// linked shard-common v0.21.0 while go.mod required v0.22.0, and v0.21.0 has
// no SplitBEEFPayload, so the record the edge now delivers read as a malformed
// object. Nothing on the host said which version was linked. The only way to
// find it was `go version -m` on the binary, over ssh, once somebody already
// suspected it.
//
// Publishing it makes version skew QUERYABLE: two hosts disagreeing, or a host
// running something other than what was released, is a PromQL question rather
// than an investigation. The deps are read from the build info the linker
// embeds, so this cannot drift from what is actually running the way a
// hand-set version string does.
func buildLabels() (version, shardCommon, goSDK string) {
	// Prefer the linker-stamped Version over the module's own.
	//
	// debug.ReadBuildInfo reports Main.Version as "(devel)" for anything not
	// built as a tagged module download, which is every binary we actually
	// deploy. A build_info series whose version label reads "(devel)" on every
	// host cannot answer the question it exists for — "is this host running
	// the release I think it is" — so the Makefile's -X main.Version stamp
	// wins whenever it has been set.
	version, shardCommon, goSDK = "unknown", "unknown", "unknown"
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		if Version != "" {
			version = Version
		}
		return
	}
	switch {
	case Version != "" && Version != "dev":
		version = Version
	case bi.Main.Version != "" && bi.Main.Version != "(devel)":
		version = bi.Main.Version
	case Version != "":
		version = Version
	}
	for _, d := range bi.Deps {
		if d == nil {
			continue
		}
		// A replaced module reports the REPLACEMENT's version; that is the one
		// actually compiled in, so it is the one worth publishing.
		v := d.Version
		if d.Replace != nil && d.Replace.Version != "" {
			v = d.Replace.Version
		}
		switch d.Path {
		case "github.com/lightwebinc/shard-common":
			shardCommon = v
		case "github.com/bsv-blockchain/go-sdk":
			goSDK = v
		}
	}
	return
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	ver, sc, sdk := buildLabels()
	gauge(ch, descBuild, 1, ver, sc, sdk, runtime.Version())
	if c.config != nil {
		gauge(ch, descConfig, 1, c.config...)
	}

	if c.feed != nil {
		f := c.feed
		s := c.feed.Stats()
		counter(ch, descFeed, s.Submitted, "submitted")
		counter(ch, descFeed, s.EngineError, "engine_error")
		counter(ch, descFeed, s.ParseError, "parse_error")
		counter(ch, descFeed, s.UnknownTopic, "unknown_topic")
		counter(ch, descFeed, s.Rejected, "rejected")
		counter(ch, descFeed, s.Sunk, "sunk")
		counter(ch, descFeed, s.Shed, "shed")
		counter(ch, descFeed, s.Retried, "retried")
		counter(ch, descFeed, s.EngineRefused, "engine_refused")
		counter(ch, descFeed, s.SPVRefused, "spv_refused")
		if c.verify {
			counter(ch, descVerifyScripts, s.VerifyScripts)
			for _, k := range feed.MismatchKinds {
				counter(ch, descMismatch, s.Mismatch[k], k)
			}
		}
		for _, sh := range shadows(f) {
			// Preset every kind at zero: a shadow that has never matched
			// must show a zero match series, not no series.
			ss := sh.Stats()
			for _, k := range feed.ShadowKinds {
				counter(ch, descShadow, ss.Kinds[k], sh.Name, k)
			}
			queued, capacity := sh.Backlog()
			gauge(ch, descShadowQueue, float64(queued), sh.Name)
			gauge(ch, descShadowQueueCap, float64(capacity), sh.Name)
		}
		for _, a := range alsos(f) {
			as := a.Stats()
			for _, r := range feed.AlsoResults {
				counter(ch, descAlso, as.Results[r], a.Name, a.Target, r)
			}
			gauge(ch, descAlsoQueue, float64(as.QueuedBytes), a.Name, a.Target)
			gauge(ch, descAlsoQueueCap, float64(as.CapacityBytes), a.Name, a.Target)
		}
		if f.Submit != nil {
			// Where the workers' time goes: the queue in front of them, and
			// how many are verifying or waiting on each route's engine.
			// Every route and stage is preset at zero.
			queued, capacity := f.Backlog()
			gauge(ch, descEngineQueue, float64(queued))
			gauge(ch, descEngineQueueCap, float64(capacity))
			qb, qbCap := f.BacklogBytes()
			gauge(ch, descEngineQueueBytes, float64(qb))
			gauge(ch, descEngineQueueBytesCap, float64(qbCap))
			gauge(ch, descWorkers, float64(f.Workers))
			busy := f.Busy()
			for _, route := range f.RouteNames() {
				for _, stage := range feed.WorkerStages {
					gauge(ch, descWorkersBusy, float64(busy[feed.BusyKey{Route: route, Stage: stage}]), route, stage)
				}
				counter(ch, descShedRoute, s.ShedByRoute[route], route)
			}
		}
		// Preset every elected topic's outcomes at zero before emitting what
		// actually happened.
		//
		// Without this the steak series exist only for outcomes that have
		// OCCURRED, and that breaks the alert that matters most. A bridge
		// restarted INTO a state where the engine admits nothing never creates
		// an `admitted` series at all, so a rule of the form
		// `empty > 0 and on(instance) admitted == 0` finds no right-hand
		// vector and silently never fires — blind in exactly the scenario it
		// exists for. The same trap the rules file warns about in prose, here
		// in the emitter.
		seen := make(map[feed.SteakKey]bool, len(s.Steak))
		for k := range s.Steak {
			seen[k] = true
		}
		for _, name := range f.Topics {
			for _, oc := range []string{feed.OutcomeAdmitted, feed.OutcomeEmpty, feed.OutcomeError} {
				if k := (feed.SteakKey{Topic: name, Outcome: oc}); !seen[k] {
					counter(ch, descSteak, 0, name, oc)
				}
			}
		}
		for k, v := range s.Steak {
			counter(ch, descSteak, v, k.Topic, k.Outcome)
		}
	}
	if c.store != nil {
		s := c.store.Stats()
		counter(ch, descHeaders, s.Observed, "observed")
		counter(ch, descHeaders, s.Rejected, "rejected")
		counter(ch, descHeaders, s.Orphaned, "orphaned")
		counter(ch, descHeaders, s.Reanchored, "reanchored")
		counter(ch, descHeaders, s.Replaced, "replaced")
		counter(ch, descHeaders, s.Pruned, "pruned")
		gauge(ch, descTip, float64(s.TipHeight))
	}
	if c.store != nil {
		// Provenance comes from the store because that is the path the
		// engine's reads take (over the read API). An earlier version read it
		// from the in-process tracker, which the engine never calls, so the
		// series was zero for the life of the process and the claim it exists
		// to prove was undecidable from it.
		s := c.store.Stats()
		counter(ch, descRoots, s.FromLane, "lane")
		counter(ch, descRoots, s.FromHistory, "history")
		counter(ch, descRoots, s.FromFallback, "fallback")
		counter(ch, descRoots, s.Misses, "miss")
		w := c.store.WarmStats()
		gauge(ch, descWarm, float64(w.Target), "target")
		gauge(ch, descWarm, float64(w.Loaded), "loaded")
		gauge(ch, descWarm, float64(w.Failed), "failed")
		gauge(ch, descWarmDuration, w.Duration.Seconds())
	}
	if c.facade != nil {
		s := c.facade.Stats()
		counter(ch, descFacade, s.OK, "ok")
		counter(ch, descFacade, s.Malformed, "malformed")
		counter(ch, descFacade, s.UnknownTopic, "unknown_topic")
		counter(ch, descFacade, s.LoopDropped, "loop_dropped")
		counter(ch, descFacade, s.EngineError, "engine_error")
		counter(ch, descFacade, s.PublishFailed, "publish_failed")
	}
	if c.queue != nil {
		s := c.queue.Stats()
		counter(ch, descQueue, s.Enqueued, "enqueued")
		counter(ch, descQueue, s.Sent, "sent")
		counter(ch, descQueue, s.Shed, "shed")
		counter(ch, descQueue, s.Failed, "failed")
		counter(ch, descQueue, s.Resent, "resent")
		gauge(ch, descQueueDepth, float64(s.Depth))
		gauge(ch, descUnconfirmed, float64(s.Unconfirmed))
		counter(ch, descPeerClosed, s.PeerClosed)
	}
	if c.guard != nil {
		gauge(ch, descGuard, float64(c.guard.Stats().Entries))
	}
	for _, l := range c.lanes {
		s := l.Stats()
		counter(ch, descLane, s.Objects, l.Name, "objects")
		counter(ch, descLane, s.Errors, l.Name, "errors")
		counter(ch, descLane, s.Rejected, l.Name, "rejected")
		counter(ch, descLane, s.Bytes, l.Name, "bytes")
		gauge(ch, descLaneConns, float64(s.Active), l.Name)
		bound := 0.0
		if l.Bound() {
			bound = 1
		}
		gauge(ch, descLaneBound, bound, l.Name)
	}
}

// ready reports whether every lane's listener is open. Readiness gates on it:
// until the lanes are bound the bridge cannot accept delivery, and reporting
// ready before then invites an edge to dial a socket that is not there.
func (c *collector) ready() bool {
	for _, l := range c.lanes {
		if !l.Bound() {
			return false
		}
	}
	return true
}

func (c *collector) handler() http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	if c.timings != nil {
		reg.MustRegister(c.timings.collectors()...)
	}
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
