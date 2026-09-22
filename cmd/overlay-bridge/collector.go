package main

import (
	"net/http"

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
		"Roots answered, by where the answer came from. An all-fallback run has not demonstrated that the lane feeds verification.",
		[]string{"source"}, nil)
	descFacade = prometheus.NewDesc("overlay_bridge_facade_total",
		"Facade submissions, by result.", []string{"result"}, nil)
	descQueue = prometheus.NewDesc("overlay_bridge_publish_total",
		"Up-tunnel publications, by result. A shed record is NOT on the plane and is not billed.",
		[]string{"result"}, nil)
	descQueueDepth = prometheus.NewDesc("overlay_bridge_publish_queue_depth",
		"Instantaneous publish backlog.", nil, nil)
	descGuard = prometheus.NewDesc("overlay_bridge_guard_entries",
		"Loop-guard entries retained.", nil, nil)
	descLane = prometheus.NewDesc("overlay_bridge_lane_objects_total",
		"Objects read off a lane, by lane and outcome.", []string{"lane", "outcome"}, nil)
	descLaneConns = prometheus.NewDesc("overlay_bridge_lane_connections_active",
		"Connections open on a lane right now.", []string{"lane"}, nil)
	descLaneBound = prometheus.NewDesc("overlay_bridge_lane_bound",
		"1 when a lane's listener is open.", []string{"lane"}, nil)
)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descFeed, descSteak, descHeaders, descTip, descRoots, descFacade,
		descQueue, descQueueDepth, descGuard, descLane, descLaneConns, descLaneBound,
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

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	if c.feed != nil {
		s := c.feed.Stats()
		counter(ch, descFeed, s.Submitted, "submitted")
		counter(ch, descFeed, s.EngineError, "engine_error")
		counter(ch, descFeed, s.ParseError, "parse_error")
		counter(ch, descFeed, s.UnknownTopic, "unknown_topic")
		counter(ch, descFeed, s.Rejected, "rejected")
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
	if c.tracker != nil {
		s := c.tracker.Stats()
		counter(ch, descRoots, s.FromLane, "lane")
		counter(ch, descRoots, s.FromFallback, "fallback")
		counter(ch, descRoots, s.Misses, "miss")
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
		gauge(ch, descQueueDepth, float64(s.Depth))
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
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
