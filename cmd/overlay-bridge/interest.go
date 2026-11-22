package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/shard-common/interest"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/lanes"
)

// lanesBound waits until every lane listens, or ctx is done.
func lanesBound(ctx context.Context, ls ...*lanes.Lane) bool {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		all := true
		for _, l := range ls {
			all = all && l.Bound()
		}
		if all {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}

// announcer tells the delivery edge which topics this bridge needs: topic
// interest (shard-common/interest).
//
// -topics is the only topic name map the bridge has, and the consumer's
// election lives in an account the bridge does not own. The two can drift,
// and a topic the edge is not delivering is discarded in silence here. With
// an announcer the bridge says what it needs and the edge answers for each
// topic: served, or the reason it is not. A topic the election does not
// cover is then a line in this log at the moment it happens, and where the
// account has an interest quota it is simply delivered.
type announcer struct {
	client   *interest.Client
	bind     netip.AddrPort
	withdraw bool
	log      *slog.Logger

	// How often the edges' state is checked for logging.
	watchEvery time.Duration

	mu        sync.Mutex
	names     map[interest.TopicID]string
	status    map[interest.TopicID]interest.Status
	sendErrs  int       // failed sends not yet logged
	sendErrAt time.Time // when one was last logged
}

// maxLifetime is the longest lifetime the wire can ask for; the next value
// up asks for the edge's default.
const maxLifetime = time.Duration(interest.LifetimeDefault-1) * time.Second

// newAnnouncer returns nil when -interest-upstream is not set.
func newAnnouncer(c config, names []string, log *slog.Logger) (*announcer, error) {
	if strings.TrimSpace(c.interestUpstream) == "" {
		return nil, nil
	}
	bind, err := netip.ParseAddrPort(c.interestBind)
	if err != nil {
		return nil, fmt.Errorf("-interest-bind %q: %w", c.interestBind, err)
	}
	// Resolve each upstream in the family the bind address sends from. A
	// name with both kinds of address would otherwise resolve to IPv4,
	// which a socket bound to an IPv6 address cannot reach.
	network, family := "udp", ""
	switch ba := bind.Addr().Unmap(); {
	case ba.Is4():
		network, family = "udp4", "IPv4"
	case !ba.IsUnspecified():
		network, family = "udp6", "IPv6"
	}
	var ups []netip.AddrPort
	for _, f := range splitList(c.interestUpstream) {
		ua, err := net.ResolveUDPAddr(network, f)
		if err != nil {
			if family != "" {
				return nil, fmt.Errorf("-interest-upstream %q: %w; -interest-bind sends %s only", f, err, family)
			}
			return nil, fmt.Errorf("-interest-upstream %q: %w", f, err)
		}
		ap := ua.AddrPort()
		if ap.Port() == 0 || ap.Addr().IsUnspecified() {
			return nil, fmt.Errorf("-interest-upstream %q: need an address and a port", f)
		}
		ups = append(ups, ap)
	}
	if c.interestLifetime < 0 || c.interestLifetime%time.Second != 0 {
		return nil, fmt.Errorf("-interest-lifetime %s: whole seconds, or 0 for the edge's default", c.interestLifetime)
	}
	if c.interestLifetime > maxLifetime {
		return nil, fmt.Errorf("-interest-lifetime %s: at most %s", c.interestLifetime, maxLifetime)
	}
	a := &announcer{
		bind: bind, withdraw: c.interestWithdraw, log: log, watchEvery: time.Second,
		names:  make(map[interest.TopicID]string, len(names)),
		status: make(map[interest.TopicID]interest.Status, len(names)),
	}
	a.client = interest.NewClient(interest.ClientConfig{
		SenderID:  interest.NewSenderID(c.interestLabel),
		Upstreams: ups,
		Lifetime:  uint32(c.interestLifetime / time.Second),
		OnStatus:  a.onStatus,
	})
	for _, n := range names {
		id := interest.TopicID(objfmt.TopicID(n))
		a.names[id] = n
		a.status[id] = interest.StatusPending
		a.client.Add(id)
	}
	log.Info("topic interest is ON", "upstreams", ups, "announce_from", bind.String(), "topics", len(names))
	return a, nil
}

// onStatus reports the edge's answer for a topic, loudly when it means
// objects are not arriving.
func (a *announcer) onStatus(id interest.TopicID, st interest.Status) {
	a.mu.Lock()
	name := a.names[id]
	prev := a.status[id]
	a.status[id] = st
	a.mu.Unlock()
	switch {
	case st.Served():
		if !prev.Served() {
			a.log.Info("topic served by the edge", "topic", name)
		}
	case st == interest.StatusPending:
	case st == interest.StatusOutsideBound:
		a.log.Warn("TOPIC NOT DELIVERED: it is not in the consumer's election and the account has no interest quota",
			"topic", name,
			"consequence", "the edge delivers nothing for this topic; objects published to it never reach this bridge",
			"fix", "add the topic to the consumer's election, or have an interest quota granted")
	case st == interest.StatusOverCap:
		a.log.Warn("TOPIC NOT DELIVERED: the consumer's interest quota is used up",
			"topic", name,
			"consequence", "asked again at every refresh; delivered as soon as quota is free",
			"fix", "add the topic to the consumer's election, or have the quota raised")
	case st == interest.StatusNotServed || st == interest.StatusUnserved:
		a.log.Warn("TOPIC NOT DELIVERED: no path this bridge announces to delivers the topic's group",
			"topic", name,
			"consequence", "no stream exists for this topic on this tunnel")
	default:
		a.log.Warn("TOPIC NOT DELIVERED", "topic", name, "reason", st.String())
	}
}

// Serve announces until ctx is done. The announcer is not on the data path:
// a socket that cannot be opened, or that fails, is logged and opened again,
// and never stops the bridge.
func (a *announcer) Serve(ctx context.Context) error {
	go a.watch(ctx)
	wait := time.Second
	for {
		start := time.Now()
		err := a.serveOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(start) > time.Minute {
			wait = time.Second
		}
		a.log.Error("TOPIC INTEREST STOPPED: the announcer's socket failed",
			"err", err, "retry_in", wait,
			"consequence", "the edge keeps what was announced until its lifetime runs out; nothing new is announced meanwhile",
			"fix", "-interest-bind must be an address of this host; it is tried again")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		if wait *= 2; wait > 30*time.Second {
			wait = 30 * time.Second
		}
	}
}

func (a *announcer) serveOnce(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(a.bind))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return interest.RunClient(ctx, conn, a.client, interest.RunOptions{
		// Off by default: a bridge that restarts wants what it needs to keep
		// arriving meanwhile, and what it announced lapses by itself if it
		// does not come back.
		WithdrawOnExit: a.withdraw,
		OnError:        a.sendError,
	})
}

// sendError logs a failed send: the first at once, then at most one line a
// minute with the number that failed, so a bridge that cannot reach its edge
// says so without flooding the log.
func (a *announcer) sendError(err error) {
	a.mu.Lock()
	a.sendErrs++
	now := time.Now()
	if now.Sub(a.sendErrAt) < time.Minute {
		a.mu.Unlock()
		return
	}
	n := a.sendErrs
	a.sendErrs, a.sendErrAt = 0, now
	a.mu.Unlock()
	a.log.Warn("TOPIC INTEREST: cannot send to the edge", "err", err, "failed", n,
		"consequence", "the edge is not told what this bridge needs",
		"fix", "check the route from -interest-bind to -interest-upstream")
}

// watch logs what the edges' silence means.
//
// Every edge silent is a warning: nothing the bridge announces is applied,
// what the edges hold for it lapses when its lifetime runs out, and each
// topic keeps its last answer meanwhile, so without this line a bridge whose
// edge has gone would say nothing.
//
// One edge silent while another answers is not. A standby edge holds no
// record of a consumer it is not delivering to, so it stays silent until
// delivery moves to it, and the edge that delivered goes silent once delivery
// has moved away. A warning there would fire on every bridge that names both
// of its edges, as it should, and teach its operator to ignore the one that
// matters. So each such edge gets one line at Info.
//
// An idle bridge sends only at each refresh, so a silence is noticed then.
func (a *announcer) watch(ctx context.Context) {
	t := time.NewTicker(a.watchEvery)
	defer t.Stop()
	w := newEdgeWatch(a.log)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		w.step(a.client.Upstreams())
	}
}

// edgeWatch turns successive looks at the edges into log lines, as watch
// describes. It keeps no clock, so a test can drive it a step at a time.
type edgeWatch struct {
	log      *slog.Logger
	answered map[netip.AddrPort]bool // has answered at least once
	noted    map[netip.AddrPort]bool // its silence has been logged
	warned   bool                    // the last look found no edge answering
}

func newEdgeWatch(log *slog.Logger) *edgeWatch {
	return &edgeWatch{log: log, answered: map[netip.AddrPort]bool{}, noted: map[netip.AddrPort]bool{}}
}

func (w *edgeWatch) step(ups []interest.UpstreamInfo) {
	reachable, silent := 0, 0
	for _, u := range ups {
		switch {
		case u.Reachable:
			reachable++
		case isSilent(u):
			silent++
		}
	}
	// No edge answers once one has shown it is silent and none answers: then
	// nothing this bridge announces is applied, whether or not every edge
	// has reached the silent mark yet.
	noneAnswers := reachable == 0 && silent > 0
	for _, u := range ups {
		addr := u.Addr.String()
		switch {
		case u.Reachable:
			// Said for an edge whose silence was logged, and for the first edge
			// to answer after the warning, so the warning never stands alone.
			if w.noted[u.Addr] || w.warned {
				w.log.Info("topic interest: the edge answers", "upstream", addr)
			}
			w.noted[u.Addr], w.answered[u.Addr] = false, true
		case isSilent(u) && !w.noted[u.Addr] && !noneAnswers:
			w.noted[u.Addr] = true
			if w.answered[u.Addr] {
				w.log.Info("topic interest: an edge stopped answering while another answers",
					"upstream", addr, "consequence", "the edge that answers holds this bridge's interest")
			} else {
				w.log.Info("topic interest: an edge does not answer while another does, as a standby edge does until delivery moves to it",
					"upstream", addr)
			}
		}
	}
	if noneAnswers && !w.warned {
		names := make([]string, 0, len(ups))
		for _, u := range ups {
			names = append(names, u.Addr.String())
			if isSilent(u) {
				w.noted[u.Addr] = true
			}
		}
		w.log.Warn("TOPIC INTEREST: no edge answers", "upstreams", strings.Join(names, ","),
			"consequence", "nothing this bridge announces is applied, and what the edges hold for it lapses when its lifetime runs out",
			"fix", "check that the edges listen for interest and that the tunnel passes it")
	}
	w.warned = noneAnswers
}

// isSilent reports an upstream that has stopped answering, or never has.
// The client marks an edge that has answered unreachable after three
// timeouts in a row. Before its first answer an edge is sent one probe at a
// time, so three timeouts are three unanswered probes in a row there too.
func isSilent(u interest.UpstreamInfo) bool { return !u.Reachable && u.Stats.Timeouts >= 3 }

var (
	descInterestTopics = prometheus.NewDesc("overlay_bridge_interest_topics",
		"Topics this bridge announces to its edge, by the edge's answer: ok (served), pending (no answer yet), outside_bound (not in the election and no quota), over_cap (quota used up), not_served (no announced path delivers the topic's group), or another refusal. Anything but ok and pending is a topic whose objects are not arriving.",
		[]string{"status"}, nil)
	descInterestUp = prometheus.NewDesc("overlay_bridge_interest_upstream",
		"State of each edge this bridge announces to: reachable (it has answered since the last three consecutive losses), synced (it holds exactly what this bridge announced), held (topics it holds for this bridge).",
		[]string{"upstream", "state"}, nil)
	descInterestEvents = prometheus.NewDesc("overlay_bridge_interest_events_total",
		"Exchanges with each edge that went other than plainly: timeout (unanswered), sync (a full replay), token (first contact, or the edge restarted or changed), mismatch (the edge's set differed from the one expected).",
		[]string{"upstream", "event"}, nil)
)

// interestStatuses are exported at zero so a rule over them is defined
// before anything is refused.
var interestStatuses = []interest.Status{
	interest.StatusOK, interest.StatusPending, interest.StatusOutsideBound,
	interest.StatusOverCap, interest.StatusNotServed,
}

func (a *announcer) describe(ch chan<- *prometheus.Desc) {
	ch <- descInterestTopics
	ch <- descInterestUp
	ch <- descInterestEvents
}

func (a *announcer) collect(ch chan<- prometheus.Metric) {
	counts := map[string]int{}
	for _, st := range interestStatuses {
		counts[st.String()] = 0
	}
	a.mu.Lock()
	for _, st := range a.status {
		if st == interest.StatusAddressNotHonored {
			st = interest.StatusOK
		}
		counts[st.String()]++
	}
	a.mu.Unlock()
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		gauge(ch, descInterestTopics, float64(counts[k]), k)
	}
	flag := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	for _, u := range a.client.Upstreams() {
		addr := u.Addr.String()
		gauge(ch, descInterestUp, flag(u.Reachable), addr, "reachable")
		gauge(ch, descInterestUp, flag(u.Synced), addr, "synced")
		gauge(ch, descInterestUp, float64(u.Held), addr, "held")
		counter(ch, descInterestEvents, u.Stats.Timeouts, addr, "timeout")
		counter(ch, descInterestEvents, u.Stats.Syncs, addr, "sync")
		counter(ch, descInterestEvents, u.Stats.Tokens, addr, "token")
		counter(ch, descInterestEvents, u.Stats.Mismatches, addr, "mismatch")
	}
}
