package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/lightwebinc/shard-common/interest"
	"github.com/lightwebinc/shard-common/objfmt"
)

// edgePolicy is a delivery edge's answers, reduced to an election and a
// quota.
type edgePolicy struct {
	listed map[interest.TopicID]bool
	quota  int
}

func (p *edgePolicy) Identify(netip.AddrPort) (string, bool) { return "consumer", true }
func (p *edgePolicy) Scope(string) *interest.Scope           { return nil }
func (p *edgePolicy) Bound(_ string, t interest.TopicID) interest.Bound {
	if p.listed[t] {
		return interest.BoundFree
	}
	return interest.BoundQuota
}
func (p *edgePolicy) Quota(string) int { return p.quota }

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startEdge(t *testing.T, pol *edgePolicy) (*interest.Receiver, string) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	rcv := interest.NewReceiver(interest.ReceiverConfig{Policy: pol})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = interest.ServeReceiver(ctx, conn, rcv, interest.ServeOptions{Tick: 20 * time.Millisecond})
	}()
	t.Cleanup(func() { cancel(); <-done; _ = conn.Close() })
	return rcv, conn.LocalAddr().String()
}

func gather(t *testing.T, a *announcer) map[string]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(&collector{interest: a})
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "overlay_bridge_interest") {
			continue
		}
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range m.GetLabel() {
				if l.GetName() != "upstream" {
					key += "/" + l.GetValue()
				}
			}
			out[key] = value(m)
		}
	}
	return out
}

func value(m *dto.Metric) float64 {
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

func TestAnnouncerOffByDefault(t *testing.T) {
	a, err := newAnnouncer(config{interestBind: "[::]:0"}, []string{"tm_a"}, slog.Default())
	if err != nil || a != nil {
		t.Fatalf("with no upstream the announcer must be off: %v %v", a, err)
	}
	// And the collector must not mention it.
	reg := prometheus.NewRegistry()
	reg.MustRegister(&collector{})
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), "overlay_bridge_interest") {
			t.Fatalf("%s exported with the announcer off", mf.GetName())
		}
	}
}

func TestAnnouncerRejectsBadFlags(t *testing.T) {
	cases := map[string]config{
		"-interest-upstream": {interestUpstream: "nonsense", interestBind: "[::]:0"},
		"address and a port": {interestUpstream: "[::]:8729", interestBind: "[::]:0"},
		"-interest-bind":     {interestUpstream: "[::1]:8729", interestBind: "nonsense"},
		"-interest-lifetime": {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestLifetime: 1500 * time.Millisecond},
		"at most":            {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestLifetime: maxLifetime + time.Second},
		"sends IPv6 only":    {interestUpstream: "192.0.2.1:8729", interestBind: "[2001:db8::1]:0"},
	}
	for want, c := range cases {
		c.interestLabel = "t"
		if _, err := newAnnouncer(c, []string{"tm_a"}, slog.Default()); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%+v: error %v, want it to mention %q", c, err, want)
		}
	}
}

// The bridge announces what -topics needs. The edge serves what the election
// covers and what the quota allows, and the bridge says in its log, by
// name, which topics are not arriving and why.
func TestAnnouncerReportsWhatIsNotDelivered(t *testing.T) {
	pol := &edgePolicy{listed: map[interest.TopicID]bool{
		interest.TopicID(objfmt.TopicID("tm_elected")): true,
	}, quota: 1}
	rcv, edge := startEdge(t, pol)

	var logs syncBuffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	a, err := newAnnouncer(config{
		interestUpstream: edge, interestBind: "[::1]:0", interestLabel: "bridge-test",
	}, []string{"tm_elected", "tm_quota_1", "tm_quota_2"}, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		m := gather(t, a)
		if m["overlay_bridge_interest_topics/ok"] == 2 && m["overlay_bridge_interest_topics/over_cap"] == 1 &&
			m["overlay_bridge_interest_topics/pending"] == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("answers did not settle: %v\n%s", m, logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	m := gather(t, a)
	for key, want := range map[string]float64{
		"overlay_bridge_interest_topics/outside_bound":  0,
		"overlay_bridge_interest_topics/not_served":     0,
		"overlay_bridge_interest_upstream/reachable":    1,
		"overlay_bridge_interest_upstream/synced":       1,
		"overlay_bridge_interest_upstream/held":         2,
		"overlay_bridge_interest_events_total/mismatch": 0,
	} {
		if m[key] != want {
			t.Errorf("%s = %v, want %v", key, m[key], want)
		}
	}
	// One replay, unless a loaded host delayed an answer past the timeout.
	if m["overlay_bridge_interest_events_total/timeout"] == 0 && m["overlay_bridge_interest_events_total/sync"] != 1 {
		t.Errorf("%v replays without a timeout, want 1", m["overlay_bridge_interest_events_total/sync"])
	}
	if got := len(rcv.Live("consumer")); got != 2 {
		t.Errorf("the edge holds %d topics for the bridge, want 2", got)
	}
	out := logs.String()
	if !strings.Contains(out, "topic served by the edge") || !strings.Contains(out, "tm_elected") {
		t.Errorf("a served topic was not reported:\n%s", out)
	}
	if !strings.Contains(out, "interest quota is used up") || !strings.Contains(out, "tm_quota_2") {
		t.Errorf("the topic over the quota was not named:\n%s", out)
	}

	// No quota at all: the topic is outside the bound, and the log says how
	// to fix it.
	pol2 := &edgePolicy{listed: map[interest.TopicID]bool{}}
	_, edge2 := startEdge(t, pol2)
	var logs2 syncBuffer
	b, err := newAnnouncer(config{interestUpstream: edge2, interestBind: "[::1]:0", interestLabel: "bridge-test-2"},
		[]string{"tm_unelected"}, slog.New(slog.NewTextHandler(&logs2, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- b.Serve(ctx2) }()
	deadline = time.Now().Add(5 * time.Second)
	for gather(t, b)["overlay_bridge_interest_topics/outside_bound"] != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("no answer: %s", logs2.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if out := logs2.String(); !strings.Contains(out, "not in the consumer's election") || !strings.Contains(out, "tm_unelected") {
		t.Errorf("the unelected topic was not explained:\n%s", out)
	}

	cancel()
	cancel2()
	if err := <-done; err != nil {
		t.Errorf("serve: %v", err)
	}
	if err := <-done2; err != nil {
		t.Errorf("serve: %v", err)
	}
	// A bridge that stops does not withdraw: what it announced stays at the
	// edge for its lifetime, so a restart loses nothing.
	time.Sleep(100 * time.Millisecond)
	if got := len(rcv.Live("consumer")); got != 2 {
		t.Errorf("the edge holds %d topics after the bridge stopped, want 2 still", got)
	}
}

func waitFor(t *testing.T, logs *syncBuffer, s string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), s) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q in the log:\n%s", s, logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A socket the announcer cannot open is logged and tried again. It never
// stops the bridge.
func TestAnnouncerSurvivesItsSocket(t *testing.T) {
	bind := "[2001:db8::1]:0"
	if c, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort(bind))); err == nil {
		_ = c.Close()
		t.Skip("the documentation address is assigned on this host")
	}
	var logs syncBuffer
	a, err := newAnnouncer(config{interestUpstream: "[2001:db8::2]:8729", interestBind: bind, interestLabel: "bridge-test-4"},
		[]string{"tm_a"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	waitFor(t, &logs, "TOPIC INTEREST STOPPED")
	select {
	case err := <-done:
		t.Fatalf("Serve returned while its context was live: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// The edge goes away. Every topic keeps the last answer it had, so the
// bridge says so in a line of its own.
func TestAnnouncerSaysWhenTheEdgeStopsAnswering(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	rcv := interest.NewReceiver(interest.ReceiverConfig{Policy: &edgePolicy{quota: -1}})
	ectx, stopEdge := context.WithCancel(context.Background())
	edgeDone := make(chan struct{})
	go func() {
		defer close(edgeDone)
		_ = interest.ServeReceiver(ectx, conn, rcv, interest.ServeOptions{Tick: 20 * time.Millisecond})
	}()
	var logs syncBuffer
	a, err := newAnnouncer(config{interestUpstream: conn.LocalAddr().String(), interestBind: "[::1]:0", interestLabel: "bridge-test-3"},
		[]string{"tm_a"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a.watchEvery = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	waitFor(t, &logs, "topic served by the edge")

	stopEdge()
	<-edgeDone
	_ = conn.Close()
	// Something to say, so that the silence is noticed now and not at the
	// next refresh.
	a.client.Add(interest.TopicID(objfmt.TopicID("tm_b")))
	waitFor(t, &logs, "TOPIC INTEREST: no edge answers")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// startTestEdge runs a real receiver on the IPv6 loopback until stop is
// called, and returns its address.
func startTestEdge(t *testing.T) (addr string, stop func()) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	rcv := interest.NewReceiver(interest.ReceiverConfig{Policy: &edgePolicy{quota: -1}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = interest.ServeReceiver(ctx, conn, rcv, interest.ServeOptions{Tick: 20 * time.Millisecond})
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done; _ = conn.Close() }) }
	t.Cleanup(stop)
	return conn.LocalAddr().String(), stop
}

// deadAddr returns a loopback address and port with nothing listening on it.
func deadAddr(t *testing.T) string {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	a := c.LocalAddr().String()
	_ = c.Close()
	return a
}

// A bridge names both of its edges, and the standby stays silent: that is
// one line at Info, never a warning.
func TestAnnouncerStandbyIsQuiet(t *testing.T) {
	primary, _ := startTestEdge(t)
	standby := deadAddr(t)
	var logs syncBuffer
	a, err := newAnnouncer(config{interestUpstream: primary + "," + standby, interestBind: "[::1]:0", interestLabel: "bridge-test-5"},
		[]string{"tm_a"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a.watchEvery = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	waitFor(t, &logs, "topic served by the edge")
	waitFor(t, &logs, "as a standby edge does until delivery moves to it")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if out := logs.String(); strings.Contains(out, "level=WARN") {
		t.Fatalf("a silent standby must not warn:\n%s", out)
	}
}

// Delivery moves: the edge that answered goes silent while the other
// answers. One line at Info; the warning is for when no edge answers.
func TestAnnouncerEdgeMovedIsQuiet(t *testing.T) {
	first, stopFirst := startTestEdge(t)
	second, _ := startTestEdge(t)
	var logs syncBuffer
	a, err := newAnnouncer(config{interestUpstream: first + "," + second, interestBind: "[::1]:0", interestLabel: "bridge-test-6"},
		[]string{"tm_a"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a.watchEvery = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	waitFor(t, &logs, "topic served by the edge")
	// Both edges must have answered before one goes away.
	reachable := func() int {
		n := 0
		for _, u := range a.client.Upstreams() {
			if u.Reachable {
				n++
			}
		}
		return n
	}
	deadline := time.Now().Add(10 * time.Second)
	for reachable() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("both edges did not answer:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopFirst()
	a.client.Add(interest.TopicID(objfmt.TopicID("tm_b")))
	waitFor(t, &logs, "an edge stopped answering while another answers")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if out := logs.String(); strings.Contains(out, "level=WARN") {
		t.Fatalf("an edge going silent while another answers must not warn:\n%s", out)
	}
}
