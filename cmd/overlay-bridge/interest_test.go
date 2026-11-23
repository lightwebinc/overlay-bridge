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

// startEdge runs a real receiver on the IPv6 loopback. stop ends it early;
// the test's cleanup ends it otherwise.
func startEdge(t *testing.T, pol *edgePolicy) (rcv *interest.Receiver, addr string, stop func()) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	rcv = interest.NewReceiver(interest.ReceiverConfig{Policy: pol})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = interest.ServeReceiver(ctx, conn, rcv, interest.ServeOptions{Tick: 20 * time.Millisecond})
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done; _ = conn.Close() }) }
	t.Cleanup(stop)
	return rcv, conn.LocalAddr().String(), stop
}

// silentAddr returns the address of a socket that is bound and never
// answers: an edge that stays silent. It stays bound until the test ends,
// so no other socket can take its port.
func silentAddr(t *testing.T) string {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c.LocalAddr().String()
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
		// A delivery address is named to the hop the bridge announces to, so
		// without one it would be dropped in silence.
		"needs -interest-upstream":               {interestBind: "[::]:0", interestDeliver: "[2001:db8::1]:9171"},
		`-interest-deliver-to "nonsense"`:        {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestDeliver: "nonsense"},
		`-interest-deliver-to "[2001:db8::1]:0"`: {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestDeliver: "[2001:db8::1]:0"},
		"unicast delivery to ::":                 {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestDeliver: "[::]:9171"},
		"unicast delivery to ff05::1":            {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestDeliver: "[ff05::1]:9171"},
		"with a zone":                            {interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestDeliver: "[fe80::1%eth0]:9171"},
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
	rcv, edge, _ := startEdge(t, pol)

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
	_, edge2, _ := startEdge(t, pol2)
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
	_, edge, stopEdge := startEdge(t, &edgePolicy{quota: -1})
	var logs syncBuffer
	a, err := newAnnouncer(config{interestUpstream: edge, interestBind: "[::1]:0", interestLabel: "bridge-test-3"},
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
	// Something to say, so that the silence is noticed now and not at the
	// next refresh.
	a.client.Add(interest.TopicID(objfmt.TopicID("tm_b")))
	waitFor(t, &logs, "TOPIC INTEREST: no edge answers")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// A bridge names both of its edges, and the standby stays silent: that is
// one line at Info, never a warning.
func TestAnnouncerStandbyIsQuiet(t *testing.T) {
	_, primary, _ := startEdge(t, &edgePolicy{quota: -1})
	standby := silentAddr(t)
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
	_, first, stopFirst := startEdge(t, &edgePolicy{quota: -1})
	_, second, _ := startEdge(t, &edgePolicy{quota: -1})
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

// The decisions behind the silence lines, one look at the edges at a time.
func TestEdgeWatch(t *testing.T) {
	a := netip.MustParseAddrPort("[2001:db8::a]:8729")
	b := netip.MustParseAddrPort("[2001:db8::b]:8729")
	up := func(x netip.AddrPort) interest.UpstreamInfo { return interest.UpstreamInfo{Addr: x, Reachable: true} }
	silent := func(x netip.AddrPort) interest.UpstreamInfo {
		return interest.UpstreamInfo{Addr: x, Stats: interest.UpstreamStats{Timeouts: 3}}
	}
	waiting := func(x netip.AddrPort) interest.UpstreamInfo { return interest.UpstreamInfo{Addr: x} }
	type look struct {
		what string
		ups  []interest.UpstreamInfo
		want []string // one string per line expected from this look
	}
	run := func(t *testing.T, looks []look) {
		t.Helper()
		var logs syncBuffer
		w := newEdgeWatch(slog.New(slog.NewTextHandler(&logs, nil)))
		seen := 0
		for _, l := range looks {
			w.step(l.ups)
			all := logs.String()
			out := all[seen:]
			seen = len(all)
			if n := strings.Count(out, "\n"); n != len(l.want) {
				t.Fatalf("%s: %d lines, want %d:\n%s", l.what, n, len(l.want), out)
			}
			for _, s := range l.want {
				if !strings.Contains(out, s) {
					t.Fatalf("%s: no %q in:\n%s", l.what, s, out)
				}
			}
		}
	}
	t.Run("standby and failover", func(t *testing.T) {
		run(t, []look{
			{"nothing heard yet", []interest.UpstreamInfo{waiting(a), waiting(b)}, nil},
			{"the primary answers", []interest.UpstreamInfo{up(a), waiting(b)}, nil},
			{"the standby is silent", []interest.UpstreamInfo{up(a), silent(b)}, []string{"level=INFO msg=\"topic interest: an edge does not answer while another does"}},
			{"and is said once", []interest.UpstreamInfo{up(a), silent(b)}, nil},
			{"delivery moves", []interest.UpstreamInfo{silent(a), up(b)}, []string{
				"level=INFO msg=\"topic interest: an edge stopped answering while another answers\" upstream=[2001:db8::a]:8729",
				"level=INFO msg=\"topic interest: the edge answers\" upstream=[2001:db8::b]:8729"}},
			{"neither answers", []interest.UpstreamInfo{silent(a), silent(b)}, []string{"level=WARN msg=\"TOPIC INTEREST: no edge answers\""}},
			{"the warning is said once", []interest.UpstreamInfo{silent(a), silent(b)}, nil},
			{"one answers again", []interest.UpstreamInfo{up(a), silent(b)}, []string{"level=INFO msg=\"topic interest: the edge answers\" upstream=[2001:db8::a]:8729"}},
		})
	})
	t.Run("the edge that answered goes silent before the other shows anything", func(t *testing.T) {
		run(t, []look{
			{"the primary answers", []interest.UpstreamInfo{up(a), waiting(b)}, nil},
			{"it goes silent, the other has not shown anything", []interest.UpstreamInfo{silent(a), waiting(b)}, []string{"level=WARN msg=\"TOPIC INTEREST: no edge answers\""}},
			{"the other answers", []interest.UpstreamInfo{silent(a), up(b)}, []string{"level=INFO msg=\"topic interest: the edge answers\" upstream=[2001:db8::b]:8729"}},
		})
	})
	t.Run("one edge", func(t *testing.T) {
		run(t, []look{
			{"nothing heard yet", []interest.UpstreamInfo{waiting(a)}, nil},
			{"it never answers", []interest.UpstreamInfo{silent(a)}, []string{"level=WARN msg=\"TOPIC INTEREST: no edge answers\""}},
			{"said once", []interest.UpstreamInfo{silent(a)}, nil},
			{"it answers", []interest.UpstreamInfo{up(a)}, []string{"level=INFO msg=\"topic interest: the edge answers\""}},
		})
	})
}

// tapEdge runs a real receiver behind a socket that decodes every ANNOUNCE
// on its way through, so a test sees what the bridge puts on the wire. The
// receiver sees the tap as the sender, and its answers go back the same way.
func tapEdge(t *testing.T, pol *edgePolicy) (addr string, records func() []interest.Record) {
	t.Helper()
	_, edge, _ := startEdge(t, pol)
	edgeAddr := netip.MustParseAddrPort(edge)
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	t.Cleanup(func() { _ = front.Close() })
	var (
		mu     sync.Mutex
		seen   []interest.Record
		sender netip.AddrPort
	)
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := front.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			b := bytes.Clone(buf[:n])
			if from == edgeAddr {
				mu.Lock()
				to := sender
				mu.Unlock()
				_, _ = front.WriteToUDPAddrPort(b, to)
				continue
			}
			mu.Lock()
			sender = from
			if m, err := interest.DecodeAnnounce(b); err == nil {
				for _, r := range m.Records {
					r.Ext = bytes.Clone(r.Ext)
					seen = append(seen, r)
				}
			}
			mu.Unlock()
			_, _ = front.WriteToUDPAddrPort(b, edgeAddr)
		}
	}()
	return front.LocalAddr().String(), func() []interest.Record {
		mu.Lock()
		defer mu.Unlock()
		return append([]interest.Record(nil), seen...)
	}
}

// With -interest-deliver-to every topic names the bridge's lane, so a
// distributing relay above sends the topic's records there. An edge holds
// the topics and ignores the address, and the bridge says that once.
func TestAnnouncerNamesItsLane(t *testing.T) {
	edge, records := tapEdge(t, &edgePolicy{quota: -1})
	var logs syncBuffer
	lane := netip.MustParseAddrPort("[2001:db8:c:1::10]:9171")
	a, err := newAnnouncer(config{
		interestUpstream: edge, interestBind: "[::1]:0", interestLabel: "bridge-test-lane",
		interestDeliver: lane.String(), beefLane: "[::]:9171",
	}, []string{"tm_one", "tm_two"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for gather(t, a)["overlay_bridge_interest_topics/ok"] != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("answers did not settle: %v\n%s", gather(t, a), logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("serve: %v", err)
	}

	// On the wire: every record that asks for a topic names the lane.
	asked := map[interest.TopicID]bool{}
	for _, r := range records() {
		if r.Lifetime == interest.LifetimeWithdraw {
			continue
		}
		asked[r.Topic] = true
		tlvs, err := interest.ParseTLVs(r.Ext)
		if err != nil || len(tlvs) != 1 || tlvs[0].Type != interest.ExtDelivery {
			t.Fatalf("record for %x carries %x, want one delivery address", r.Topic[:4], r.Ext)
		}
		d, err := interest.ParseDelivery(tlvs[0].Value)
		if err != nil {
			t.Fatal(err)
		}
		if d.Kind != interest.DeliverUnicast || d.Encoding != interest.EncodeRecords || d.Addr != lane.Addr() || d.Port != lane.Port() {
			t.Errorf("record names %+v, want a unicast stream of records to %s", d, lane)
		}
	}
	if len(asked) != 2 {
		t.Errorf("%d topics asked for on the wire, want 2", len(asked))
	}

	m := gather(t, a)
	if m["overlay_bridge_interest_topics/bad_address"] != 0 {
		t.Errorf("bad_address = %v, want 0 and exported", m["overlay_bridge_interest_topics/bad_address"])
	}
	if _, ok := m["overlay_bridge_interest_topics/bad_address"]; !ok {
		t.Error("bad_address is not exported for a bridge that names an address")
	}
	out := logs.String()
	if n := strings.Count(out, "does not honor -interest-deliver-to"); n != 1 {
		t.Errorf("the address not honored was said %d times, want once:\n%s", n, out)
	}
	if !strings.Contains(out, "deliver_to="+lane.String()) {
		t.Errorf("the startup line does not name the lane:\n%s", out)
	}
	if strings.Contains(out, "does not listen on") {
		t.Errorf("the lane's own port was reported as another:\n%s", out)
	}
}

// A port the lane does not listen on is said at startup, and a refused
// address is named by topic with the fix.
func TestAnnouncerDeliverToWarnings(t *testing.T) {
	var logs syncBuffer
	a, err := newAnnouncer(config{
		interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestLabel: "t",
		interestDeliver: "[2001:db8::1]:9181", beefLane: "[::]:9171",
	}, []string{"tm_refused"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "names a port -beef-lane does not listen on") {
		t.Errorf("a port mismatch was not reported:\n%s", logs.String())
	}
	a.onStatus(interest.TopicID(objfmt.TopicID("tm_refused")), interest.StatusBadAddress)
	out := logs.String()
	if !strings.Contains(out, "refused the address -interest-deliver-to names") || !strings.Contains(out, "tm_refused") {
		t.Errorf("a refused address was not explained:\n%s", out)
	}
	if got := gather(t, a)["overlay_bridge_interest_topics/bad_address"]; got != 1 {
		t.Errorf("bad_address = %v, want 1", got)
	}

	// No address named: no bad_address series, as before.
	b, err := newAnnouncer(config{interestUpstream: "[::1]:8729", interestBind: "[::]:0", interestLabel: "t"},
		[]string{"tm_a"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := gather(t, b)["overlay_bridge_interest_topics/bad_address"]; ok {
		t.Error("bad_address is exported for a bridge that names no address")
	}
}
