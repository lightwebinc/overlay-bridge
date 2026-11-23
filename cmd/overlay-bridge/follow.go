package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/overlay-bridge/feed"
)

// A follower keeps the bridge's topics equal to what its engines carry
// (-topics-from engine), so a topic added to or dropped from an engine needs
// no restart here and no second list to keep in step.
//
// It asks each engine the bridge submits to (-engine, and the primary of
// every -engine-route) for its topic managers, GET /listTopicManagers, the
// route overlay-express and this repository's Go engine both serve. A topic
// is carried when the engine its route sends it to lists it, so a topic one
// engine lists but routes elsewhere is not taken from that engine.
//
// It holds nothing that outlives the process: what each engine last listed
// is soft state, asked again every -topics-every. Until an engine first
// answers, the -topics names routed to it stand in for its list, and an
// engine that stops answering keeps the list it last gave: a topic is dropped
// only when an engine that answers no longer lists it, never because an
// engine is down. A primary that answers 404, 405 or 501 does not list its
// topics (a storage provider, say), and the -topics names routed to it are
// its topics for as long as the bridge runs.
type follower struct {
	origins []*origin
	// routes are the -engine-route globs in order, each with its primary;
	// a topic no glob matches goes to the first origin, -engine.
	routes []followRoute
	every  time.Duration
	client *http.Client
	log    *slog.Logger

	mu      sync.Mutex
	current []string // sorted; what the bridge carries now
	changes uint64
}

type followRoute struct {
	route  *feed.Route // for Matches
	origin *origin
}

// origin is one engine and what it last listed.
type origin struct {
	base string
	// known is what the engine last listed or, until it answers, the
	// -topics names routed to it.
	known []string
	etag  string
	// answered: it has listed its topics at least once. static: it answered
	// that it does not list them.
	answered, static bool
	// up is the last read's outcome; failing marks a failure that has been
	// logged and not yet a recovery.
	up, failing bool
}

// listPath is the standard route that lists an overlay host's topic managers.
const listPath = "/listTopicManagers"

// maxList bounds the answer read: a list of topic names and their metadata.
const maxList = 16 << 20

// newFollower takes -engine, each -engine-route primary and -topics, the
// names that stand in for an engine's list until it answers.
func newFollower(c config, log *slog.Logger) (*follower, error) {
	fo := &follower{every: c.topicsEvery, client: &http.Client{Timeout: 10 * time.Second}, log: log}
	byBase := map[string]*origin{}
	add := func(base string) *origin {
		base = strings.TrimRight(base, "/")
		if o, ok := byBase[base]; ok {
			return o
		}
		o := &origin{base: base, up: true}
		byBase[base] = o
		fo.origins = append(fo.origins, o)
		return o
	}
	add(c.engine)
	for _, s := range c.engineRoutes {
		rs, err := feed.ParseRouteSpec(s)
		if err != nil {
			return nil, fmt.Errorf("-engine-route: %w", err)
		}
		fo.routes = append(fo.routes, followRoute{route: &feed.Route{Pattern: rs.Pattern}, origin: add(rs.Primary)})
	}
	for _, n := range uniqueSorted(splitList(c.topics)) {
		o := fo.originOf(n)
		o.known = append(o.known, n)
	}
	fo.current = fo.carried()
	return fo, nil
}

// originOf is the engine a topic is submitted to: the first route whose glob
// matches it, else -engine.
func (fo *follower) originOf(topic string) *origin {
	for _, r := range fo.routes {
		if r.route.Matches(topic) {
			return r.origin
		}
	}
	return fo.origins[0]
}

// carried is every topic an engine lists that its route sends to that engine,
// sorted. A primary that does not list (static) carries the -topics names
// routed to it and every topic a listing engine lists that is routed to it:
// a host whose engine carries a storage topic routed to its provider carries
// it without naming it twice.
func (fo *follower) carried() []string {
	var out []string
	for _, o := range fo.origins {
		for _, n := range o.known {
			if to := fo.originOf(n); to == o || (to.static && !o.static) {
				out = append(out, n)
			}
		}
	}
	return uniqueSorted(out)
}

// Topics is what the bridge carries now.
func (fo *follower) Topics() []string {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	return slices.Clone(fo.current)
}

// prime asks every engine before the lanes open, for at most wait, so a
// bridge that starts beside its engine starts with the engine's topics
// rather than learning them after deliveries have begun. An engine that has
// not answered by then is named, and the -topics names routed to it stand
// in for its list until it does.
func (fo *follower) prime(ctx context.Context, wait time.Duration) []string {
	deadline := time.Now().Add(wait)
	pause := 250 * time.Millisecond
	for {
		pending := 0
		for _, o := range fo.origins {
			if !o.answered && !o.static {
				fo.read(ctx, o, false)
				if !o.answered && !o.static {
					pending++
				}
			}
		}
		if pending == 0 || ctx.Err() != nil || !time.Now().Add(pause).Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(pause):
		}
		pause = min(2*pause, 2*time.Second)
	}
	fo.mu.Lock()
	for _, o := range fo.origins {
		if !o.answered && !o.static {
			fo.log.Warn("TOPICS NOT YET FROM THE ENGINE: it has not answered; starting with the -topics names routed to it",
				"engine", o.base, "topics", len(o.known), "waited", wait,
				"consequence", "until it answers, a topic it carries that -topics does not name is not delivered here",
				"fix", "start the engine before the bridge, or name its topics in -topics as well")
			o.failing = true // its first answer is logged
		}
	}
	fo.current = fo.carried()
	out := slices.Clone(fo.current)
	fo.mu.Unlock()
	fo.log.Info("topics from the engines", "topics", len(out), "engines", len(fo.origins), "every", fo.every)
	return out
}

// run asks the engines every -topics-every until ctx ends and hands apply
// each set that differs from the last. apply runs on this goroutine, one
// call at a time.
func (fo *follower) run(ctx context.Context, apply func(names []string)) error {
	t := time.NewTicker(fo.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for _, o := range fo.origins {
			if !o.static {
				fo.read(ctx, o, true)
			}
		}
		fo.mu.Lock()
		next, prev := fo.carried(), fo.current
		if slices.Equal(prev, next) {
			fo.mu.Unlock()
			continue
		}
		fo.current = next
		fo.changes++
		fo.mu.Unlock()
		added, removed := diffSorted(prev, next)
		fo.log.Info("topics changed: following the engines", "added", added, "removed", removed, "topics", len(next))
		apply(slices.Clone(next))
	}
}

// read asks one engine for its topics. A failure keeps what it last listed.
// loud says a failure is worth a warning: once it has started, not while the
// bridge is waiting for it at start.
func (fo *follower) read(ctx context.Context, o *origin, loud bool) {
	names, etag, status, err := fo.list(ctx, o)
	fo.mu.Lock()
	defer fo.mu.Unlock()
	switch {
	case err == nil && status == http.StatusNotModified:
	case err == nil && status == http.StatusOK:
		o.known, o.etag, o.answered = names, etag, true
	case err == nil && (status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented):
		// It answers, but not this question: not an engine that lists.
		o.static = true
		if len(o.known) == 0 {
			fo.log.Warn("TOPICS NOT FOLLOWED: an engine does not list its topics and -topics names none routed to it",
				"engine", o.base, "status", status,
				"consequence", "nothing is delivered here for the topics this engine carries",
				"fix", "name its topics in -topics, or run an engine that serves GET "+listPath)
		} else {
			fo.log.Info("this engine does not list its topics: the -topics names routed to it are its topics",
				"engine", o.base, "status", status, "topics", len(o.known))
		}
	default:
		if err == nil {
			err = fmt.Errorf("answered %d", status)
		}
		if o.up && loud {
			fo.log.Warn("TOPICS NOT FOLLOWED: an engine does not answer for its topics; keeping what it last listed",
				"engine", o.base, "err", err, "topics", len(o.known),
				"consequence", "a topic added to it meanwhile is not delivered here; one dropped from it is still announced",
				"watch", "overlay_bridge_topics_engine_up")
			o.failing = true
		}
		o.up = false
		return
	}
	if !o.up && o.failing {
		fo.log.Info("an engine answers for its topics again", "engine", o.base, "topics", len(o.known))
	}
	o.up, o.failing = true, false
}

// list GETs one engine's topic managers. status is the HTTP status; names and
// etag are set on 200.
func (fo *follower) list(ctx context.Context, o *origin) (names []string, etag string, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.base+listPath, nil)
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Set("Accept", "application/json")
	fo.mu.Lock()
	if o.etag != "" {
		req.Header.Set("If-None-Match", o.etag)
	}
	fo.mu.Unlock()
	resp, err := fo.client.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, "", resp.StatusCode, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxList+1))
	if err != nil {
		return nil, "", 0, err
	}
	if len(body) > maxList {
		return nil, "", 0, fmt.Errorf("the list exceeds %d bytes", maxList)
	}
	// A map of topic name to its manager's metadata; only the names count.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, "", 0, fmt.Errorf("the list is not a JSON object of topic names: %w", err)
	}
	for n := range m {
		if n = strings.TrimSpace(n); n != "" {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return names, resp.Header.Get("ETag"), http.StatusOK, nil
}

var (
	descTopics = prometheus.NewDesc("overlay_bridge_topics",
		"Topics this bridge carries now: the names it maps delivered records to, may publish, and announces. With -topics-from engine it follows the engines' lists.",
		nil, nil)
	descTopicsUp = prometheus.NewDesc("overlay_bridge_topics_engine_up",
		"1 when the last ask of an engine for its topics was answered (or the engine does not list them, and -topics stands in), 0 when it failed and the bridge keeps what the engine last listed.",
		[]string{"engine"}, nil)
	descTopicsChanges = prometheus.NewDesc("overlay_bridge_topics_changes_total",
		"Times the carried topics changed because an engine's list did.", nil, nil)
)

func (fo *follower) describe(ch chan<- *prometheus.Desc) {
	ch <- descTopics
	ch <- descTopicsUp
	ch <- descTopicsChanges
}

func (fo *follower) collect(ch chan<- prometheus.Metric) {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	gauge(ch, descTopics, float64(len(fo.current)))
	for _, o := range fo.origins {
		up := 0.0
		if o.up {
			up = 1
		}
		gauge(ch, descTopicsUp, up, o.base)
	}
	counter(ch, descTopicsChanges, fo.changes)
}

// uniqueSorted sorts names and drops repeats and blanks.
func uniqueSorted(names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// diffSorted returns what next adds to prev and what it drops; both sorted.
func diffSorted(prev, next []string) (added, removed []string) {
	for _, n := range next {
		if _, ok := slices.BinarySearch(prev, n); !ok {
			added = append(added, n)
		}
	}
	for _, n := range prev {
		if _, ok := slices.BinarySearch(next, n); !ok {
			removed = append(removed, n)
		}
	}
	return added, removed
}

// errNoFollow is returned by validate for a -topics-from value it does not know.
var errNoFollow = errors.New("-topics-from must be empty (the -topics list) or engine")

// nameSet is names as the facade's set.
func nameSet(names []string) map[string]struct{} {
	s := make(map[string]struct{}, len(names))
	for _, n := range names {
		s[n] = struct{}{}
	}
	return s
}
