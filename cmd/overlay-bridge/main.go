// Command overlay-bridge runs an unmodified overlay services engine with a
// multicast delivery fabric as its transport.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/lightwebinc/overlay-bridge/facade"
	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/overlay-bridge/headers"
	"github.com/lightwebinc/overlay-bridge/pkg/secpverify"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/lightwebinc/overlay-bridge/uptunnel"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/lanes"
)

// Version is stamped at build time.
var Version = "dev"

type config struct {
	mode string

	beefLane   string
	headerLane string
	topics     string
	subURL     string
	subID      string
	subToken   string

	engine        string
	shadowEngine  string
	shadowWorkers int
	shadowQueue   int
	shadowFacade  bool
	engineRoutes  routeFlags
	engineTimeout time.Duration
	engineWorkers int
	engineQueue   int
	submitRetries int
	maxObject     int

	facadeListen  string
	headersListen string

	headerAnchor        string
	headerAnchorTimeout time.Duration
	headerMinBits       uint
	headerWindow        uint
	headerWarm          uint
	headerWarmWorkers   int
	headerWarmTimeout   time.Duration

	edgeIngress        string
	edgeBeefPort       int
	publishSource      string
	publishQueue       int
	publishPreferAfter time.Duration

	guardTTL     time.Duration
	guardEntries int

	metricsAddr string
	statsEvery  time.Duration
	pprofListen string

	verifyMode      string
	verifyNetwork   string
	verifyFinality  string
	verifyMarkerKey string
	verifyTimeout   time.Duration
	sigVerifier     string
}

func main() {
	var c config
	flag.StringVar(&c.mode, "mode", "all", "sink | feed | all")
	flag.StringVar(&c.beefLane, "beef-lane", "[::]:9171", "BRC-149 delivery-record lane; the edge dials this")
	flag.StringVar(&c.headerLane, "header-lane", "[::]:9172", "BRC-135 bare header lane; the edge dials this")
	flag.StringVar(&c.topics, "topics", "", "comma list of elected topic names")
	flag.StringVar(&c.subURL, "subscription-url", "", "broker base URL; when set, -topics is reconciled against the consumer's election at startup")
	flag.StringVar(&c.subID, "subscription-consumer", "", "consumer id to reconcile -topics against")
	flag.StringVar(&c.subToken, "subscription-token", "", "bearer token for the broker read; prefer OVERLAY_BRIDGE_SUBSCRIPTION_TOKEN")
	flag.StringVar(&c.engine, "engine", "", "BRC-22 submit base, root-mounted, no /api/v1 prefix")
	flag.StringVar(&c.shadowEngine, "shadow-engine", "", "a second BRC-22 submit base fed every object the primary is fed, asynchronously, and compared per topic; empty = off")
	flag.IntVar(&c.shadowWorkers, "shadow-workers", 2, "concurrent shadow submits")
	flag.IntVar(&c.shadowQueue, "shadow-queue", 1024, "objects queued for the shadow; a full queue drops, never blocks the primary")
	flag.BoolVar(&c.shadowFacade, "shadow-facade", false, "also offer the facade's accepted submissions to the topic's shadow, so the publishing host counts parity on what it publishes")
	flag.Var(&c.engineRoutes, "engine-route", "GLOB=PRIMARY[,shadow=SHADOW]: the topics GLOB names submit to PRIMARY (a bare origin) with SHADOW as their shadow, instead of -engine and -shadow-engine; repeatable, first match wins")
	flag.DurationVar(&c.engineTimeout, "engine-timeout", 30*time.Second, "per-submit ceiling")
	flag.IntVar(&c.engineWorkers, "engine-workers", 4, "concurrent engine submits; the lane never waits on the engine")
	flag.IntVar(&c.engineQueue, "engine-queue", 256, "deliveries queued behind the workers; a full queue sheds")
	flag.IntVar(&c.submitRetries, "engine-retries", 4, "retries for a failed engine submit before the object is DROPPED; -1 disables")
	flag.IntVar(&c.maxObject, "max-object", 0, "object-byte ceiling; 0 = codec default (64 MiB)")
	flag.StringVar(&c.facadeListen, "facade-listen", "[::]:9175", "the client-facing BRC-22 /submit listener; empty = off")
	flag.StringVar(&c.headersListen, "headers-listen", "[::]:9178", "chain-tracker read API; empty = off")
	flag.StringVar(&c.headerAnchor, "header-anchor", "", "header service for the initial anchor, gap re-anchor and roots below it")
	flag.DurationVar(&c.headerAnchorTimeout, "header-anchor-timeout", 5*time.Second, "bounded; never unbounded")
	flag.UintVar(&c.headerMinBits, "header-min-bits", 0x207fffff, "reject a header declaring an easier compact target than this")
	flag.UintVar(&c.headerWindow, "header-window", 0, "retained height window; 0 = unbounded")
	flag.UintVar(&c.headerWarm, "header-warm", headers.DefaultWarmDepth, "at startup, load the roots of this many heights ending at the anchor from -header-anchor before the lanes open, so a proof against a recent block does not pay the anchor round trip on the verify path; 0 = off")
	flag.IntVar(&c.headerWarmWorkers, "header-warm-workers", 8, "concurrent anchor reads during the warm-up")
	flag.DurationVar(&c.headerWarmTimeout, "header-warm-timeout", time.Minute, "ceiling on the warm-up; past it the bridge starts with what it has loaded, and the rest is fetched on first use")
	flag.StringVar(&c.edgeIngress, "edge-ingress", "", "up-tunnel hosts, comma list in failover order (side A then side B)")
	flag.IntVar(&c.edgeBeefPort, "edge-beef-port", 8725, "the open ingress port")
	flag.StringVar(&c.publishSource, "publish-source", "", "local source address for the up-tunnel; the slot inner the facade sources from")
	flag.IntVar(&c.publishQueue, "publish-queue", 1024, "bounded publish queue depth")
	flag.DurationVar(&c.publishPreferAfter, "publish-prefer-after", 5*time.Minute, "fail back to the first ingress address after this long off it; 0 = sticky")
	flag.DurationVar(&c.guardTTL, "guard-ttl", 30*time.Minute, "loop-guard entry lifetime")
	flag.IntVar(&c.guardEntries, "guard-entries", 1<<20, "loop-guard ceiling")
	flag.StringVar(&c.metricsAddr, "metrics-addr", "[::]:9179", "/metrics, /healthz, /readyz; empty = off")
	flag.DurationVar(&c.statsEvery, "stats-every", time.Minute, "stats line interval; 0 = off")
	flag.StringVar(&c.pprofListen, "pprof-listen", "", "loopback host:port serving net/http/pprof, with block and mutex profiling at low rates; empty = off")
	flag.StringVar(&c.verifyMode, "verify", "off", "bridge-side SPV of delivered objects: off | shadow (verify and compare, submit as before) | enforce (drop refused objects, mark passing ones)")
	flag.StringVar(&c.verifyNetwork, "verify-network", "main", "activation heights scripts are judged under: main | test | all (every rule from block 0; regtest-style chains)")
	flag.StringVar(&c.verifyFinality, "verify-finality", "ignore", "finality policy for unmined transactions: ignore (the overlay plane: committed-record carriers are non-final by design) | require (bitcoin-sv's rule)")
	flag.StringVar(&c.verifyMarkerKey, "verify-marker-key", "", "file holding the key the enforce-mode trust marker is computed under; mode 0600; required for enforce")
	flag.DurationVar(&c.verifyTimeout, "verify-timeout", 5*time.Second, "ceiling on one verification; past it the object goes to the engine unmarked")
	flag.StringVar(&c.sigVerifier, "sig-verifier", "decred", "secp256k1 ECDSA verifier behind go-sdk's hooks: decred (pkg/secpverify, decred's secp256k1, the same verdicts) | sdk (go-sdk's built-in crypto/ecdsa path)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(Version)
		return
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(c, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(c config, log *slog.Logger) error {
	if err := c.validate(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("overlay-bridge starting", "version", Version, "mode", c.mode)
	if c.verifier() == "decred" {
		secpverify.Install()
	}
	log.Info("ECDSA verifier", "sig_verifier", c.verifier())

	names := splitList(c.topics)
	reconcileTopics(ctx, c, names, log)
	g := guard.New(c.guardTTL, c.guardEntries)

	// ---- headers: the chain tracker the engine verifies against.
	store, err := buildStore(ctx, c, log)
	if err != nil {
		return err
	}
	tracker := headers.NewTracker(store)
	warmStore(ctx, c, store, log)

	// ---- feed: delivered objects become ordinary submits.
	var submitter feed.DetailSubmitter
	if c.mode != "sink" {
		submitter = &feed.Client{Base: c.engine, Timeout: c.engineTimeout, Log: log}
	}
	vmode, _ := feed.ParseVerifyMode(c.verifyMode) // validated above
	tim := newTimings()
	f := &feed.Feed{
		Topics:        feed.NewTopicMap(names),
		Submit:        submitter,
		Guard:         g,
		MaxObject:     c.maxObject,
		Workers:       c.engineWorkers,
		QueueDepth:    c.engineQueue,
		SubmitRetries: c.submitRetries,
		Log:           log,
		VerifyMode:    vmode,
		OnVerified:    tim.onVerified,
		OnSubmitted:   tim.onSubmitted,
		OnDequeued:    tim.onDequeued,
	}
	if vmode != feed.VerifyOff {
		network, _ := spv.ParseNetwork(c.verifyNetwork)    // validated above
		finality, _ := spv.ParseFinality(c.verifyFinality) // validated above
		f.Verify = &spv.Verifier{Tracker: tracker, Network: network, Finality: finality, Timeout: c.verifyTimeout}
		if c.verifyMarkerKey != "" {
			key, err := spv.LoadKey(c.verifyMarkerKey)
			if err != nil {
				return err
			}
			f.MarkerKey = key
		}
		log.Info("bridge SPV verification", "mode", vmode, "network", network, "finality", c.verifyFinality, "marker", len(f.MarkerKey) > 0)
	}

	newShadow := func(base, name string) *feed.Shadow {
		return &feed.Shadow{
			Submit:      &feed.Client{Base: base, Timeout: c.engineTimeout, Log: log},
			Workers:     c.shadowWorkers,
			QueueDepth:  c.shadowQueue,
			Log:         log,
			Name:        name,
			OnSubmitted: tim.onShadow(name),
		}
	}
	if c.shadowEngine != "" && submitter != nil {
		f.Shadow = newShadow(c.shadowEngine, feed.DefaultRoute)
		log.Info("shadow engine", "base", c.shadowEngine, "workers", c.shadowWorkers, "queue", c.shadowQueue)
	}
	if submitter != nil {
		for _, rs := range c.engineRoutes {
			pattern, primary, shadow, _ := feed.ParseRoute(rs) // validated above
			r := &feed.Route{
				Pattern: pattern,
				Submit:  &feed.Client{Base: primary, Timeout: c.engineTimeout, Log: log},
			}
			if shadow != "" {
				r.Shadow = newShadow(shadow, pattern)
			}
			f.Routes = append(f.Routes, r)
			log.Info("engine route", "topics", pattern, "primary", primary, "shadow", shadow)
		}
	}
	if c.shadowFacade {
		log.Info("facade submissions offered to the shadow")
	}

	// ---- up-tunnel and the facade.
	var queue *uptunnel.Queue
	if c.mode == "all" && c.edgeIngress != "" {
		client := &uptunnel.Client{
			Addrs:  ingressAddrs(c.edgeIngress, c.edgeBeefPort),
			Prefer: c.publishPreferAfter,
			Log:    log,
		}
		if c.publishSource != "" {
			// Assert the source exists locally rather than discovering at the
			// first publish that own-traffic exclusion has been silently
			// missing: a wrong source is delivered, billable egress plus a
			// full verify per object, not a harmless duplicate.
			addr, err := resolveSource(c.publishSource)
			if err != nil {
				return err
			}
			client.LocalAddr = addr
		}
		defer func() { _ = client.Close() }()
		queue = uptunnel.NewQueue(client, c.publishQueue, log)
	}

	tasks := newGroup(ctx)
	if submitter != nil && f.Workers > 0 {
		// Engine submits run off the lane's read loop, so an engine stall
		// never holds the delivery socket past the edge's write deadline.
		tasks.go_("engine-workers", f.Start)
	}
	for _, sh := range shadows(f) {
		tasks.go_("shadow-workers:"+sh.Name, sh.Start)
	}

	var fac *facade.Facade
	if c.mode == "all" && c.facadeListen != "" {
		topicSet := make(map[string]struct{}, len(names))
		for _, n := range names {
			topicSet[n] = struct{}{}
		}
		var pub facade.Publisher
		if queue != nil {
			pub = queue
		}
		fac = facade.New(facade.Config{
			Engine: submitter, Publish: pub, Guard: g,
			Topics: topicSet, MaxObject: c.maxObject, Log: log,
			Routes: f.Routes, Shadow: f.Shadow, ShadowFacade: c.shadowFacade,
		})
		addr := firstAddr(c.facadeListen)
		tasks.go_("facade", func(ctx context.Context) error { return fac.Serve(ctx, addr) })
	}

	if queue != nil {
		tasks.go_("publish-queue", queue.Run)
	}

	// ---- lanes. MaxObject bounds RECORD bytes, not object bytes: the handler
	// receives one whole class object as the codec defines it, and a delivery
	// record is its object plus a 36-byte prefix.
	objectCeiling := c.maxObject
	if objectCeiling == 0 {
		objectCeiling = objfmt.DefaultMaxObject
	}
	beefLane := &lanes.Lane{
		Name: "beef", Class: objfmt.ClassBEEFDelivery, Addr: c.beefLane,
		Handle: f.Handle, Log: log, MaxObject: objectCeiling + objfmt.BEEFDeliveryHeaderSize,
		SizeHistogram: true,
	}
	headerLane := &lanes.Lane{
		Name: "header", Class: objfmt.ClassBlockHeader, Addr: c.headerLane,
		Handle: headerHandler(store, tracker), Log: log, MaxObject: headers.Size,
	}
	tasks.go_("beef-lane", beefLane.Serve)
	tasks.go_("header-lane", headerLane.Serve)

	// ---- header read API.
	var api *headers.API
	if c.headersListen != "" {
		api = &headers.API{Listen: splitList(c.headersListen), Store: store, Log: log}
		tasks.go_("header-api", api.Serve)
	}

	// ---- metrics and health.
	if c.metricsAddr != "" {
		col := &collector{
			feed: f, guard: g, store: store, tracker: tracker,
			facade: fac, queue: queue,
			lanes:   []*lanes.Lane{beefLane, headerLane},
			timings: tim,
			verify:  vmode != feed.VerifyOff,
			config:  configLabels(c),
		}
		tasks.go_("metrics", func(ctx context.Context) error {
			return serveMetrics(ctx, c.metricsAddr, col, log)
		})
	}

	if c.pprofListen != "" {
		tasks.go_("pprof", func(ctx context.Context) error { return servePprof(ctx, c.pprofListen, log) })
	}

	if c.statsEvery > 0 {
		tasks.go_("stats", func(ctx context.Context) error {
			t := time.NewTicker(c.statsEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
					fs, hs := f.Stats(), store.Stats()
					log.Info("stats",
						"submitted", fs.Submitted, "engine_error", fs.EngineError,
						"unknown_topic", fs.UnknownTopic, "rejected", fs.Rejected,
						"headers", hs.Observed, "header_rejected", hs.Rejected,
						"tip", hs.TipHeight)
				}
			}
		})
	}

	return tasks.wait()
}

// headerHandler chains a header and advances the tracker's height in one step.
//
// Learn is the tracker's only writer of the reported height, so a lane reader
// that forgets it leaves an engine asking how far the chain has got being told
// zero. Wiring the two together here is what keeps that from being possible.
func headerHandler(store *headers.Store, tracker *headers.Tracker) lanes.Handler {
	return func(ctx context.Context, hdr []byte) error {
		obs, err := store.Observe(ctx, hdr)
		if err != nil {
			if errors.Is(err, headers.ErrProofOfWork) || errors.Is(err, headers.ErrHeaderSize) {
				return fmt.Errorf("%w: %v", lanes.ErrReject, err)
			}
			return err
		}
		if obs.Tip {
			// Only the tip is progress. A competing header at a height already
			// held is chained but is not the tip, and reporting it as the
			// chain's height would move the height on a fork.
			tracker.Learn(obs.Height)
		}
		return nil
	}
}

func (c config) validate() error {
	switch c.mode {
	case "sink", "feed", "all":
	default:
		return fmt.Errorf("-mode must be sink, feed or all, got %q", c.mode)
	}
	if c.mode != "sink" {
		if c.engine == "" {
			return errors.New("-engine is required unless -mode sink")
		}
		if strings.Contains(c.engine, "/api/v1") {
			return errors.New("-engine is a bare origin: it carries no /api/v1 prefix")
		}
		if c.headerAnchor == "" {
			return errors.New("-header-anchor is required unless -mode sink")
		}
		if strings.TrimSpace(c.topics) == "" {
			return errors.New("-topics is required unless -mode sink: the bridge holds the only topic name map")
		}
		if strings.Contains(c.shadowEngine, "/api/v1") {
			return errors.New("-shadow-engine is a bare origin: it carries no /api/v1 prefix")
		}
		if c.shadowEngine != "" && strings.TrimRight(c.shadowEngine, "/") == strings.TrimRight(c.engine, "/") {
			return errors.New("-shadow-engine is the same origin as -engine: a shadow of itself compares nothing")
		}
		for _, rs := range c.engineRoutes {
			if _, _, _, err := feed.ParseRoute(rs); err != nil {
				return fmt.Errorf("-engine-route: %w", err)
			}
		}
	} else if c.shadowEngine != "" {
		return errors.New("-shadow-engine needs an engine to shadow: -mode sink submits nowhere")
	} else if len(c.engineRoutes) > 0 {
		return errors.New("-engine-route needs an engine to route from: -mode sink submits nowhere")
	}
	if c.shadowFacade && c.shadowEngine == "" && !c.engineRoutes.anyShadow() {
		return errors.New("-shadow-facade needs a shadow to offer to: set -shadow-engine or a route with shadow=")
	}
	if _, err := feed.ParseVerifyMode(c.verifyMode); err != nil {
		return fmt.Errorf("-verify: %w", err)
	}
	if _, err := spv.ParseNetwork(c.verifyNetwork); err != nil {
		return fmt.Errorf("-verify-network: %w", err)
	}
	if _, err := spv.ParseFinality(c.verifyFinality); err != nil {
		return fmt.Errorf("-verify-finality: %w", err)
	}
	switch c.verifier() {
	case "decred", "sdk":
	default:
		return fmt.Errorf("-sig-verifier must be decred or sdk, got %q", c.sigVerifier)
	}
	if c.verifyMode == string(feed.VerifyEnforce) && c.verifyMarkerKey == "" {
		return errors.New("-verify enforce needs -verify-marker-key: without the marker the engine verifies every object again and enforce only adds a second verification")
	}
	if c.pprofListen != "" {
		if err := loopbackAddr(c.pprofListen); err != nil {
			return err
		}
	}
	if c.mode == "all" && c.facadeListen != "" && c.edgeIngress == "" {
		return errors.New("-facade-listen is set but -edge-ingress is empty: the facade would admit submissions it cannot publish")
	}
	return nil
}

func buildStore(ctx context.Context, c config, log *slog.Logger) (*headers.Store, error) {
	opt := headers.Options{
		MinBits:       uint32(c.headerMinBits),
		Window:        uint32(c.headerWindow),
		LookupTimeout: c.headerAnchorTimeout,
		Log:           log,
	}
	if c.headerAnchor != "" {
		src := &anchorClient{base: c.headerAnchor, timeout: c.headerAnchorTimeout}
		opt.Fallback, opt.Lookup = src, src
		a, err := src.tip(ctx)
		if err != nil {
			return nil, fmt.Errorf("anchor the header chain at %s: %w", c.headerAnchor, err)
		}
		opt.Anchor = a
		log.Info("header chain anchored", "height", a.Height, "hash", a.Hash.String())
	} else {
		// Sink mode with no anchor: a synthetic anchor lets the lane be
		// terminated and counted. Nothing verifies against it.
		opt.Anchor = headers.Anchor{Hash: chainhash.Hash{0x01}}
	}
	return headers.New(opt)
}

// configLabels is the overlay_bridge_config_info label set (R7): what a
// measurement of this process must be stamped with.
func configLabels(c config) []string {
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "100"
	}
	limit := "off"
	if l := debug.SetMemoryLimit(-1); l != math.MaxInt64 {
		limit = strconv.FormatInt(l, 10)
	}
	return []string{
		c.verifyMode, c.verifyNetwork, c.verifyFinality,
		strconv.Itoa(c.engineWorkers), strconv.Itoa(runtime.GOMAXPROCS(0)), gogc, limit, c.verifier(),
	}
}

// verifier is -sig-verifier, decred when unset.
func (c config) verifier() string {
	if c.sigVerifier == "" {
		return "decred"
	}
	return c.sigVerifier
}

// warmStore runs the header warm-up before the lanes open, bounded by
// -header-warm-timeout. It never fails startup: a height it does not load is
// fetched on first use, as it was before warm-up existed.
func warmStore(ctx context.Context, c config, store *headers.Store, log *slog.Logger) {
	if c.headerAnchor == "" || c.headerWarm == 0 {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, c.headerWarmTimeout)
	defer cancel()
	err := store.Warm(wctx, uint32(c.headerWarm), c.headerWarmWorkers)
	st := store.WarmStats()
	attrs := []any{"target", st.Target, "loaded", st.Loaded, "failed", st.Failed, "took", st.Duration.Round(time.Millisecond)}
	switch {
	case err != nil:
		log.Warn("header warm-up cut short; the rest load on first use", append(attrs, "err", err)...)
	case st.Failed > 0:
		log.Warn("header warm-up incomplete; the rest load on first use", attrs...)
	default:
		log.Info("header warm-up done", attrs...)
	}
}

// resolveSource turns -publish-source into a local address and asserts it
// exists on this machine.
func resolveSource(s string) (*net.TCPAddr, error) {
	host := strings.Trim(s, "[]")
	if h, _, err := net.SplitHostPort(s); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("-publish-source %q is not an IP address", s)
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("enumerate interfaces: %w", err)
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.Equal(ip) {
			return &net.TCPAddr{IP: ip}, nil
		}
	}
	return nil, fmt.Errorf("-publish-source %s is not an address on this machine; own-traffic exclusion would not match and delivery would be billed back to us", ip)
}

func ingressAddrs(list string, port int) []string {
	var out []string
	for _, h := range splitList(list) {
		if _, _, err := net.SplitHostPort(h); err == nil {
			out = append(out, h)
			continue
		}
		out = append(out, net.JoinHostPort(strings.Trim(h, "[]"), fmt.Sprint(port)))
	}
	return out
}

// routeFlags collects every -engine-route value in order.
type routeFlags []string

func (r *routeFlags) String() string { return strings.Join(*r, " ") }
func (r *routeFlags) Set(v string) error {
	*r = append(*r, v)
	return nil
}

// anyShadow reports whether some route names a shadow (parse errors read
// as no shadow; validate reports them).
func (r routeFlags) anyShadow() bool {
	for _, rs := range r {
		if _, _, shadow, err := feed.ParseRoute(rs); err == nil && shadow != "" {
			return true
		}
	}
	return false
}

// shadows lists every shadow the feed runs: the default, then each route's.
func shadows(f *feed.Feed) []*feed.Shadow {
	var out []*feed.Shadow
	if f.Shadow != nil {
		out = append(out, f.Shadow)
	}
	for _, r := range f.Routes {
		if r.Shadow != nil {
			out = append(out, r.Shadow)
		}
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func firstAddr(s string) string {
	if l := splitList(s); len(l) > 0 {
		return l[0]
	}
	return s
}

func serveMetrics(ctx context.Context, addr string, col *collector, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", col.handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !col.ready() {
			http.Error(w, "lanes not bound", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	log.Info("metrics listening", "addr", addr)
	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	return ctx.Err()
}
