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
	"net/netip"
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

	topicsFrom  string
	topicsEvery time.Duration
	topicsWait  time.Duration

	interestUpstream string
	interestBind     string
	interestLabel    string
	interestLifetime time.Duration
	interestWithdraw bool
	interestDeliver  string

	engine        string
	shadowEngine  string
	shadowWorkers int
	shadowQueue   int
	alsoWorkers   int
	alsoQueue     int
	alsoBytes     int64
	shadowFacade  bool
	engineRoutes  routeFlags
	trusted       routeFlags // -facade-trusted values
	publishLanes  routeFlags // -publish-lane values: SOURCE=HOSTS
	engineTimeout time.Duration
	engineWorkers int
	engineQueue   int
	stopDrain     time.Duration
	engineQBytes  int64
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
	publishQBytes      int64
	publishWait        time.Duration
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
	flag.StringVar(&c.topics, "topics", "", "comma list of elected topic names; with -topics-from engine, only the names that stand in for an engine's list until it answers")
	flag.StringVar(&c.topicsFrom, "topics-from", "", "engine = carry the topics the engines carry: each engine this bridge submits to (-engine and every -engine-route primary) is asked GET /listTopicManagers every -topics-every, and a topic it adds or drops is mapped, published and announced, or no longer, without a restart. Empty = the -topics list")
	flag.DurationVar(&c.topicsEvery, "topics-every", 5*time.Second, "with -topics-from engine, how often each engine is asked for its topics")
	flag.DurationVar(&c.topicsWait, "topics-wait", 30*time.Second, "with -topics-from engine, how long the bridge waits at start for its engines to answer before it opens its lanes with what it knows")
	flag.StringVar(&c.subURL, "subscription-url", "", "subscription source base URL (GET {url}/v1/state, see docs/contracts.md); with -subscription-consumer, -topics is reconciled against that consumer's subscription at startup")
	flag.StringVar(&c.subID, "subscription-consumer", "", "consumer id to reconcile -topics against")
	flag.StringVar(&c.subToken, "subscription-token", "", "bearer token for the subscription source read; prefer OVERLAY_BRIDGE_SUBSCRIPTION_TOKEN")
	flag.StringVar(&c.interestUpstream, "interest-upstream", "", "comma list of [host]:port to announce topic interest to: the delivery edges this consumer's tunnel reaches, or a relay. When set, the bridge tells the edge which topics -topics needs and logs the edge's answer for each; empty = off")
	flag.StringVar(&c.interestBind, "interest-bind", "[::]:0", "local [host]:port to announce from; the edge identifies the consumer by this address, so on a host with several addresses name the consumer's own tunnel address")
	flag.StringVar(&c.interestLabel, "interest-label", "overlay-bridge", "label that distinguishes this bridge from any other announcer on the same host")
	flag.DurationVar(&c.interestLifetime, "interest-lifetime", 0, "lifetime to ask the edge for, in whole seconds; 0 asks for the edge's default")
	flag.BoolVar(&c.interestWithdraw, "interest-withdraw-on-exit", false, "withdraw the interest on exit, so the edge stops delivering announced topics at once; leave off for a bridge that restarts")
	flag.StringVar(&c.interestDeliver, "interest-deliver-to", "", "address:port to name with every announced topic as where its delivery records should arrive: this bridge's -beef-lane, as the hop above reaches it. For a bridge behind a relay that distributes: it ends the consumer's lane and sends each topic's records to the address its hosts name. An edge does not honor it and delivers as provisioned; empty = name no address")
	flag.StringVar(&c.engine, "engine", "", "BRC-22 submit base, root-mounted, no /api/v1 prefix")
	flag.StringVar(&c.shadowEngine, "shadow-engine", "", "a second BRC-22 submit base fed every object the primary is fed, asynchronously, and compared per topic; empty = off")
	flag.IntVar(&c.shadowWorkers, "shadow-workers", 2, "concurrent shadow submits")
	flag.IntVar(&c.shadowQueue, "shadow-queue", 1024, "objects queued for the shadow; a full queue drops, never blocks the primary")
	flag.IntVar(&c.alsoWorkers, "also-workers", 2, "concurrent submits to each also= route target")
	flag.IntVar(&c.alsoQueue, "also-queue", 1024, "objects queued for each also= route target; a full queue sheds, never blocks the primary")
	flag.Int64Var(&c.alsoBytes, "also-queue-bytes", 256<<20, "bytes queued or in flight for each also= route target; past it the next object is shed")
	flag.BoolVar(&c.shadowFacade, "shadow-facade", false, "also offer the facade's accepted submissions to the topic's shadow, so the publishing host counts parity on what it publishes")
	flag.Var(&c.engineRoutes, "engine-route", "GLOB=PRIMARY[,shadow=SHADOW][,also=TARGET]...: the topics GLOB names submit to PRIMARY (a bare origin) with SHADOW as their shadow, instead of -engine and -shadow-engine, and each object is also handed to every TARGET; repeatable, first match wins")
	flag.DurationVar(&c.engineTimeout, "engine-timeout", 30*time.Second, "per-submit ceiling")
	flag.IntVar(&c.engineWorkers, "engine-workers", 4, "concurrent engine submits; the lane never waits on the engine")
	flag.IntVar(&c.engineQueue, "engine-queue", 256, "deliveries queued behind the workers; a full queue sheds")
	flag.DurationVar(&c.stopDrain, "stop-drain", feed.DefaultStopDrain, "at stop, how long the workers keep submitting what is already queued (the unit's stop timeout must exceed it)")
	flag.Int64Var(&c.engineQBytes, "engine-queue-bytes", 0, "also bound the engine queue by the bytes queued (0 = by -engine-queue alone); past it a delivery is shed")
	flag.IntVar(&c.submitRetries, "engine-retries", 4, "retries for a failed engine submit before the object is DROPPED; -1 disables")
	flag.IntVar(&c.maxObject, "max-object", 0, "object-byte ceiling; 0 = codec default (64 MiB)")
	flag.StringVar(&c.facadeListen, "facade-listen", "[::]:9175", "the client-facing BRC-22 /submit listener; empty = off")
	flag.Var(&c.trusted, "facade-trusted", "GLOB=CIDR[,CIDR]: the facade publishes the topics GLOB names from a source in a CIDR without the local route (no engine, no shadow, no also, no check); for a storage node's own gateway, whose provider has every piece already. Repeatable; none by default")
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
	flag.Var(&c.publishLanes, "publish-lane", "SOURCE=HOSTS: one more up-tunnel lane, for a consumer with several tunnels: its slot inner (the local source address) and its ingress hosts, a comma list in failover order. Repeatable. -edge-ingress with -publish-source is the first lane and each -publish-lane adds one; a record goes whole to the lane with the least queued. Every lane needs a source address of its own: the fabric numbers one source's records as one flow, and two connections from one address interleave it")
	flag.IntVar(&c.publishQueue, "publish-queue", 1024, "records queued for the up-tunnel; a full queue waits -publish-wait for room, then sheds")
	flag.Int64Var(&c.publishQBytes, "publish-queue-bytes", 256<<20, "also bound the publish queue by the bytes queued (0 = by -publish-queue alone); a record larger than the bound is carried alone, never refused for its size")
	flag.DurationVar(&c.publishWait, "publish-wait", 2*time.Second, "how long a submission waits for room in a full publish queue before it is shed and answered 503 (0 = never: a full queue sheds at once)")
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
	// With -topics-from engine the engines say what is carried. Asked before
	// anything opens, so the lanes and the announcer start with their lists.
	var follow *follower
	if c.topicsFrom == "engine" {
		var err error
		if follow, err = newFollower(c, log); err != nil {
			return err
		}
		names = follow.prime(ctx, c.topicsWait)
	}
	reconcileTopics(ctx, c, names, log)
	ann, err := newAnnouncer(c, names, log)
	if err != nil {
		return err
	}
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
		QueueBytes:    c.engineQBytes,
		StopDrain:     c.stopDrain,
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
		verifier := &spv.Verifier{Tracker: tracker, Network: network, Finality: finality, Timeout: c.verifyTimeout}
		// The loop guard's identity of an object is taken with its txids.
		verifier.ObjectDigest = g != nil
		f.Verify = verifier
		if c.verifyMarkerKey != "" {
			key, err := spv.LoadKey(c.verifyMarkerKey)
			if err != nil {
				return err
			}
			f.MarkerKey = key
			if vmode == feed.VerifyEnforce {
				// Only enforce sends the marker: the verifier takes it in
				// the traversal that takes the txids.
				verifier.MarkerKey = key
			}
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
		for _, s := range c.engineRoutes {
			rs, _ := feed.ParseRouteSpec(s) // validated above
			r := &feed.Route{
				Pattern: rs.Pattern,
				Submit:  &feed.Client{Base: rs.Primary, Timeout: c.engineTimeout, Log: log},
			}
			if rs.Shadow != "" {
				r.Shadow = newShadow(rs.Shadow, rs.Pattern)
			}
			for _, a := range rs.Also {
				r.Also = append(r.Also, &feed.Also{
					Submit:     &feed.Client{Base: a, Timeout: c.engineTimeout, Log: log},
					Name:       rs.Pattern,
					Target:     a,
					Workers:    c.alsoWorkers,
					QueueDepth: c.alsoQueue,
					QueueBytes: c.alsoBytes,
					Log:        log,
				})
			}
			f.Routes = append(f.Routes, r)
			log.Info("engine route", "topics", rs.Pattern, "primary", rs.Primary, "shadow", rs.Shadow, "also", strings.Join(rs.Also, " "))
		}
	}
	if c.shadowFacade {
		log.Info("facade submissions offered to the shadow")
	}

	// ---- up-tunnel and the facade. One lane (-edge-ingress, from
	// -publish-source), and one more for each -publish-lane.
	var publish *uptunnel.Lanes
	var laneNames []string
	if c.mode == "all" && c.edgeIngress != "" {
		specs := []laneSpec{{source: c.publishSource, hosts: c.edgeIngress}}
		for _, v := range c.publishLanes {
			sp, _ := parseLane(v) // validated at start
			specs = append(specs, sp)
		}
		var queues []*uptunnel.Queue
		for _, sp := range specs {
			client := &uptunnel.Client{
				Addrs:  ingressAddrs(sp.hosts, c.edgeBeefPort),
				Prefer: c.publishPreferAfter,
				Log:    log,
			}
			name := "default"
			if sp.source != "" {
				// Assert the source exists locally rather than discovering at the
				// first publish that own-traffic exclusion has been silently
				// missing: a wrong source is delivered back, a delivery plus a
				// full verify per object, not a harmless duplicate.
				addr, err := resolveSource(sp.source)
				if err != nil {
					return err
				}
				client.LocalAddr = addr
				name = sp.source
			}
			defer func() { _ = client.Close() }()
			q := uptunnel.NewQueue(client, c.publishQueue, log)
			q.MaxBytes, q.Wait = c.publishQBytes, c.publishWait
			queues = append(queues, q)
			laneNames = append(laneNames, name)
		}
		publish = uptunnel.NewLanes(queues...)
		if len(queues) > 1 {
			log.Info("up-tunnel lanes", "lanes", len(queues), "sources", strings.Join(laneNames, ","))
		}
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
	for _, a := range alsos(f) {
		tasks.go_("also-workers:"+a.Name+">"+a.Target, a.Start)
	}

	var fac *facade.Facade
	if c.mode == "all" && c.facadeListen != "" {
		topicSet := nameSet(names)
		var pub facade.Publisher
		if publish != nil {
			pub = publish
		}
		var trusted []facade.TrustRule
		for _, v := range c.trusted {
			tr, _ := facade.ParseTrust(v) // validated at start
			trusted = append(trusted, tr)
			log.Info("facade: trusted source", "topics", tr.Pattern, "nets", fmt.Sprint(tr.Nets))
		}
		fac = facade.New(facade.Config{
			Engine: submitter, Publish: pub, Guard: g,
			Topics: topicSet, MaxObject: c.maxObject, Log: log,
			Routes: f.Routes, Shadow: f.Shadow, ShadowFacade: c.shadowFacade,
			Trusted: trusted,
		})
		addr := firstAddr(c.facadeListen)
		tasks.go_("facade", func(ctx context.Context) error { return fac.Serve(ctx, addr) })
	}

	if publish != nil {
		tasks.go_("publish-queue", publish.Run)
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
	// Only once the lanes listen: the edge starts delivering an announced
	// topic as soon as it accepts the interest.
	if ann != nil {
		tasks.go_("interest", func(ctx context.Context) error {
			if !lanesBound(ctx, beefLane, headerLane) {
				return nil
			}
			return ann.Serve(ctx)
		})
	}
	// A change of the engines' topics: the feed names deliveries of a new
	// topic and the facade takes its submissions BEFORE the edge is asked
	// for it, and the edge is told of a topic gone after neither does.
	if follow != nil {
		apply := func(names []string) {
			f.SetTopics(feed.NewTopicMap(names))
			if fac != nil {
				fac.SetTopics(nameSet(names))
			}
			if ann != nil {
				ann.update(names)
			}
			if c.subURL != "" && c.subID != "" {
				reconcileTopics(ctx, c, names, log)
			}
		}
		tasks.go_("topics-follow", func(ctx context.Context) error { return follow.run(ctx, apply) })
	}

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
			facade: fac, publish: publish, publishLanes: laneNames,
			lanes:    []*lanes.Lane{beefLane, headerLane},
			timings:  tim,
			verify:   vmode != feed.VerifyOff,
			config:   configLabels(c),
			interest: ann,
			follow:   follow,
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
	switch c.topicsFrom {
	case "":
	case "engine":
		if c.mode == "sink" {
			return errors.New("-topics-from engine needs an engine to ask: -mode sink submits nowhere")
		}
		if c.topicsEvery < time.Second {
			return errors.New("-topics-every must be at least 1s")
		}
		if c.topicsWait < 0 {
			return errors.New("-topics-wait must be 0 (do not wait) or more")
		}
	default:
		return fmt.Errorf("%w, got %q", errNoFollow, c.topicsFrom)
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
		if strings.TrimSpace(c.topics) == "" && c.topicsFrom == "" {
			return errors.New("-topics is required unless -mode sink or -topics-from engine: the bridge holds the only topic name map")
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
	for _, v := range c.trusted {
		if _, err := facade.ParseTrust(v); err != nil {
			return fmt.Errorf("-facade-trusted: %w", err)
		}
	}
	if len(c.trusted) > 0 && (c.mode != "all" || c.facadeListen == "") {
		return errors.New("-facade-trusted needs the facade: -mode all and -facade-listen")
	}
	if c.shadowFacade && c.shadowEngine == "" && !c.engineRoutes.anyShadow() {
		return errors.New("-shadow-facade needs a shadow to offer to: set -shadow-engine or a route with shadow=")
	}
	if c.engineQBytes < 0 {
		return errors.New("-engine-queue-bytes must be 0 (no byte bound) or more")
	}
	if c.alsoWorkers < 0 || c.alsoQueue < 0 || c.alsoBytes < 0 {
		return errors.New("-also-workers, -also-queue and -also-queue-bytes must not be negative (0 takes the default)")
	}
	if c.publishQBytes < 0 || c.publishWait < 0 {
		return errors.New("-publish-queue-bytes and -publish-wait must be 0 (off) or more")
	}
	if len(c.publishLanes) > 0 {
		if c.edgeIngress == "" || c.publishSource == "" {
			return errors.New("-publish-lane adds lanes to the first one: set -edge-ingress and -publish-source too, so every lane has a source address of its own")
		}
		// Compared as addresses, not as text: "[2001:db8::10]" and
		// "2001:DB8:0::10" are one source, and so one flow.
		first, err := sourceAddr(c.publishSource)
		if err != nil {
			return fmt.Errorf("-publish-source: %w", err)
		}
		seen := map[netip.Addr]bool{first: true}
		for _, v := range c.publishLanes {
			sp, err := parseLane(v)
			if err != nil {
				return fmt.Errorf("-publish-lane: %w", err)
			}
			a, err := sourceAddr(sp.source)
			if err != nil {
				return fmt.Errorf("-publish-lane: %w", err)
			}
			if seen[a] {
				return fmt.Errorf("-publish-lane %s: that source address already has a lane; two connections from one address are one flow on the fabric and interleave its sequence numbers", sp.source)
			}
			seen[a] = true
		}
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

// sourceAddr is the address a publish source names, read as resolveSource
// reads it (brackets and a port are allowed), in one form: two spellings of
// one address are the same source.
func sourceAddr(s string) (netip.Addr, error) {
	host := strings.Trim(s, "[]")
	if h, _, err := net.SplitHostPort(s); err == nil {
		host = h
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("%q is not an IP address", s)
	}
	return a.Unmap(), nil
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
	return nil, fmt.Errorf("-publish-source %s is not an address on this machine; own-traffic exclusion would not match and our own objects would be delivered back to us", ip)
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

// alsos lists every route's also targets, in route order.
func alsos(f *feed.Feed) []*feed.Also {
	var out []*feed.Also
	for _, r := range f.Routes {
		out = append(out, r.Also...)
	}
	return out
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

// laneSpec is one up-tunnel lane: the local source address it publishes from
// ("" = the host's own choice, the first lane without -publish-source) and
// its ingress hosts, a comma list in failover order.
type laneSpec struct {
	source, hosts string
}

// parseLane parses a -publish-lane value, SOURCE=HOSTS.
func parseLane(v string) (laneSpec, error) {
	source, hosts, ok := strings.Cut(v, "=")
	source, hosts = strings.TrimSpace(source), strings.TrimSpace(hosts)
	if !ok || source == "" || hosts == "" {
		return laneSpec{}, fmt.Errorf("%q is not SOURCE=HOSTS", v)
	}
	if net.ParseIP(strings.Trim(source, "[]")) == nil {
		return laneSpec{}, fmt.Errorf("%q: the source %q is not an IP address", v, source)
	}
	return laneSpec{source: strings.Trim(source, "[]"), hosts: hosts}, nil
}
