// Package facade serves the engine's own submit interface, so a client
// publishes once through the bridge instead of to every host of a topic.
//
// An accepted submission is forwarded to the LOCAL engine first, so the client
// gets that engine's real admittance instructions and the local host has
// admitted the object before anything else happens, and is then published once
// onto the object plane for every other subscribed host.
//
// # One listener
//
// There is one listener and one origin. An earlier design had a second,
// private listener to distinguish submissions arriving from the local engine's
// own propagation; the ruling that switched propagation off removed the thing
// it was there to hear, so there is nothing left to distinguish.
//
// # The reply keeps the host's shape
//
// Two engines answer /submit differently: one wraps the result, the other
// returns it bare. The facade re-emits whichever form the host used rather
// than normalising, so a client that worked against the host directly keeps
// working when it is pointed here.
package facade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// Publisher puts one BRC-149 submission record on the plane. The facade calls
// it from inside a client's synchronous request, so it may hold the caller
// only for a bounded wait for room in its own queue (overload then shows as
// request latency rather than a refusal) and never for the plane itself.
//
// sent runs once the record has ACTUALLY reached the plane and never if it
// has not. Acceptance is not publication: an asynchronous send can still fail
// after the client has been answered.
type Publisher interface {
	Publish(ctx context.Context, record []byte, sent func()) error
}

// Config configures a Facade.
type Config struct {
	// Engine is the local engine client.
	Engine feed.DetailSubmitter
	// Publish puts records on the plane. Nil disables publishing, which makes
	// the facade a plain pass-through to the local engine.
	Publish Publisher
	// Guard is the loop guard, shared with the feed.
	Guard *guard.Guard
	// Topics is the set of names this bridge may publish. The bridge holds the
	// only name map, so a topic it cannot name it cannot publish.
	Topics map[string]struct{}
	// MaxObject bounds a submitted object. Zero takes the codec default.
	MaxObject int
	Log       *slog.Logger
	// Routes send the topics each pattern names to an engine of their own,
	// as the feed's routes do; every other topic goes to Engine.
	Routes []*feed.Route
	// Shadow is the default shadow (the feed's), offered a submission only
	// when ShadowFacade is set; a routed topic's shadow is its route's.
	Shadow *feed.Shadow
	// ShadowFacade offers each accepted submission to the topic's shadow
	// once the local engine has answered, with that answer to compare
	// against, so a host that publishes through its own facade counts
	// parity on what it publishes and not only on what the lane delivers.
	// A submission the engine failed is not offered: there is no answer to
	// compare, and the client's retry brings the object again.
	ShadowFacade bool
	// Trusted names the sources whose submissions on some topics are
	// published without the local route: no engine, no shadow, no other
	// local target, and no local check. A storage node's gateway hands its
	// own provider every piece in process, so the facade's route to that
	// provider would only check again what the gateway minted. Empty, every
	// submission is routed and checked; the operator lists only its own
	// addresses.
	Trusted []TrustRule
}

// TrustRule is one -facade-trusted value: the topics Pattern names (a
// glob, as a route's) from a source in Nets are published unrouted.
type TrustRule struct {
	Pattern string
	Nets    []netip.Prefix
}

// trusts reports whether a submission on topic from addr (a request's
// RemoteAddr) skips the local route.
func (f *Facade) trusts(addr, topic string) bool {
	if len(f.cfg.Trusted) == 0 {
		return false
	}
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false
	}
	ip := ap.Addr().Unmap()
	for _, r := range f.cfg.Trusted {
		if ok, err := path.Match(r.Pattern, topic); err != nil || !ok {
			continue
		}
		for _, n := range r.Nets {
			if n.Contains(ip) {
				return true
			}
		}
	}
	return false
}

// ParseTrust parses one -facade-trusted value: GLOB=CIDR[,CIDR]...
func ParseTrust(v string) (TrustRule, error) {
	pattern, nets, ok := strings.Cut(v, "=")
	if !ok || pattern == "" || nets == "" {
		return TrustRule{}, fmt.Errorf("%q is not GLOB=CIDR[,CIDR]", v)
	}
	if _, err := path.Match(pattern, ""); err != nil {
		return TrustRule{}, fmt.Errorf("%q: the pattern: %v", v, err)
	}
	r := TrustRule{Pattern: pattern}
	for _, s := range strings.Split(nets, ",") {
		p, err := netip.ParsePrefix(strings.TrimSpace(s))
		if err != nil {
			return TrustRule{}, fmt.Errorf("%q: %v", v, err)
		}
		r.Nets = append(r.Nets, p.Masked())
	}
	return r, nil
}

// Facade is the submit server.
type Facade struct {
	cfg Config

	mu                                                                   sync.Mutex
	ok, malformed, unknownTopic, loopDropped, engineError, publishFailed uint64
	trusted                                                              uint64
}

// New returns a facade.
func New(cfg Config) *Facade { return &Facade{cfg: cfg} }

// Stats is a point-in-time snapshot.
type Stats struct {
	OK, Malformed, UnknownTopic, LoopDropped, EngineError, PublishFailed uint64
	// Trusted counts topics published unrouted for a trusted source.
	Trusted uint64
}

// Stats returns a counter snapshot.
func (f *Facade) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Stats{f.ok, f.malformed, f.unknownTopic, f.loopDropped, f.engineError, f.publishFailed, f.trusted}
}

func (f *Facade) bump(p *uint64) {
	f.mu.Lock()
	*p++
	f.mu.Unlock()
}

func (f *Facade) maxObject() int {
	if f.cfg.MaxObject > 0 {
		return f.cfg.MaxObject
	}
	return objfmt.DefaultMaxObject
}

// Handler returns the mux.
func (f *Facade) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /submit", f.submit)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func (f *Facade) submit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topics, err := ParseTopics(r.Header.Values("x-topics"))
	if err != nil {
		f.bump(&f.malformed)
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Bound the read. The body is whatever a client sent, so the ceiling is
	// applied to the reader rather than checked after the fact. A body with
	// a Content-Length is read in behind the submission record's envelope
	// for the first topic, so that buffer is the record the plane gets and
	// a slice of it the object: the object is not copied into a record of
	// its own (4 MiB a piece on a storage plane, zeroed and copied once per
	// piece before).
	var prefix []byte
	if r.ContentLength > 0 && len(topics) > 0 {
		prefix, _ = objfmt.EncodeBEEFRecordHeader([]string{topics[0]}, int(r.ContentLength))
	}
	object, firstRecord, err := readBodyAfter(r, f.maxObject(), prefix)
	if errors.Is(err, errTooLarge) {
		f.bump(&f.malformed)
		httpError(w, http.StatusRequestEntityTooLarge, "object exceeds the configured maximum")
		return
	}
	if err != nil {
		f.bump(&f.malformed)
		httpError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if !objfmt.IsBEEFObject(object) {
		// The marker gate is the only structural claim made here; the engine
		// is the authority on whether the object is valid.
		f.bump(&f.malformed)
		httpError(w, http.StatusBadRequest, "body does not lead with a BEEF marker")
		return
	}
	for _, name := range topics {
		if _, ok := f.cfg.Topics[name]; !ok {
			f.bump(&f.unknownTopic)
			httpError(w, http.StatusBadRequest, fmt.Sprintf("this bridge does not carry topic %q", name))
			return
		}
	}

	contentID := objfmt.ContentID(object)

	steak := overlay.Steak{}
	wrapped := false
	for _, name := range topics {
		topicID := objfmt.TopicID(name)

		// Ask the guard, but do NOT mark yet. An object the plane delivered was
		// marked Delivered when it was delivered, so a client that reads from
		// the plane and hands the same bytes to its own facade is recognised
		// here and never pushed back up the tunnel. That is the loop this
		// guard exists to stop, and a read is enough to stop it.
		//
		// Marking only after a SUCCESSFUL publish is deliberate. Marking here
		// would mean a publish that fails leaves the object marked, so every
		// retry is dropped as a loop and the object is stranded off the plane
		// for the life of the entry, while the client is told nothing is
		// wrong. The registry has no way to take a mark back.
		//
		// The race this opens is one object published twice if two identical
		// submissions are in flight at the same instant, and it is covered
		// elsewhere: the fabric ingress claims the identical
		// (ContentID, TopicID) key and drops the second. The guard is defence
		// in depth on this path; the stranding it would cause has no second
		// line at all.
		var known bool
		if f.cfg.Guard != nil {
			_, known = f.cfg.Guard.Lookup(contentID, topicID)
		}

		// A trusted source's topic is published unrouted (Config.Trusted):
		// answered as an engine answers an object it admits, and nothing
		// local is asked.
		trusted := f.trusts(r.RemoteAddr, name)
		if trusted {
			f.bump(&f.trusted)
			steak[name] = &overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{0}}
		} else {
			// Forward either way. A repeat is harmless: the host dedups it and
			// answers with an empty admittance, which is what the client would
			// have got by submitting to the host directly.
			engine, shadow := f.enginesFor(name)
			answer, err := engine.SubmitDetail(ctx, name, object)
			if err != nil {
				f.bump(&f.engineError)
				f.logf("facade: engine submit failed", "topic", name, "err", err)
				// No per-item verdict list: the caller requeues the whole
				// submission rather than voting on any one object.
				httpError(w, http.StatusBadGateway, "engine submit failed")
				return
			}
			wrapped = wrapped || answer.Wrapped
			for k, v := range answer.Steak {
				steak[k] = v
			}
			if _, ok := steak[name]; !ok {
				steak[name] = &overlay.AdmittanceInstructions{}
			}
			if f.cfg.ShadowFacade {
				// Never blocks, never fails: a non-blocking put on the shadow's
				// own queue, as the feed's offer is.
				shadow.Offer(name, object, "", answer.Steak[name])
			}
		}

		if known {
			// Already seen in either direction: forwarded, never republished.
			f.bump(&f.loopDropped)
			f.logf("facade: not publishing an object already seen", "topic", name)
			continue
		}
		// The route's other local targets get what the primary accepted: the
		// plane does not deliver this host's own publications back to it
		// (own-traffic exclusion), so this hand-off is their delivery.
		if !trusted {
			feed.RouteFor(f.cfg.Routes, name).OfferAlso(name, object)
		}
		if f.cfg.Publish == nil {
			continue
		}
		// One record per topic. A record naming every topic at once is
		// admitted, but the plane delivers only its leading topics (one
		// through the public door, the operator's cap through a tunnel) and
		// carries the rest as labels; one record per topic keeps every topic
		// the client named deliverable, and each record still names the
		// topic it is for, so a subscriber sees what it was published under.
		record := firstRecord
		if record == nil || name != topics[0] {
			if record, err = objfmt.EncodeBEEFRecord([]string{name}, object); err != nil {
				f.bump(&f.publishFailed)
				f.logf("facade: could not encode submission record", "topic", name, "err", err)
				continue
			}
		}
		// The guard is marked Submitted only once the record has actually been
		// sent, from the publisher's callback. Marking on acceptance into the
		// queue would re-create the stranding this file already fixed once,
		// one hop later: the send fails after the client got 200, the mark
		// stays, and every retry is dropped as a loop for the guard TTL.
		g, cid, tid := f.cfg.Guard, contentID, topicID
		sent := func() {
			if g != nil {
				g.Mark(cid, tid, registry.Submitted)
			}
		}
		if err := f.cfg.Publish.Publish(ctx, record, sent); err != nil {
			// The object is admitted locally but is NOT on the plane, and the
			// guard is deliberately left unmarked so a retry can publish it.
			//
			// Say so with a status the client can act on. Answering 200 here
			// would tell a client it published when it did not, which is the
			// one thing a publish interface must never do; the local
			// admittance is real but it is not what was asked for. A shed
			// object is not billed, and this path books no delivered egress.
			f.bump(&f.publishFailed)
			f.logf("facade: publish failed; object is not on the plane", "topic", name, "err", err)
			w.Header().Set("Retry-After", "1")
			httpError(w, http.StatusServiceUnavailable,
				"admitted locally but not published: the plane is unreachable or the publish queue is full; retry")
			return
		}
	}

	f.bump(&f.ok)
	writeSteak(w, steak, wrapped)
}

// errTooLarge reports a body past the object ceiling.
var errTooLarge = errors.New("facade: object exceeds the configured maximum")

// readBody reads the request body, bounded by limit.
//
// A request that declares a Content-Length within the bound is read into one
// buffer of exactly that size plus one byte, and the extra byte is what
// proves nothing follows: a body that ends before its declared length, or
// runs past it, is an error, as a body that cannot be read is. io.ReadAll
// grew its buffer step by step: for the 4 MiB pieces a client puts through
// the facade, about thirty allocations and two and a half times the body in
// bytes (measured on Go 1.27), on the client's synchronous request path.
//
// Without a Content-Length (a chunked body), or with one past the bound, the
// read falls back to ReadAll under a limit of one byte past the ceiling, so a
// body that proves too large is refused the same way whether or not its
// length was declared.
func readBody(r *http.Request, limit int) ([]byte, error) {
	object, _, err := readBodyAfter(r, limit, nil)
	return object, err
}

// readBodyAfter is readBody with the body read in behind prefix when its
// Content-Length is declared: it returns the object, and the buffer holding
// prefix then the object (nil when the body was read without one), which
// a prefix shaped as the record's envelope makes the record itself.
func readBodyAfter(r *http.Request, limit int, prefix []byte) (object, buf []byte, err error) {
	if cl := r.ContentLength; cl >= 0 && cl <= int64(limit) {
		buf := make([]byte, len(prefix)+int(cl)+1)
		copy(buf, prefix)
		n, err := io.ReadFull(r.Body, buf[len(prefix):])
		switch {
		case int64(n) == cl && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)):
			if prefix == nil {
				return buf[:n], nil, nil
			}
			return buf[len(prefix) : len(prefix)+n], buf[:len(prefix)+n], nil
		case err == nil:
			return nil, nil, errors.New("facade: body runs past its Content-Length")
		default:
			return nil, nil, err
		}
	}
	object, err = io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
	if err != nil {
		return nil, nil, err
	}
	if len(object) > limit {
		return nil, nil, errTooLarge
	}
	return object, nil, nil
}

// enginesFor picks the engine and the shadow for a topic: its route's, or
// the facade's own. A route whose engine does not report the answer's
// wrapper form is relayed bare.
func (f *Facade) enginesFor(topic string) (feed.DetailSubmitter, *feed.Shadow) {
	r := feed.RouteFor(f.cfg.Routes, topic)
	if r == nil || r.Submit == nil {
		return f.cfg.Engine, f.cfg.Shadow
	}
	if ds, ok := r.Submit.(feed.DetailSubmitter); ok {
		return ds, r.Shadow
	}
	return bareDetail{r.Submit}, r.Shadow
}

// bareDetail adapts a plain Submitter to DetailSubmitter, answering bare.
type bareDetail struct{ feed.Submitter }

func (b bareDetail) SubmitDetail(ctx context.Context, topic string, object []byte) (feed.Answer, error) {
	steak, err := b.Submit(ctx, topic, object)
	return feed.Answer{Steak: steak}, err
}

// writeSteak re-emits the answer in the same shape the host used.
func writeSteak(w http.ResponseWriter, steak overlay.Steak, wrapped bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	// Relay the WIRE form, not the SDK struct: the struct has no JSON tags,
	// so encoding it emits Go field names and a client sees no admitted
	// outputs. See wire.go.
	w2 := wireSteak(steak)
	var body any = w2
	if wrapped {
		body = map[string]any{"STEAK": w2}
	}
	_ = json.NewEncoder(w).Encode(body)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (f *Facade) logf(msg string, kv ...any) {
	if f.cfg.Log != nil {
		f.cfg.Log.Warn(msg, kv...)
	}
}

// Serve runs the facade on addr until ctx is cancelled.
func (f *Facade) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: f.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
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
