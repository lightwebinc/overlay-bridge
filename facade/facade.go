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
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// Publisher puts one BRC-149 submission record on the plane. It must not
// block: the facade calls it from inside a client's synchronous request.
type Publisher interface {
	Publish(ctx context.Context, record []byte) error
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
}

// Facade is the submit server.
type Facade struct {
	cfg Config

	mu                                                                   sync.Mutex
	ok, malformed, unknownTopic, loopDropped, engineError, publishFailed uint64
}

// New returns a facade.
func New(cfg Config) *Facade { return &Facade{cfg: cfg} }

// Stats is a point-in-time snapshot.
type Stats struct {
	OK, Malformed, UnknownTopic, LoopDropped, EngineError, PublishFailed uint64
}

// Stats returns a counter snapshot.
func (f *Facade) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Stats{f.ok, f.malformed, f.unknownTopic, f.loopDropped, f.engineError, f.publishFailed}
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
	// applied to the reader rather than checked after the fact.
	object, err := io.ReadAll(io.LimitReader(r.Body, int64(f.maxObject())+1))
	if err != nil {
		f.bump(&f.malformed)
		httpError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if len(object) > f.maxObject() {
		f.bump(&f.malformed)
		httpError(w, http.StatusRequestEntityTooLarge, "object exceeds the configured maximum")
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

		// Mark BEFORE forwarding. This is the whole trick: an object the plane
		// delivered was marked when it was delivered, so a client that reads
		// from the plane and hands the same bytes to its own facade cannot
		// push them back up the tunnel.
		var known bool
		if f.cfg.Guard != nil {
			_, known = f.cfg.Guard.Mark(contentID, topicID, registry.Submitted)
		}

		// Forward either way. A repeat is harmless: the host dedups it and
		// answers with an empty admittance, which is what the client would
		// have got by submitting to the host directly.
		answer, err := f.cfg.Engine.SubmitDetail(ctx, name, object)
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

		if known {
			// Already seen in either direction: forwarded, never republished.
			f.bump(&f.loopDropped)
			f.logf("facade: not publishing an object already seen", "topic", name)
			continue
		}
		if f.cfg.Publish == nil {
			continue
		}
		// One record per topic, never a multi-topic record: the open ingress
		// admits a single topic per submission and silently books the rest as
		// a multi-topic refusal, which an operator is pre-briefed to read as
		// correct behaviour and would therefore never diagnose.
		record, err := objfmt.EncodeBEEFRecord([]string{name}, object)
		if err != nil {
			f.bump(&f.publishFailed)
			f.logf("facade: could not encode submission record", "topic", name, "err", err)
			continue
		}
		if err := f.cfg.Publish.Publish(ctx, record); err != nil {
			// The object is admitted locally but is NOT on the plane. Say so
			// rather than reporting a clean publish.
			f.bump(&f.publishFailed)
			f.logf("facade: publish failed; object is not on the plane", "topic", name, "err", err)
		}
	}

	f.bump(&f.ok)
	writeSteak(w, steak, wrapped)
}

// writeSteak re-emits the answer in the same shape the host used.
func writeSteak(w http.ResponseWriter, steak overlay.Steak, wrapped bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	var body any = steak
	if wrapped {
		body = map[string]any{"STEAK": steak}
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
