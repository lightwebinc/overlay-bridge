package headers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// DefaultChaintracksPrefix is where a stock overlay-express host looks for the
// chaintracks read surface. It is NOT "/v2": the client's own default carries
// the "/chaintracks" segment, and an operator who wants a bare "/v2" passes an
// explicit prefix on their side.
const DefaultChaintracksPrefix = "/chaintracks/v2"

// API is the header read surface. Two shapes are served from one store.
//
// The native shape (GET /v1/tip, GET /v1/root/{height}) is this repository's
// own contract, consumed by the Go chain-tracker client in ./chainclient. It
// ships first and the bridge's own tests use it.
//
// The chaintracks shape exists because a stock TypeScript host is pointed at a
// header service with one configuration call, and that call speaks this
// dialect. Serving it is what makes "point the engine at the bridge" a single
// line rather than a patch.
type API struct {
	// Listen is one or more addresses to serve on, typically the machine's
	// admin address and loopback.
	Listen []string
	// Store answers every route.
	Store *Store
	// ChaintracksPrefix overrides DefaultChaintracksPrefix.
	ChaintracksPrefix string
	Log               *slog.Logger

	listening atomic.Bool
}

func (a *API) prefix() string {
	if a.ChaintracksPrefix != "" {
		return "/" + strings.Trim(a.ChaintracksPrefix, "/")
	}
	return DefaultChaintracksPrefix
}

// Listening reports whether at least one listener is up.
func (a *API) Listening() bool { return a.listening.Load() }

// Handler builds the mux.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	// ---- native shape
	mux.HandleFunc("GET /v1/tip", func(w http.ResponseWriter, r *http.Request) {
		hash, height := a.Store.Tip()
		writeJSON(w, http.StatusOK, map[string]any{
			"hash":   hash.String(),
			"height": height,
			"known":  a.Store.Known(height),
		})
	})
	mux.HandleFunc("GET /v1/root/{height}", func(w http.ResponseWriter, r *http.Request) {
		h, ok := parseHeight(w, r.PathValue("height"))
		if !ok {
			return
		}
		root, err := a.Store.RootAt(r.Context(), h)
		if err != nil || root == nil {
			// Unknown height is 404, not an error: the caller distinguishes
			// "not yet" from "something is broken" by the status alone.
			writeJSON(w, http.StatusNotFound, map[string]any{"height": h})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"height":     h,
			"merkleRoot": root.String(),
			"known":      a.Store.Known(h),
		})
	})

	// ---- chaintracks shape
	p := a.prefix()
	mux.HandleFunc("GET "+p+"/height", func(w http.ResponseWriter, r *http.Request) {
		_, height := a.Store.Tip()
		writeJSON(w, http.StatusOK, map[string]any{"height": height})
	})
	mux.HandleFunc("GET "+p+"/header/height/{height}", func(w http.ResponseWriter, r *http.Request) {
		h, ok := parseHeight(w, r.PathValue("height"))
		if !ok {
			return
		}
		root, err := a.Store.RootAt(r.Context(), h)
		if err != nil || root == nil {
			// 404 is the correct answer for a height we do not hold. The
			// client turns it into "no header", and turns any other non-ok
			// status into a thrown exception, so answering 200-with-zeroes or
			// 500 here converts "not yet" into an incident.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hash, _ := a.Store.Tip()
		if canon, ok := a.Store.hashAt(h); ok {
			hash = canon
		}
		// The caller compares merkleRoot as a STRING against the root it
		// holds, so the rendering has to be the SDK's display hex exactly. A
		// byte-reversed or 0x-prefixed value fails every comparison while
		// returning 200, which is the worst failure available here.
		writeJSON(w, http.StatusOK, map[string]any{
			"height":     h,
			"hash":       hash.String(),
			"merkleRoot": root.String(),
		})
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// hashAt returns the canonical hash at a height.
func (s *Store) hashAt(height uint32) (chainhash.Hash, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.canon[height]
	return h, ok
}

// Serve runs the API on every configured address until ctx is cancelled.
func (a *API) Serve(ctx context.Context) error {
	if len(a.Listen) == 0 {
		return fmt.Errorf("headers: no listen address configured")
	}
	h := a.Handler()
	errCh := make(chan error, len(a.Listen))
	var servers []*http.Server
	for _, addr := range a.Listen {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, s := range servers {
				_ = s.Close()
			}
			return fmt.Errorf("headers: listen %s: %w", addr, err)
		}
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, srv)
		if a.Log != nil {
			a.Log.Info("header read API listening", "addr", ln.Addr().String())
		}
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}
	if len(servers) == 0 {
		return fmt.Errorf("headers: no usable listen address")
	}
	a.listening.Store(true)
	defer a.listening.Store(false)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		for _, s := range servers {
			_ = s.Close()
		}
		return err
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdown)
	}
	return ctx.Err()
}

func parseHeight(w http.ResponseWriter, raw string) (uint32, bool) {
	n, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "height must be an unsigned integer"})
		return 0, false
	}
	return uint32(n), true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
