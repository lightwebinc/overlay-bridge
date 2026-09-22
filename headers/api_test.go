package headers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

func apiWithOneHeader(t *testing.T) (*API, Observation) {
	t.Helper()
	s := newStore(t, Options{MinBits: easyBits})
	anchor, _ := s.Tip()
	obs, err := s.Observe(context.Background(), mineHeader(t, anchor, rootN(1), easyBits))
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	return &API{Store: s}, obs
}

func get(t *testing.T, h http.Handler, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

func TestNativeShape(t *testing.T) {
	a, obs := apiWithOneHeader(t)
	h := a.Handler()

	rec, body := get(t, h, "/v1/tip")
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/tip = %d", rec.Code)
	}
	if body["hash"] != obs.Hash.String() || body["height"].(float64) != float64(obs.Height) {
		t.Fatalf("/v1/tip body = %v", body)
	}

	rec, body = get(t, h, "/v1/root/101")
	if rec.Code != http.StatusOK || body["merkleRoot"] != obs.Root.String() {
		t.Fatalf("/v1/root/101 = %d %v", rec.Code, body)
	}

	// A height we do not hold is 404, so a caller can tell "not yet" from a fault.
	if rec, _ := get(t, h, "/v1/root/999999"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown height = %d, want 404", rec.Code)
	}
	if rec, _ := get(t, h, "/v1/root/not-a-number"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad height = %d, want 400", rec.Code)
	}
}

// TestChaintracksShape pins the contract a stock TypeScript host speaks, and
// in particular the three details that fail silently if they drift.
func TestChaintracksShape(t *testing.T) {
	a, obs := apiWithOneHeader(t)
	h := a.Handler()

	// The prefix carries the /chaintracks segment. Serving only /v2 would
	// satisfy nothing: the client's own default looks here.
	rec, body := get(t, h, "/chaintracks/v2/height")
	if rec.Code != http.StatusOK {
		t.Fatalf("/chaintracks/v2/height = %d", rec.Code)
	}
	if body["height"].(float64) != float64(obs.Height) {
		t.Fatalf("height body = %v", body)
	}

	rec, body = get(t, h, "/chaintracks/v2/header/height/101")
	if rec.Code != http.StatusOK {
		t.Fatalf("header/height/101 = %d", rec.Code)
	}
	// The caller compares merkleRoot as a STRING, so the rendering must be the
	// display hex exactly: no byte reversal, no 0x prefix. Getting this wrong
	// returns 200 and fails every comparison, which is the worst failure here.
	if body["merkleRoot"] != obs.Root.String() {
		t.Fatalf("merkleRoot = %v, want %q", body["merkleRoot"], obs.Root.String())
	}
	if body["hash"] != obs.Hash.String() {
		t.Fatalf("hash = %v, want %q", body["hash"], obs.Hash.String())
	}
	if got := body["merkleRoot"].(string); len(got) != 64 {
		t.Fatalf("merkleRoot %q is not 64 hex characters", got)
	}

	// An unknown height is 404. The client turns 404 into "no header" and any
	// other non-ok status into a thrown exception.
	if rec, _ := get(t, h, "/chaintracks/v2/header/height/999999"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown height = %d, want 404", rec.Code)
	}
}

func TestChaintracksPrefixOverride(t *testing.T) {
	a, _ := apiWithOneHeader(t)
	a.ChaintracksPrefix = "/v2"
	h := a.Handler()
	if rec, _ := get(t, h, "/v2/height"); rec.Code != http.StatusOK {
		t.Fatalf("overridden prefix = %d", rec.Code)
	}
	if rec, _ := get(t, h, "/chaintracks/v2/height"); rec.Code == http.StatusOK {
		t.Fatal("the default prefix still answered after an override")
	}
}

func TestServeRefusesWithNoAddress(t *testing.T) {
	a := &API{Store: newStore(t, Options{})}
	if err := a.Serve(context.Background()); err == nil {
		t.Fatal("Serve with no address returned nil")
	}
}

type namingFallback struct{ stubSource }

func (n *namingFallback) HeaderForHeight(_ context.Context, h uint32) (chainhash.Hash, chainhash.Hash, error) {
	r, ok := n.roots[h]
	if !ok {
		return chainhash.Hash{}, chainhash.Hash{}, errors.New("unknown")
	}
	var hash chainhash.Hash
	hash[0] = byte(h) // a distinct, recognisable hash per height
	return hash, r, nil
}

// TestChaintracksNeverFabricatesAHash is the regression for a fallback height
// answered with the TIP's hash. A chaintracks client keys headers by hash, so
// a wrong hash under a 200 is a fabricated header identity: the worst failure
// available here.
func TestChaintracksNeverFabricatesAHash(t *testing.T) {
	// A fallback that answers roots but cannot name blocks: the route must 404
	// rather than guess.
	s := newStore(t, Options{MinBits: easyBits, Fallback: &stubSource{roots: map[uint32]chainhash.Hash{50: rootN(9)}}})
	a := &API{Store: s}
	if rec, _ := get(t, a.Handler(), "/chaintracks/v2/header/height/50"); rec.Code != http.StatusNotFound {
		t.Fatalf("root-only fallback: code = %d, want 404 rather than a fabricated hash", rec.Code)
	}

	// A fallback that CAN name blocks: the real hash is served.
	nf := &namingFallback{stubSource{roots: map[uint32]chainhash.Hash{50: rootN(9)}}}
	s2 := newStore(t, Options{MinBits: easyBits, Fallback: nf})
	a2 := &API{Store: s2}
	rec, body := get(t, a2.Handler(), "/chaintracks/v2/header/height/50")
	if rec.Code != http.StatusOK {
		t.Fatalf("naming fallback: code = %d", rec.Code)
	}
	tip, _ := s2.Tip()
	if body["hash"] == tip.String() {
		t.Fatalf("fallback height was answered with the tip's hash: %v", body)
	}
	var want chainhash.Hash
	want[0] = 50
	if body["hash"] != want.String() {
		t.Fatalf("hash = %v, want the fallback's %s", body["hash"], want.String())
	}
}
