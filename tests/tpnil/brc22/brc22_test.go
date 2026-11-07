//go:build tpnil

// Package brc22 turns the source observations of the TPNIL ISI-2 BRC-22 submit
// boundary vectors into runtime observations for go-overlay-services: every
// header vector goes through the server's real HTTP routes, in front of an
// engine that records the topics it is handed, and go-sdk's facilitator is
// pointed at the running server. Each vector's result must equal the
// go-overlay-services observation the vector file records.
//
//	go -C tests/tpnil/brc22 test -tags tpnil -count=1 -v ./...
package brc22

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/gasp"
	server "github.com/bsv-blockchain/go-overlay-services/pkg/server"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/overlay/topic"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gofiber/fiber/v2"
)

// recorder is an engine that admits output 0 on every topic it is handed and
// remembers the topic list, so the server's own header handling is observed.
type recorder struct {
	mu     sync.Mutex
	topics []string
}

func (r *recorder) Submit(_ context.Context, b overlay.TaggedBEEF, _ engine.SumbitMode, ready engine.OnSteakReady) (overlay.Steak, error) {
	r.mu.Lock()
	r.topics = append([]string(nil), b.Topics...)
	r.mu.Unlock()
	steak := overlay.Steak{}
	for _, t := range b.Topics {
		steak[t] = &overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{0}, CoinsToRetain: []uint32{}}
	}
	if ready != nil {
		ready(&steak)
	}
	return steak, nil
}

func (r *recorder) last() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.topics...)
}

var errUnused = errors.New("not used by the boundary harness")

func (*recorder) Lookup(context.Context, *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
	return nil, errUnused
}
func (*recorder) GetUTXOHistory(context.Context, *engine.Output, func(*transaction.Beef, uint32, uint32) bool, uint32) (*engine.Output, error) {
	return nil, errUnused
}
func (*recorder) SyncAdvertisements(context.Context) error { return errUnused }
func (*recorder) StartGASPSync(context.Context) error      { return errUnused }
func (*recorder) ProvideForeignSyncResponse(context.Context, *gasp.InitialRequest, string) (*gasp.InitialResponse, error) {
	return nil, errUnused
}
func (*recorder) ProvideForeignGASPNode(context.Context, *transaction.Outpoint, *transaction.Outpoint, string) (*gasp.Node, error) {
	return nil, errUnused
}
func (*recorder) ListTopicManagers() map[string]*overlay.MetaData          { return nil }
func (*recorder) ListLookupServiceProviders() map[string]*overlay.MetaData { return nil }
func (*recorder) GetDocumentationForLookupServiceProvider(string) (string, error) {
	return "", errUnused
}
func (*recorder) GetDocumentationForTopicManager(string) (string, error) { return "", errUnused }
func (*recorder) HandleNewMerkleProof(context.Context, *chainhash.Hash, *transaction.MerklePath) error {
	return errUnused
}

type vectorFile struct {
	Vectors []struct {
		ID           string          `json:"id"`
		Input        json.RawMessage `json:"input"`
		Observations []struct {
			Implementation string          `json:"implementation"`
			Result         json.RawMessage `json:"result"`
		} `json:"observations"`
	} `json:"vectors"`
}

func TestTPNIL_BRC22SubmitBoundaryRuntime(t *testing.T) {
	path := os.Getenv("TPNIL_BRC22_VECTORS")
	if path == "" {
		t.Fatal("TPNIL_BRC22_VECTORS is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var vf vectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatal(err)
	}

	type variant struct {
		name string
		base string
		rec  *recorder
	}
	var variants []variant
	for _, name := range []string{"RegisterRoutes", "RegisterRoutesWithErrorHandler"} {
		rec := &recorder{}
		cfg := server.DefaultRegisterRoutesConfig
		cfg.Engine = rec
		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		if name == "RegisterRoutes" {
			server.RegisterRoutes(app, &cfg)
		} else {
			// RegisterRoutesWithErrorHandler builds and returns a new app with the
			// package's error handler; the caller serves the returned app.
			app = server.RegisterRoutesWithErrorHandler(app, &cfg)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func(a *fiber.App) { _ = a.Listener(ln) }(app)
		defer func(a *fiber.App) { _ = a.Shutdown() }(app)
		base := "http://" + ln.Addr().String()
		waitUp(t, base)
		variants = append(variants, variant{name, base, rec})
	}

	checked := 0
	for _, v := range vf.Vectors {
		var want json.RawMessage
		for _, o := range v.Observations {
			if o.Implementation == "go-overlay-services" {
				want = o.Result
			}
		}
		// Every vector this harness replays must carry the server observation it
		// is compared with; a missing one fails here, by name, never as a skip.
		if (strings.HasPrefix(v.ID, "brc22.topics.") || v.ID == "brc22.response.topic-keyed") && want == nil {
			t.Fatalf("%s: no go-overlay-services observation recorded", v.ID)
		}
		switch {
		case strings.HasPrefix(v.ID, "brc22.topics."):
			var in struct {
				Value string `json:"value"`
			}
			if err := json.Unmarshal(v.Input, &in); err != nil {
				t.Fatalf("%s input: %v", v.ID, err)
			}
			var w struct {
				Topics     []string       `json:"topics"`
				Rejected   string         `json:"rejected"`
				HTTPStatus map[string]int `json:"httpStatus"`
			}
			if err := json.Unmarshal(want, &w); err != nil {
				t.Fatalf("%s recorded result: %v", v.ID, err)
			}
			var seen []string
			for _, x := range variants {
				status, _ := post(t, x.base, in.Value)
				switch {
				case w.Rejected != "":
					if status != w.HTTPStatus[x.name] {
						t.Fatalf("%s under %s: HTTP %d, recorded %d", v.ID, x.name, status, w.HTTPStatus[x.name])
					}
					seen = append(seen, fmt.Sprintf("%s rejected with HTTP %d", x.name, status))
				default:
					if status != http.StatusOK || !reflect.DeepEqual(x.rec.last(), w.Topics) {
						t.Fatalf("%s under %s: HTTP %d, engine received %q, recorded %q", v.ID, x.name, status, x.rec.last(), w.Topics)
					}
					seen = append(seen, fmt.Sprintf("%s handed the engine %q", x.name, x.rec.last()))
				}
			}
			t.Logf("BRC22 %s runtime: %s; matches the recorded go-overlay-services observation", v.ID, strings.Join(seen, ", "))
			checked++
		case v.ID == "brc22.response.topic-keyed":
			var w struct {
				Body json.RawMessage `json:"body"`
			}
			if err := json.Unmarshal(want, &w); err != nil {
				t.Fatalf("%s recorded result: %v", v.ID, err)
			}
			for _, x := range variants {
				status, body := post(t, x.base, `["tm_example_1"]`)
				if status != http.StatusOK || !sameJSON(body, w.Body) {
					t.Fatalf("%s under %s: HTTP %d body %s, recorded %s", v.ID, x.name, status, body, w.Body)
				}
			}
			t.Logf("BRC22 %s runtime: both registrations answer exactly the recorded body, the topic map wrapped under STEAK", v.ID)
			checked++
		case v.ID == "brc22.response.client-decoding":
			f := &topic.HTTPSOverlayBroadcastFacilitator{Client: httpClient}
			steak, err := f.Send(variants[0].base, &overlay.TaggedBEEF{Beef: []byte{0x01}, Topics: []string{"tm_example_1"}})
			if err != nil {
				t.Fatalf("%s: facilitator error %v, recorded none", v.ID, err)
			}
			keys := make([]string, 0, len(*steak))
			for k := range *steak {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			if !reflect.DeepEqual(keys, []string{"STEAK"}) {
				t.Fatalf("%s: facilitator read topics %v, recorded [STEAK]", v.ID, keys)
			}
			t.Logf("BRC22 %s runtime: go-sdk's facilitator read topics %v and no error, matching the recorded observation", v.ID, keys)
			checked++
		}
	}
	if checked != 9 {
		t.Fatalf("checked %d vectors, want the 7 header vectors and both response vectors", checked)
	}
	t.Logf("BRC22 runtime: %d vectors observed through go-overlay-services' HTTP routes", checked)
}

func sameJSON(a, b []byte) bool {
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// httpClient bounds every request, so a server that accepts and never answers
// fails the test instead of holding it to the go test deadline.
var httpClient = &http.Client{Timeout: 10 * time.Second}

func post(t *testing.T, base, topics string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/submit", bytes.NewReader([]byte{0x01}))
	if err != nil {
		t.Fatalf("POST /submit request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Topics", topics)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("POST /submit: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("POST /submit body: %v", err)
	}
	return resp.StatusCode, b
}

func waitUp(t *testing.T, base string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		if resp, err := httpClient.Get(base + "/"); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("server did not start")
}
