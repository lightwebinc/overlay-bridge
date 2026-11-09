//go:build tpnil

// Package tpnil holds the joined qualification of the overlay tier on the
// multicast delivery fabric: a BEEF object submitted through one overlay host's
// BRC-22 boundary reaches a second host's unmodified engine through the BEEF
// object plane (BRC-148, BRC-149), and both answer the same BRC-24 lookup. It
// follows the TPNIL ISI-2 vectors V001 to V003 and drives only public
// interfaces: the bridge facade's and the engines' HTTP routes and the bridges'
// metrics.
//
//	go test -tags tpnil -run '^TestTPNIL_OverlayDeliveryContract$' -count=1 -v ./tests/tpnil
//
// Each requirement prints one line beginning with its identifier, which a TPNIL
// component qualification plan names as required output.
package tpnil

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
)

// fixtureSchemas are the BEEF fixture documents the qualification reads: the
// one written by multicast-test's fixture tool, and the laboratory's earlier one.
// Both carry exactly schema, txid, topic, encoding and beefHex.
var fixtureSchemas = map[string]bool{"tpnil-beef-fixture@1.0.0": true, "lightweb-beef-fixture@1.0.0": true}

type fixture struct {
	Schema   string `json:"schema"`
	TxID     string `json:"txid"`
	Topic    string `json:"topic"`
	Encoding string `json:"encoding"`
	BEEFHex  string `json:"beefHex"`
}

type environment struct {
	Schema     string            `json:"schema"`
	Components map[string]string `json:"components"`
}

type env struct {
	fixture, environment                string
	pubFacade, pubEngine, pubMetrics    string
	recvEngine, recvMetrics, lookupName string
}

func readEnv(t *testing.T) env {
	t.Helper()
	get := func(name string) string {
		v := strings.TrimRight(strings.TrimSpace(os.Getenv(name)), "/")
		if v == "" {
			t.Fatalf("%s is required", name)
		}
		return v
	}
	return env{
		fixture:     get("TPNIL_BEEF_FIXTURE"),
		environment: get("TPNIL_ENVIRONMENT"),
		pubFacade:   get("TPNIL_PUBLISHER_FACADE_URL"),
		pubEngine:   get("TPNIL_PUBLISHER_ENGINE_URL"),
		pubMetrics:  get("TPNIL_PUBLISHER_BRIDGE_METRICS"),
		recvEngine:  get("TPNIL_RECEIVER_ENGINE_URL"),
		recvMetrics: get("TPNIL_RECEIVER_BRIDGE_METRICS"),
		lookupName:  get("TPNIL_LOOKUP_SERVICE"),
	}
}

type admittance struct {
	OutputsToAdmit []uint32 `json:"outputsToAdmit"`
	CoinsToRetain  []uint32 `json:"coinsToRetain"`
	CoinsRemoved   []uint32 `json:"coinsRemoved,omitempty"`
}

// counters is one reading of the series the qualification compares.
type counters struct {
	facadeOK, published                  float64
	laneObjects, fed, admitted, emptySub float64
}

func read(t *testing.T, ctx context.Context, e env, topic string) counters {
	t.Helper()
	pub := scrape(t, ctx, e.pubMetrics)
	recv := scrape(t, ctx, e.recvMetrics)
	return counters{
		facadeOK:    pub.sum("overlay_bridge_facade_total", map[string]string{"result": "ok"}),
		published:   pub.sum("overlay_bridge_publish_total", map[string]string{"result": "sent"}),
		laneObjects: recv.sum("overlay_bridge_lane_objects_total", map[string]string{"lane": "beef", "outcome": "objects"}),
		fed:         recv.sum("overlay_bridge_feed_total", map[string]string{"outcome": "submitted"}),
		admitted:    recv.sum("overlay_bridge_engine_submits_total", map[string]string{"topic": topic, "outcome": "admitted"}),
		emptySub:    recv.sum("overlay_bridge_engine_submits_total", map[string]string{"topic": topic, "outcome": "empty"}),
	}
}

func TestTPNIL_OverlayDeliveryContract(t *testing.T) {
	e := readEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	var fx fixture
	readJSON(t, e.fixture, &fx)
	if !fixtureSchemas[fx.Schema] || fx.Topic == "" {
		t.Fatalf("fixture schema %q topic %q", fx.Schema, fx.Topic)
	}
	obj, err := hex.DecodeString(fx.BEEFHex)
	if err != nil {
		t.Fatalf("fixture BEEF: %v", err)
	}
	if !beefHolds(obj, fx.TxID) {
		t.Fatalf("fixture BEEF does not hold transaction %s", fx.TxID)
	}
	t.Logf("fixture: %s BEEF of %d bytes holds transaction %s for topic %s", fx.Encoding, len(obj), fx.TxID, fx.Topic)

	var en environment
	readJSON(t, e.environment, &en)
	for _, k := range sortedKeys(en.Components) {
		t.Logf("environment: %s %s", k, en.Components[k])
	}
	outpoint := fx.TxID + ".0"
	if n := lookupCount(t, ctx, e.recvEngine, e.lookupName, outpoint); n != 0 {
		t.Fatalf("the receiving host already holds %s before the run", outpoint)
	}
	before := read(t, ctx, e, fx.Topic)
	t.Logf("OVL-01 the receiving host's engine holds no output %s before the submission", outpoint)

	// OVL-02 (ISI-2 V002): submission through the publishing host's BRC-22
	// boundary, with the topic header in the JSON-array form BRC-22 specifies.
	steak, status := submit(t, ctx, e.pubFacade, fx.Topic, obj)
	if status != http.StatusOK {
		t.Fatalf("facade POST /submit: HTTP %d", status)
	}
	ins, ok := steak[fx.Topic]
	if !ok || len(steak) != 1 {
		t.Fatalf("facade answer is not the topic-keyed admittance of BRC-22: %v", steak)
	}
	if len(ins.OutputsToAdmit) != 1 || ins.OutputsToAdmit[0] != 0 {
		t.Fatalf("facade admitted %v, want [0]", ins.OutputsToAdmit)
	}
	t.Logf("OVL-02 the publishing host's BRC-22 boundary answered with the topic-keyed admittance of BRC-22: %s outputsToAdmit [0]", fx.Topic)

	// OVL-03 (ISI-2 V001): the object crosses the plane once and the receiving
	// host's engine admits it through its own submit interface.
	var after counters
	waitFor(t, ctx, 120*time.Second, "the receiving host's engine to admit the object", func() bool {
		after = read(t, ctx, e, fx.Topic)
		return after.published > before.published && after.laneObjects > before.laneObjects &&
			after.fed > before.fed && after.admitted > before.admitted
	})
	t.Logf("OVL-03 the publishing bridge sent the record (%s); the receiving bridge took %s objects from its BEEF lane, fed %s to its engine, and the engine admitted %s on %s",
		delta(before.published, after.published), delta(before.laneObjects, after.laneObjects),
		delta(before.fed, after.fed), delta(before.admitted, after.admitted), fx.Topic)

	// OVL-04 (ISI-2 V003): both engines answer the same BRC-24 lookup, and the
	// BEEF each returns resolves to the submitted transaction.
	for _, h := range []struct{ role, url string }{{"publishing", e.pubEngine}, {"receiving", e.recvEngine}} {
		outs := lookup(t, ctx, h.url, e.lookupName, outpoint)
		if len(outs) != 1 || outs[0].OutputIndex != 0 || !beefHolds(outs[0].BEEF, fx.TxID) {
			t.Fatalf("%s host lookup of %s: %d outputs, or BEEF without the transaction", h.role, outpoint, len(outs))
		}
	}
	t.Logf("OVL-04 both hosts' %s lookups return output %s, and each returned BEEF resolves to the submitted transaction", e.lookupName, outpoint)

	// OVL-05 (ISI-5 V011): a repeated submission creates no second admission.
	// BRC-22 does not define the answer to a repeat, and engines differ: one
	// answers with an empty admittance, another replays the receipt of its
	// first admission. Either is accepted; what is required is that neither
	// host then holds a second output, however often the repeat crosses the
	// plane.
	again, status := submit(t, ctx, e.pubFacade, fx.Topic, obj)
	if status != http.StatusOK {
		t.Fatalf("repeated facade POST /submit: HTTP %d", status)
	}
	repeat, found := again[fx.Topic]
	if !found || len(again) != 1 {
		t.Fatalf("repeated submission answered topics %v, want exactly %s", sortedTopics(again), fx.Topic)
	}
	answer := "an empty admittance"
	switch {
	case len(repeat.OutputsToAdmit) == 0:
	case len(repeat.OutputsToAdmit) == 1 && repeat.OutputsToAdmit[0] == 0:
		answer = "its original admittance, outputsToAdmit [0]"
	default:
		t.Fatalf("repeated submission admitted %v, neither empty nor the original admittance", repeat.OutputsToAdmit)
	}
	settle := time.Now().Add(20 * time.Second)
	for time.Now().Before(settle) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	final := read(t, ctx, e, fx.Topic)
	for _, h := range []struct{ role, url string }{{"publishing", e.pubEngine}, {"receiving", e.recvEngine}} {
		if n := lookupCount(t, ctx, h.url, e.lookupName, outpoint); n != 1 {
			t.Fatalf("the %s host holds %d outputs at %s after the repeat", h.role, n, outpoint)
		}
	}
	t.Logf("OVL-05 the repeated submission was answered with %s; after 20 s both hosts still hold exactly one output %s (the receiving bridge took %s more objects from its lane), so the repeat created no second admission",
		answer, outpoint, delta(after.laneObjects, final.laneObjects))
}

func submit(t *testing.T, ctx context.Context, base, topic string, body []byte) (map[string]admittance, int) {
	t.Helper()
	topics, _ := json.Marshal([]string{topic})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/submit", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Topics", string(topics))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s/submit: %v", base, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var out map[string]admittance
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("submit answer: %v: %s", err, raw)
		}
	}
	return out, resp.StatusCode
}

type lookupOutput struct {
	BEEF        []byte
	OutputIndex uint32
}

func lookup(t *testing.T, ctx context.Context, base, service, outpoint string) []lookupOutput {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"service": service, "query": map[string]string{"outpoint": outpoint}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/lookup", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s/lookup: %v", base, err)
	}
	defer resp.Body.Close()
	var answer struct {
		Type    string `json:"type"`
		Outputs []struct {
			BEEF        []int  `json:"beef"`
			OutputIndex uint32 `json:"outputIndex"`
		} `json:"outputs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("lookup at %s: HTTP %d: %v", base, resp.StatusCode, err)
	}
	if answer.Type != "output-list" {
		t.Fatalf("lookup at %s: answer type %q, want output-list", base, answer.Type)
	}
	out := make([]lookupOutput, 0, len(answer.Outputs))
	for _, o := range answer.Outputs {
		b := make([]byte, len(o.BEEF))
		for i, v := range o.BEEF {
			b[i] = byte(v)
		}
		out = append(out, lookupOutput{BEEF: b, OutputIndex: o.OutputIndex})
	}
	return out
}

func lookupCount(t *testing.T, ctx context.Context, base, service, outpoint string) int {
	t.Helper()
	return len(lookup(t, ctx, base, service, outpoint))
}

// beefHolds reports whether a BEEF (V1, V2 or Atomic) carries the transaction.
func beefHolds(obj []byte, txid string) bool {
	if b, err := transaction.NewBeefFromBytes(obj); err == nil && b.FindTransaction(txid) != nil {
		return true
	}
	if b, _, err := transaction.NewBeefFromAtomicBytes(obj); err == nil && b.FindTransaction(txid) != nil {
		return true
	}
	return false
}

type samples []struct {
	name   string
	labels string
	value  float64
}

func scrape(t *testing.T, ctx context.Context, url string) samples {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape %s: HTTP %d", url, resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		t.Fatalf("scrape %s: read: %v", url, err)
	}
	var out samples
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, rest := line, "", ""
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name, labels, rest = line[:i], line[i+1:j], strings.TrimSpace(line[j+1:])
		} else if f := strings.Fields(line); len(f) >= 2 {
			name, rest = f[0], f[1]
		} else {
			continue
		}
		if f := strings.Fields(rest); len(f) > 0 {
			if v, err := strconv.ParseFloat(f[0], 64); err == nil {
				out = append(out, struct {
					name   string
					labels string
					value  float64
				}{name, labels, v})
			}
		}
	}
	return out
}

// sum adds every sample of a family whose labels include every pair given.
func (s samples) sum(name string, want map[string]string) float64 {
	var total float64
	for _, x := range s {
		if x.name != name {
			continue
		}
		ok := true
		for k, v := range want {
			if !strings.Contains(","+x.labels+",", ","+k+`="`+v+`",`) {
				ok = false
				break
			}
		}
		if ok {
			total += x.value
		}
	}
	return total
}

func waitFor(t *testing.T, ctx context.Context, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	t.Fatalf("timed out after %s waiting for %s", limit, what)
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func delta(before, after float64) string {
	return strconv.FormatFloat(after-before, 'f', -1, 64)
}

// sortedTopics lists the topic keys of an admittance answer, for failure messages.
func sortedTopics(m map[string]admittance) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
