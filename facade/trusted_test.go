package facade

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/overlay-bridge/guard"
)

func trustedFacade(t *testing.T, eng *stubEngine, pub *stubPublisher, rules ...string) *Facade {
	t.Helper()
	var trusted []TrustRule
	for _, v := range rules {
		r, err := ParseTrust(v)
		if err != nil {
			t.Fatal(err)
		}
		trusted = append(trusted, r)
	}
	return New(Config{Engine: eng, Publish: pub, Guard: guard.New(time.Minute, 1024),
		Topics: map[string]struct{}{"tm_storeblob_a": {}, "tm_store_a": {}}, Trusted: trusted})
}

func submitFrom(f *Facade, remote, topics string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(string(body)))
	req.RemoteAddr = remote
	req.Header.Set("x-topics", topics)
	rec := httptest.NewRecorder()
	f.Handler().ServeHTTP(rec, req)
	return rec
}

// A trusted source's topic is published with no local route, and answered
// as an engine answers an object it admits; another source, or another
// topic from the trusted source, is routed and checked as before.
func TestTrustedSourcePublishesUnrouted(t *testing.T) {
	eng, pub := &stubEngine{}, &stubPublisher{}
	f := trustedFacade(t, eng, pub, "tm_storeblob_*=127.0.0.1/32,::1/128")

	rec := submitFrom(f, "127.0.0.1:50000", "tm_storeblob_a", beefObj)
	if rec.Code != http.StatusOK || eng.calls != 0 || len(pub.records) != 1 {
		t.Fatalf("trusted: code %d, engine calls %d, published %d", rec.Code, eng.calls, len(pub.records))
	}
	var ans map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &ans); err != nil {
		t.Fatal(err)
	}
	if got := ans["tm_storeblob_a"]["outputsToAdmit"]; got == nil || len(got.([]any)) != 1 {
		t.Fatalf("trusted answer %s", rec.Body)
	}
	if st := f.Stats(); st.Trusted != 1 {
		t.Fatalf("stats %+v", st)
	}

	// IPv6 loopback, as a mapped address too.
	other := append([]byte(nil), beefObj...)
	other = append(other, '6')
	if rec := submitFrom(f, "[::1]:50001", "tm_storeblob_a", other); rec.Code != http.StatusOK || eng.calls != 0 {
		t.Fatalf("trusted over IPv6: %d, engine calls %d", rec.Code, eng.calls)
	}
	mapped := append(append([]byte(nil), beefObj...), 'm')
	if rec := submitFrom(f, "[::ffff:127.0.0.1]:50002", "tm_storeblob_a", mapped); rec.Code != http.StatusOK || eng.calls != 0 {
		t.Fatalf("trusted, mapped: %d, engine calls %d", rec.Code, eng.calls)
	}

	// Another source: routed.
	stranger := append(append([]byte(nil), beefObj...), 's')
	if rec := submitFrom(f, "192.0.2.7:40000", "tm_storeblob_a", stranger); rec.Code != http.StatusOK || eng.calls != 1 {
		t.Fatalf("stranger: %d, engine calls %d", rec.Code, eng.calls)
	}
	// Another topic from the trusted source: routed.
	rec2 := append(append([]byte(nil), beefObj...), 'r')
	if rec := submitFrom(f, "127.0.0.1:50003", "tm_store_a", rec2); rec.Code != http.StatusOK || eng.calls != 2 {
		t.Fatalf("records topic: %d, engine calls %d", rec.Code, eng.calls)
	}
}

// Without rules nobody is trusted, whatever the source.
func TestNoTrustByDefault(t *testing.T) {
	eng, pub := &stubEngine{}, &stubPublisher{}
	f := trustedFacade(t, eng, pub)
	if rec := submitFrom(f, "127.0.0.1:50000", "tm_storeblob_a", beefObj); rec.Code != http.StatusOK || eng.calls != 1 {
		t.Fatalf("no rules: %d, engine calls %d", rec.Code, eng.calls)
	}
}

func TestParseTrust(t *testing.T) {
	r, err := ParseTrust("tm_x_*=10.0.0.5/24, fd00::/8")
	if err != nil || r.Pattern != "tm_x_*" || len(r.Nets) != 2 || r.Nets[0].String() != "10.0.0.0/24" {
		t.Fatalf("%+v %v", r, err)
	}
	for _, bad := range []string{"", "tm_x", "=10.0.0.0/8", "tm_x=", "tm_[=10.0.0.0/8", "tm_x=10.0.0.1"} {
		if _, err := ParseTrust(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
