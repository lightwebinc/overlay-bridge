package feed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/lightwebinc/shard-common/objfmt"
)

type fixedVerifier struct{ vd spv.Verdict }

func (v fixedVerifier) Verify(context.Context, []byte) spv.Verdict { return v.vd }

// markingSubmitter records the marker each submit carried.
type markingSubmitter struct {
	stubSubmitter
	markers []string
}

func (m *markingSubmitter) SubmitMarked(ctx context.Context, topic string, object []byte, marker string) (overlay.Steak, error) {
	m.markers = append(m.markers, marker)
	return m.Submit(ctx, topic, object)
}

var testKey = []byte("0123456789abcdef0123456789abcdef")

func verifyFeed(t *testing.T, sub Submitter, mode VerifyMode, res spv.Result) *Feed {
	t.Helper()
	f := newFeed(t, sub, "tm_example")
	f.Verify = fixedVerifier{spv.Verdict{Result: res, Scripts: 3}}
	f.VerifyMode = mode
	f.MarkerKey = testKey
	return f
}

func deliver(t *testing.T, f *Feed) error {
	t.Helper()
	return f.Handle(context.Background(), objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_example"), beefObj))
}

func TestEnforceDropsARefusedObject(t *testing.T) {
	sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	f := verifyFeed(t, sub, VerifyEnforce, spv.ResultRefused)
	if err := deliver(t, f); err == nil {
		t.Fatal("a refused object was not reported")
	}
	if len(sub.calls) != 0 {
		t.Fatalf("a refused object reached the engine: %v", sub.calls)
	}
	if st := f.Stats(); st.SPVRefused != 1 || st.VerifyScripts != 3 {
		t.Fatalf("stats %+v", st)
	}
}

func TestEnforceMarksAPassingObject(t *testing.T) {
	sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	f := verifyFeed(t, sub, VerifyEnforce, spv.ResultOK)
	if err := deliver(t, f); err != nil {
		t.Fatal(err)
	}
	if len(sub.markers) != 1 || !spv.CheckMarker(testKey, beefObj, sub.markers[0]) {
		t.Fatalf("markers %v", sub.markers)
	}
}

// No verdict means no trust: the object goes to the engine unmarked, and
// the engine verifies it in full.
func TestEnforceSubmitsAnUnverifiableObjectUnmarked(t *testing.T) {
	sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	f := verifyFeed(t, sub, VerifyEnforce, spv.ResultError)
	if err := deliver(t, f); err != nil {
		t.Fatal(err)
	}
	if len(sub.calls) != 1 || len(sub.markers) != 0 {
		t.Fatalf("calls %v markers %v", sub.calls, sub.markers)
	}
}

func TestShadowNeverMarksAndNeverDrops(t *testing.T) {
	for _, res := range []spv.Result{spv.ResultOK, spv.ResultRefused, spv.ResultError} {
		sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
		f := verifyFeed(t, sub, VerifyShadow, res)
		if err := deliver(t, f); err != nil {
			t.Fatal(err)
		}
		if len(sub.calls) != 1 || len(sub.markers) != 0 {
			t.Fatalf("%s: calls %v markers %v", res, sub.calls, sub.markers)
		}
	}
}

func TestShadowCountsMismatches(t *testing.T) {
	refused := &StatusError{Code: http.StatusBadRequest, Body: "Script verification failed"}
	cases := []struct {
		res       spv.Result
		engineErr error
		kind      string
	}{
		{spv.ResultOK, nil, ""},
		{spv.ResultRefused, refused, ""},
		{spv.ResultOK, refused, MismatchBridgeOKEngineRefused},
		{spv.ResultRefused, nil, MismatchBridgeRefusedEngineOK},
		{spv.ResultError, refused, ""},
		{spv.ResultOK, &StatusError{Code: 502}, ""}, // the engine failed: no answer to compare
	}
	for _, c := range cases {
		sub := &stubSubmitter{steak: admitted("tm_example"), err: c.engineErr}
		f := verifyFeed(t, sub, VerifyShadow, c.res)
		_ = deliver(t, f)
		st := f.Stats()
		total := uint64(0)
		for _, v := range st.Mismatch {
			total += v
		}
		switch {
		case c.kind == "" && total != 0:
			t.Fatalf("%s vs %v: mismatches %v, want none", c.res, c.engineErr, st.Mismatch)
		case c.kind != "" && (total != 1 || st.Mismatch[c.kind] != 1):
			t.Fatalf("%s vs %v: mismatches %v, want one %s", c.res, c.engineErr, st.Mismatch, c.kind)
		}
	}
}

func TestEngineRefusalIsNotRetried(t *testing.T) {
	sub := &stubSubmitter{err: &StatusError{Code: http.StatusBadRequest}}
	f := newFeed(t, sub, "tm_example")
	f.SubmitRetries = 3
	f.SubmitBackoff = time.Millisecond
	_ = deliver(t, f)
	if len(sub.calls) != 1 {
		t.Fatalf("a 4xx was submitted %d times", len(sub.calls))
	}
	if st := f.Stats(); st.EngineRefused != 1 || st.EngineError != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestSubmitDurationIsObserved(t *testing.T) {
	sub := &stubSubmitter{steak: admitted("tm_example")}
	f := newFeed(t, sub, "tm_example")
	var got []string
	f.OnSubmitted = func(o string, d time.Duration) {
		if d <= 0 {
			t.Errorf("duration %v", d)
		}
		got = append(got, o)
	}
	if err := deliver(t, f); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != OutcomeAdmitted {
		t.Fatalf("outcomes %v", got)
	}
}

// The client sends the marker as the header the engine reads, and a status
// other than 200 surfaces as a StatusError.
func TestClientSendsTheMarkerHeader(t *testing.T) {
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(spv.MarkerHeader)
		if seen == "" {
			http.Error(w, `{"error":"no"}`, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"tm_example":{"outputsToAdmit":[0],"coinsToRetain":[]}}`))
	}))
	defer srv.Close()
	c := &Client{Base: srv.URL}
	if _, err := c.SubmitMarked(context.Background(), "tm_example", beefObj, "v1=ab"); err != nil || seen != "v1=ab" {
		t.Fatalf("err %v header %q", err, seen)
	}
	_, err := c.Submit(context.Background(), "tm_example", beefObj)
	if !Refused(err) {
		t.Fatalf("a 400 is not a refusal: %v", err)
	}
}
