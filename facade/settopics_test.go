package facade

import (
	"net/http"
	"testing"
)

// A submission for a topic the bridge does not carry is refused; once
// SetTopics adds it, the same submission is forwarded and published, with no
// restart. A topic SetTopics drops is refused again.
func TestSetTopicsChangesWhatIsPublished(t *testing.T) {
	eng := &stubEngine{}
	pub := &stubPublisher{}
	f, _ := newFacade(t, eng, pub, "tm_old")

	if rec := submit(t, f, "tm_new", beefObj); rec.Code != http.StatusBadRequest {
		t.Fatalf("a topic not carried: code %d, want 400", rec.Code)
	}
	f.SetTopics(map[string]struct{}{"tm_old": {}, "tm_new": {}})
	if rec := submit(t, f, "tm_new", beefObj); rec.Code != http.StatusOK {
		t.Fatalf("after SetTopics: code %d: %s", rec.Code, rec.Body)
	}
	if eng.calls != 1 || len(pub.records) != 1 {
		t.Fatalf("engine calls %d, published %d; want 1 and 1", eng.calls, len(pub.records))
	}
	f.SetTopics(map[string]struct{}{"tm_old": {}})
	if rec := submit(t, f, "tm_new", append([]byte{}, beefObj...)); rec.Code != http.StatusBadRequest {
		t.Fatalf("a topic dropped: code %d, want 400", rec.Code)
	}
	if st := f.Stats(); st.UnknownTopic != 2 {
		t.Fatalf("unknown_topic = %d, want 2", st.UnknownTopic)
	}
}
