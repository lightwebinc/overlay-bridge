package feed

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

func TestParseRoute(t *testing.T) {
	for _, tc := range []struct {
		in                       string
		pattern, primary, shadow string
		wantErr                  string
	}{
		{"tm_bsecret_*=http://127.0.0.1:8280,shadow=http://127.0.0.1:8080", "tm_bsecret_*", "http://127.0.0.1:8280", "http://127.0.0.1:8080", ""},
		{"tm_bsecret_*=http://127.0.0.1:8280", "tm_bsecret_*", "http://127.0.0.1:8280", "", ""},
		{" tm_x_? = http://h:1 , shadow = http://h:2 ", "tm_x_?", "http://h:1", "http://h:2", ""},
		{"tm_bsecret_*", "", "", "", "want GLOB=PRIMARY"},
		{"=http://h:1", "", "", "", "want GLOB=PRIMARY"},
		{"tm_[=http://h:1", "", "", "", "bad glob"},
		{"tm_*=http://h:1/api/v1", "", "", "", "bare origin"},
		{"tm_*=http://h:1,shadow=http://h:1/", "", "", "", "same origin"},
		{"tm_*=http://h:1,mirror=http://h:2", "", "", "", "unknown option"},
		{"tm_*=http://h:1,shadow", "", "", "", "not key=value"},
	} {
		p, pr, sh, err := ParseRoute(tc.in)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%q: err = %v, want one containing %q", tc.in, err, tc.wantErr)
			}
			continue
		}
		if err != nil || p != tc.pattern || pr != tc.primary || sh != tc.shadow {
			t.Errorf("%q: got (%q, %q, %q, %v)", tc.in, p, pr, sh, err)
		}
	}
}

func TestRouteForFirstMatchWins(t *testing.T) {
	a := &Route{Pattern: "tm_bsecret_*"}
	b := &Route{Pattern: "tm_*"}
	routes := []*Route{a, b}
	if RouteFor(routes, "tm_bsecret_lab") != a {
		t.Fatal("tm_bsecret_lab should take the first route")
	}
	if RouteFor(routes, "tm_borg_lab") != b {
		t.Fatal("tm_borg_lab should take the second route")
	}
	if RouteFor(routes, "ls_other") != nil {
		t.Fatal("an unmatched topic should take the default")
	}
	if RouteFor(nil, "tm_bsecret_lab") != nil {
		t.Fatal("no routes, no match")
	}
}

// TestRoutedTopicUsesItsOwnPrimaryAndShadow is the cutover shape: one
// application's topics go to the new engine with the old one as their
// shadow, while every other topic keeps the default pair.
func TestRoutedTopicUsesItsOwnPrimaryAndShadow(t *testing.T) {
	defPrimary := &stubSubmitter{steak: admitted("tm_borg_lab")}
	defShadowEng := &shadowStub{steak: admitted("tm_borg_lab")}
	defShadow := &Shadow{Submit: defShadowEng, Workers: 1, Name: DefaultRoute}
	startShadow(t, defShadow)

	goPrimary := &stubSubmitter{steak: admitted("tm_bsecret_lab")}
	tsShadowEng := &shadowStub{steak: admitted("tm_bsecret_lab")}
	tsShadow := &Shadow{Submit: tsShadowEng, Workers: 1, Name: "tm_bsecret_*"}
	startShadow(t, tsShadow)

	f := newFeed(t, defPrimary, "tm_borg_lab", "tm_bsecret_lab")
	f.Shadow = defShadow
	f.Routes = []*Route{{Pattern: "tm_bsecret_*", Submit: goPrimary, Shadow: tsShadow}}

	for _, topic := range []string{"tm_bsecret_lab", "tm_borg_lab"} {
		rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID(topic), beefObj)
		if err := f.Handle(context.Background(), rec); err != nil {
			t.Fatalf("Handle %s: %v", topic, err)
		}
	}
	waitFor(t, "both shadows", func() bool { return total(tsShadow.Stats()) == 1 && total(defShadow.Stats()) == 1 })
	if len(goPrimary.calls) != 1 || goPrimary.calls[0] != "tm_bsecret_lab" {
		t.Fatalf("routed primary got %v", goPrimary.calls)
	}
	if len(defPrimary.calls) != 1 || defPrimary.calls[0] != "tm_borg_lab" {
		t.Fatalf("default primary got %v", defPrimary.calls)
	}
	if tsShadowEng.calls[0] != "tm_bsecret_lab" || defShadowEng.calls[0] != "tm_borg_lab" {
		t.Fatalf("shadows got %v and %v", tsShadowEng.calls, defShadowEng.calls)
	}
	if st := tsShadow.Stats(); st.Kinds[ShadowMatch] != 1 {
		t.Fatalf("routed shadow stats = %+v", st)
	}
	if st := defShadow.Stats(); st.Kinds[ShadowMatch] != 1 {
		t.Fatalf("default shadow stats = %+v", st)
	}
	// The primary's books are per topic, whichever engine answered.
	fs := f.Stats()
	if fs.Submitted != 2 || fs.Steak[SteakKey{"tm_bsecret_lab", OutcomeAdmitted}] != 1 || fs.Steak[SteakKey{"tm_borg_lab", OutcomeAdmitted}] != 1 {
		t.Fatalf("feed stats = %+v", fs)
	}
}

// TestRoutedShadowDownLeavesTheRoutedPrimaryAlone: the property the cutover
// rests on, per route. The old engine being down costs the new primary
// nothing but a counter on its own route.
func TestRoutedShadowDownLeavesTheRoutedPrimaryAlone(t *testing.T) {
	goPrimary := &stubSubmitter{steak: admitted("tm_bsecret_lab")}
	tsShadow := &Shadow{Submit: &shadowStub{err: errors.New("connection refused")}, Workers: 1, Name: "tm_bsecret_*"}
	startShadow(t, tsShadow)
	f := newFeed(t, &stubSubmitter{err: errors.New("the default engine must not be asked")}, "tm_bsecret_lab")
	f.Routes = []*Route{{Pattern: "tm_bsecret_*", Submit: goPrimary, Shadow: tsShadow}}
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_bsecret_lab"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	waitFor(t, "the shadow error", func() bool { return tsShadow.Stats().Kinds[ShadowError] == 1 })
	if fs := f.Stats(); fs.Submitted != 1 || fs.EngineError != 0 || fs.Steak[SteakKey{"tm_bsecret_lab", OutcomeAdmitted}] != 1 {
		t.Fatalf("primary stats = %+v", fs)
	}
}

// A route with no shadow: the routed primary alone, and the default shadow
// is not offered the routed topic.
func TestRouteWithoutShadowOffersNothing(t *testing.T) {
	defShadowEng := &shadowStub{steak: admitted("tm_bsecret_lab")}
	defShadow := &Shadow{Submit: defShadowEng, Workers: 1}
	startShadow(t, defShadow)
	goPrimary := &stubSubmitter{steak: admitted("tm_bsecret_lab")}
	f := newFeed(t, &stubSubmitter{}, "tm_bsecret_lab")
	f.Shadow = defShadow
	f.Routes = []*Route{{Pattern: "tm_bsecret_*", Submit: goPrimary}}
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_bsecret_lab"), beefObj)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(goPrimary.calls) != 1 {
		t.Fatalf("routed primary got %v", goPrimary.calls)
	}
	if n := total(defShadow.Stats()); n != 0 || defShadowEng.n() != 0 {
		t.Fatalf("the default shadow was offered a routed topic: %d counted, %d submitted", n, defShadowEng.n())
	}
}
