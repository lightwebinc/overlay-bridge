package main

import (
	"strings"
	"testing"
)

func baseConfig() config {
	return config{mode: "all", engine: "http://host:8080", headerAnchor: "http://host:9000", topics: "tm_a", edgeIngress: "10.0.0.1", facadeListen: "[::]:9175"}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*config)
		wantErr string
	}{
		{"a valid config", func(*config) {}, ""},
		{"unknown mode", func(c *config) { c.mode = "bogus" }, "-mode must be"},
		{"no engine", func(c *config) { c.engine = "" }, "-engine is required"},
		// The TypeScript host mounts submit on the app root and cannot be
		// given a prefix, so a prefix here is simply wrong.
		{"engine carries a prefix", func(c *config) { c.engine += "/api/v1" }, "bare origin"},
		{"no anchor", func(c *config) { c.headerAnchor = "" }, "-header-anchor is required"},
		{"no topics", func(c *config) { c.topics = "" }, "-topics is required"},
		// A facade with nowhere to publish would accept submissions, admit
		// them locally and silently never put them on the plane.
		{"facade with no ingress", func(c *config) { c.edgeIngress = "" }, "cannot publish"},
		// Sink mode terminates and counts, so it needs none of it.
		{"sink needs nothing", func(c *config) {
			*c = config{mode: "sink"}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.mutate(&c)
			err := c.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestIngressAddrsAppliesTheDefaultPort(t *testing.T) {
	got := ingressAddrs("10.0.0.1, [fd00::1] ,10.0.0.2:9999", 8725)
	want := []string{"10.0.0.1:8725", "[fd00::1]:8725", "10.0.0.2:9999"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// TestResolveSourceRefusesAnAddressWeDoNotHold pins the assertion that matters:
// publishing from an address this machine does not carry means own-traffic
// exclusion never matches, so the fabric delivers our own objects back to us.
// That is billable egress plus a full verify per object, not a harmless
// duplicate, and it is silent, so it is caught at startup instead.
func TestResolveSourceRefusesAnAddressWeDoNotHold(t *testing.T) {
	if _, err := resolveSource("192.0.2.123"); err == nil {
		t.Fatal("an address not on this machine was accepted")
	}
	if _, err := resolveSource("not-an-ip"); err == nil {
		t.Fatal("a non-address was accepted")
	}
	// Loopback is always present, so it is the one address a test may assume.
	if _, err := resolveSource("127.0.0.1"); err != nil {
		t.Fatalf("loopback rejected: %v", err)
	}
}

func TestSplitList(t *testing.T) {
	got := splitList(" a , ,b,  ")
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
	if len(splitList("")) != 0 {
		t.Fatal("empty string produced entries")
	}
}
