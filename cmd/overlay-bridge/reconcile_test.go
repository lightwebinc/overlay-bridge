package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestReconcileTopicsAgainstASubscriptionSource covers each outcome the
// startup check logs (docs/contracts.md, "The subscription source"): one line
// per outcome, worded for the subscription source whatever service answers
// it, none of them stopping the bridge. The token reaches the source in its
// Authorization header, the flag ahead of the environment, and never the log.
func TestReconcileTopicsAgainstASubscriptionSource(t *testing.T) {
	const flagToken, envToken = "flag-token-not-for-logs", "env-token-not-for-logs"
	auth := make(chan string, 4)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth <- r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"consumers":[{"id":"consumer-a","subscription":{"beefTopics":["tm_a","tm_b"]}}]}`))
	}))
	defer src.Close()
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close() // nothing listens there now

	for _, tc := range []struct {
		name      string
		url       string
		topics    []string
		flag, env string
		want      []string // in the one log line
		wantAuth  string   // the source's Authorization header; "" = no answer expected
	}{
		{name: "off", topics: []string{"tm_a"}, flag: flagToken,
			want: []string{"topic reconciliation is OFF"}},
		{name: "match, the flag token ahead of the environment", url: src.URL, topics: []string{"tm_a", "tm_b"}, flag: flagToken, env: envToken,
			want: []string{"topics reconciled"}, wantAuth: "Bearer " + flagToken},
		{name: "mismatch, the environment token", url: src.URL, topics: []string{"tm_a"}, env: envToken,
			want:     []string{"TOPIC MISMATCH", `watch="overlay_bridge_feed_total{outcome=\"unknown_topic\"}"`},
			wantAuth: "Bearer " + envToken},
		{name: "unreachable", url: gone.URL, topics: []string{"tm_a"}, flag: flagToken,
			want: []string{"topic reconciliation could not read the subscription source", "starting anyway"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OVERLAY_BRIDGE_SUBSCRIPTION_TOKEN", tc.env)
			var out bytes.Buffer
			c := config{subURL: tc.url, subID: "consumer-a", subToken: tc.flag}
			reconcileTopics(context.Background(), c, tc.topics, slog.New(slog.NewTextHandler(&out, nil)))

			got := out.String()
			if n := strings.Count(got, "\n"); n != 1 {
				t.Fatalf("logged %d lines, want 1: %q", n, got)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("log = %q, want it to contain %q", got, w)
				}
			}
			if strings.Contains(got, flagToken) || strings.Contains(got, envToken) {
				t.Errorf("the token reached the log: %q", got)
			}
			if tc.wantAuth != "" {
				if a := <-auth; a != tc.wantAuth {
					t.Errorf("Authorization = %q, want %q", a, tc.wantAuth)
				}
			}
		})
	}
}
