package main

import (
	"testing"

	"github.com/lightwebinc/overlay-bridge/feed"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestBuildInfoReportsWhatIsLinked is the outage guard. A deployed binary keeps
// the dependency version it was BUILT with for ever, and go.mod says nothing
// about what is running. On 2026-09-23 that difference cost a 90-minute total
// object-plane outage that no metric could see, and the only way to find it was
// `go version -m` on the binary over ssh.
//
// The labels must be real values read from the linker's build info, never
// placeholders: a build_info metric that reports "unknown" for the thing you
// are trying to compare is worse than none, because it looks like an answer.
func TestBuildInfoReportsWhatIsLinked(t *testing.T) {
	ver, shardCommon, goSDK := buildLabels()
	for name, got := range map[string]string{
		"version": ver, "shard_common": shardCommon, "go_sdk": goSDK,
	} {
		if got == "" {
			t.Errorf("%s label is empty; a missing label drops the series' identity", name)
		}
	}
	// Under `go test` there is no linker stamp and the module reports a
	// synthetic version, so `version` falls back to the "dev" default. What
	// must never appear is "(devel)": a host reporting that cannot answer
	// which release it is running, which is the whole point of the series.
	if ver == "(devel)" {
		t.Error(`version reads "(devel)": the linker stamp is not being preferred`)
	}
	if shardCommon == "unknown" {
		t.Error("shard_common reads 'unknown': the dep scan did not find the module that caused the outage")
	}
	if goSDK == "unknown" {
		t.Error("go_sdk reads 'unknown': the go-sdk pin is a SECURITY pin and must be visible in metrics")
	}
	t.Logf("build_info: version=%s shard_common=%s go_sdk=%s", ver, shardCommon, goSDK)
}

// TestSteakOutcomesArePresetAtZero guards the trap that would have made the
// most important alert on this bridge silently never fire.
//
// The steak counters are emitted by ranging over a map that starts EMPTY, so a
// series exists only once its outcome has happened at least once. A bridge
// restarted into a state where the engine admits nothing therefore never
// creates outcome="admitted", and a rule shaped
// `empty > 0 and on(instance) admitted == 0` finds no right-hand vector and
// matches nothing. The alert reads healthy precisely when the host is not.
func TestSteakOutcomesArePresetAtZero(t *testing.T) {
	var id [32]byte
	id[0] = 0x01
	f := &feed.Feed{Topics: map[[32]byte]string{id: "tm_lab_a"}}
	c := &collector{feed: f}

	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)

	got := map[string]bool{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatalf("write: %v", err)
		}
		var topic, outcome string
		for _, l := range d.GetLabel() {
			switch l.GetName() {
			case "topic":
				topic = l.GetValue()
			case "outcome":
				outcome = l.GetValue()
			}
		}
		if topic != "" {
			got[topic+"/"+outcome] = true
		}
	}
	for _, oc := range []string{"admitted", "empty", "error"} {
		if !got["tm_lab_a/"+oc] {
			t.Errorf("tm_lab_a/%s is absent; an absent series makes the alert that reads it match nothing", oc)
		}
	}
}
