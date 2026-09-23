package main

import "testing"

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
	// Under `go test` the main module reports a synthetic version, so only the
	// DEPS are assertable here — and they are the ones that mattered.
	if shardCommon == "unknown" {
		t.Error("shard_common reads 'unknown': the dep scan did not find the module that caused the outage")
	}
	if goSDK == "unknown" {
		t.Error("go_sdk reads 'unknown': the go-sdk pin is a SECURITY pin and must be visible in metrics")
	}
	t.Logf("build_info: version=%s shard_common=%s go_sdk=%s", ver, shardCommon, goSDK)
}
