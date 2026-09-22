package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestGroupCleanShutdownIsNotAFailure is the regression for every SIGTERM
// exiting 1. The lane terminator returns nil when its context is cancelled,
// and the group used to convert that into a task failure, so a rolling
// restart read as a crash to whatever supervised the process.
func TestGroupCleanShutdownIsNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := newGroup(ctx)
	g.go_("lane", func(ctx context.Context) error {
		<-ctx.Done()
		return nil // exactly what lanes.Lane.Serve does on cancel
	})
	cancel()
	err := g.wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("clean shutdown returned %v, want context.Canceled", err)
	}
}

// TestGroupEarlyReturnIsAFailure keeps the other half: a task that stops on
// its own before shutdown has failed, even with a nil error.
func TestGroupEarlyReturnIsAFailure(t *testing.T) {
	g := newGroup(context.Background())
	g.go_("lane", func(context.Context) error { return nil })
	err := g.wait()
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("early nil return = %v, want a task failure", err)
	}
}

func TestGroupReportsTheFirstRealError(t *testing.T) {
	g := newGroup(context.Background())
	g.go_("bad", func(context.Context) error { return errors.New("boom") })
	g.go_("ok", func(ctx context.Context) error { <-ctx.Done(); return nil })
	err := g.wait()
	var te *taskError
	if !errors.As(err, &te) || te.name != "bad" {
		t.Fatalf("err = %v, want the failure from task bad", err)
	}
	_ = time.Second
}
