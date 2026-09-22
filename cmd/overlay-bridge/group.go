package main

import (
	"context"
	"errors"
	"sync"
)

// group runs the bridge's long-lived tasks and reports the first real failure.
//
// Every task is expected to run until the context is cancelled, so a task that
// returns early has failed even if it returns nil, and the whole process comes
// down: a bridge with its object lane silently dead is worse than one that
// exits and is restarted.
type group struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu  sync.Mutex
	err error
}

func newGroup(ctx context.Context) *group {
	c, cancel := context.WithCancel(ctx)
	return &group{ctx: c, cancel: cancel}
}

// go_ starts a task. The trailing underscore keeps it out of the way of the
// keyword.
func (g *group) go_(name string, fn func(context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn(g.ctx)
		if err == nil {
			err = errors.New("task returned with no error before shutdown")
		}
		g.fail(name, err)
	}()
}

func (g *group) fail(name string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	g.mu.Lock()
	if g.err == nil {
		g.err = &taskError{name: name, err: err}
	}
	g.mu.Unlock()
	g.cancel()
}

// wait blocks until every task has stopped and returns the first failure.
func (g *group) wait() error {
	<-g.ctx.Done()
	g.cancel()
	g.wg.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	return context.Canceled
}

type taskError struct {
	name string
	err  error
}

func (e *taskError) Error() string { return e.name + ": " + e.err.Error() }
func (e *taskError) Unwrap() error { return e.err }
