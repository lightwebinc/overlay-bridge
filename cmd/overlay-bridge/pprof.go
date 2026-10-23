package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"time"
)

// Block and mutex profiling rates while -pprof-listen is set. A blocking
// event of 10 us or more is always recorded and a shorter one in proportion
// to its length; one mutex contention event in 10 is recorded. Both are the
// low-overhead settings continuous profilers ship with. With the flag unset
// the runtime records neither.
const (
	blockProfileRateNS   = 10_000
	mutexProfileFraction = 10
)

// loopbackAddr checks -pprof-listen: a host:port whose host is a loopback
// address or localhost. Profiles carry code paths and memory contents and
// the endpoint has no authentication, so it never listens beyond the
// machine; a collector runs beside the bridge.
func loopbackAddr(raw string) error {
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return fmt.Errorf("-pprof-listen %q is not host:port", raw)
	}
	if n, perr := strconv.Atoi(port); perr != nil || n <= 0 || n > 65535 {
		return fmt.Errorf("-pprof-listen %q: %q is not a port", raw, port)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("-pprof-listen %q: the host must be a loopback address (127.0.0.1, ::1 or localhost)", raw)
	}
	return nil
}

// servePprof serves net/http/pprof on addr until ctx ends, with block and
// mutex profiling on for the life of the process. A listener that cannot
// open is logged and the bridge carries on: profiling is an aid, never a
// reason to stop delivery.
func servePprof(ctx context.Context, addr string, log *slog.Logger) error {
	runtime.SetBlockProfileRate(blockProfileRateNS)
	runtime.SetMutexProfileFraction(mutexProfileFraction)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index) // allocs, block, goroutine, heap, mutex, threadcreate
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	// No write timeout: a CPU profile or trace answers after the seconds it
	// was asked to record.
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	log.Info("pprof listening", "addr", addr, "block_rate_ns", blockProfileRateNS, "mutex_fraction", mutexProfileFraction)
	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Warn("pprof listener failed; profiling is off", "addr", addr, "err", err)
		<-ctx.Done()
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	return ctx.Err()
}
