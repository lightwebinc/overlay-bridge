package headers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultWarmDepth is how many heights below and at the anchor a warm-up loads
// by default: one mainnet difficulty period, about two weeks of blocks. The
// objects a host verifies prove against recent blocks (a funding parent mined
// minutes to days ago); a proof older than this pays one anchor round trip on
// first use, as every proof did before warm-up existed.
const DefaultWarmDepth = 2016

// WarmStats is the progress of the startup warm-up.
type WarmStats struct {
	// Target is the number of heights the warm-up set out to load.
	Target uint64
	// Loaded and Failed count heights answered and not answered by the
	// fallback. A failed height is not an error for the bridge: it is fetched
	// on first use, exactly as without warm-up.
	Loaded, Failed uint64
	// Done is true once the warm-up has finished, whether or not every height
	// loaded. Duration is its wall time, zero until Done.
	Done     bool
	Duration time.Duration
}

// warmState is the store's warm-up progress. Atomics, because the metrics
// collector reads it while the warm-up runs.
type warmState struct {
	target, loaded, failed atomic.Uint64
	done                   atomic.Bool
	nanos                  atomic.Int64
}

// WarmStats returns the warm-up's progress.
func (s *Store) WarmStats() WarmStats {
	return WarmStats{
		Target:   s.warm.target.Load(),
		Loaded:   s.warm.loaded.Load(),
		Failed:   s.warm.failed.Load(),
		Done:     s.warm.done.Load(),
		Duration: time.Duration(s.warm.nanos.Load()),
	}
}

// Warm loads the roots of the depth heights ending at the anchor (anchor-depth+1
// through the anchor itself) from the fallback into the store's fallback cache,
// with workers concurrent requests.
//
// Why: the lane is a live tail, so every height at or below the anchor is
// answered by the fallback, and before this the first verification to name
// such a height paid that round trip on the verification path. Live, that put
// the bridge's verify p99 at 21 ms against a p50 of 0.66 ms. Loading them
// before the lanes open moves the round trips off the path.
//
// Provenance: a warmed root is the anchor's answer for a height at or below
// the anchor, so it is booked as history when it is read. The bridge did not
// receive it, so it is never booked as lane; and the lane, a live tail from the
// anchor, could not have carried it, so it is not booked as fallback either,
// which would report a lane gap on every host that verifies an older ancestor.
// A height the lane later delivers is answered from the lane, as before: the
// canonical chain is consulted before the cache.
//
// Warm never fails the bridge. A height the fallback cannot answer is counted
// and left to be fetched on first use; a cancelled ctx stops the warm-up where
// it is. The returned error is ctx's, or an error when there is no fallback.
func (s *Store) Warm(ctx context.Context, depth uint32, workers int) error {
	start := time.Now()
	defer func() {
		s.warm.nanos.Store(int64(time.Since(start)))
		s.warm.done.Store(true)
	}()
	if s.opt.Fallback == nil {
		return errors.New("headers: warm-up needs a fallback")
	}
	if depth == 0 {
		return nil
	}
	// Leave half the cache for heights asked later: the cache is cleared, not
	// evicted, when it fills, and a warm-up that filled it would be thrown away
	// by the first miss after it.
	if depth > fallbackCap/2 {
		depth = fallbackCap / 2
	}
	top := s.opt.Anchor.Height
	low := uint32(0)
	if top+1 > depth {
		low = top + 1 - depth
	}
	s.warm.target.Store(uint64(top-low) + 1)
	if workers < 1 {
		workers = 1
	}

	heights := make(chan uint32)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range heights {
				s.warmOne(ctx, h)
			}
		}()
	}
	// Newest first: the most recent blocks are the likeliest to be asked for,
	// so a warm-up cut short by its deadline has loaded the useful end.
feed:
	for h := top; ; h-- {
		select {
		case heights <- h:
		case <-ctx.Done():
			break feed
		}
		if h == low {
			break
		}
	}
	close(heights)
	wg.Wait()
	return ctx.Err()
}

func (s *Store) warmOne(ctx context.Context, h uint32) {
	if ctx.Err() != nil {
		s.warm.failed.Add(1)
		return
	}
	r, err := s.opt.Fallback.RootAt(ctx, h)
	if err != nil || r == nil {
		s.warm.failed.Add(1)
		return
	}
	s.mu.Lock()
	if len(s.fallbackRoots) < fallbackCap {
		s.fallbackRoots[h] = *r
	}
	s.mu.Unlock()
	s.warm.loaded.Add(1)
}
