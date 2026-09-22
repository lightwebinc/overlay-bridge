package headers

// Stats is a point-in-time snapshot of the header store.
type Stats struct {
	// Observed counts headers accepted off the lane, including re-deliveries.
	Observed uint64
	// Rejected counts headers refused on proof of work. A non-zero value is
	// not noise: this lane carries only headers, and a rejection means
	// something upstream sent one that does not meet the floor.
	Rejected uint64
	// Orphaned counts headers whose parent this tail never saw and could not
	// resolve. A burst is the expected shape after a reconnect.
	Orphaned uint64
	// Reanchored counts gaps closed through the lookup.
	Reanchored uint64
	// Replaced counts headers arriving for a height already claimed: a fork,
	// retained rather than overwritten.
	Replaced uint64
	// Pruned counts heights dropped out of the retention window.
	Pruned uint64
	// TipHeight is the best height the chain has reached. It reads zero until
	// the first header chains, which is the signal that the lane is not
	// feeding: an engine asking how far the chain has got would be told zero.
	TipHeight uint32
	// Heights is the number of retained canonical heights.
	Heights int
}

// Stats returns a counter snapshot.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Observed:   s.observed,
		Rejected:   s.rejected,
		Orphaned:   s.orphaned,
		Reanchored: s.reanchored,
		Replaced:   s.replaced,
		Pruned:     s.pruned,
		TipHeight:  s.tipH,
		Heights:    len(s.canon),
	}
}
