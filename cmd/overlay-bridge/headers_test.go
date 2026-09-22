package main

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/lightwebinc/overlay-bridge/headers"
)

const easyBits = 0x207fffff

func mine(t *testing.T, prev, root chainhash.Hash) []byte {
	t.Helper()
	hdr := make([]byte, headers.Size)
	binary.LittleEndian.PutUint32(hdr[0:4], 1)
	copy(hdr[4:36], prev[:])
	copy(hdr[36:68], root[:])
	binary.LittleEndian.PutUint32(hdr[72:76], easyBits)
	// Even the trivial floor rejects about one hash in two, so grind.
	for nonce := uint32(0); nonce < 1<<20; nonce++ {
		binary.LittleEndian.PutUint32(hdr[76:80], nonce)
		s, _ := headers.New(headers.Options{Anchor: headers.Anchor{Hash: prev, Height: 1}, MinBits: easyBits})
		if _, err := s.Observe(context.Background(), hdr); err == nil {
			return hdr
		}
	}
	t.Fatal("could not mine a header")
	return nil
}

// TestHeaderHandlerDoesNotPoisonTheTrackerOnAFork is the regression for the
// exact path the review found: the lane handler used to teach the tracker
// EVERY chained header's root, including a competing header at a height
// already held, so the tracker then answered true for a root the canonical
// chain did not commit to, and a reorganisation never corrected it.
func TestHeaderHandlerDoesNotPoisonTheTrackerOnAFork(t *testing.T) {
	store, err := headers.New(headers.Options{Anchor: headers.Anchor{Hash: chainhash.Hash{0xAA}, Height: 100}, MinBits: easyBits})
	if err != nil {
		t.Fatal(err)
	}
	tracker := headers.NewTracker(store)
	handle := headerHandler(store, tracker)
	anchor, _ := store.Tip()

	rootA, rootB := chainhash.Hash{1}, chainhash.Hash{2}
	if err := handle(context.Background(), mine(t, anchor, rootA)); err != nil {
		t.Fatalf("a: %v", err)
	}
	if h, _ := tracker.CurrentHeight(context.Background()); h != 101 {
		t.Fatalf("height after a = %d, want 101", h)
	}
	// A competing header at 101 arrives through the SAME handler.
	if err := handle(context.Background(), mine(t, anchor, rootB)); err != nil {
		t.Fatalf("b: %v", err)
	}
	ok, _ := tracker.IsValidRootForHeight(context.Background(), &rootA, 101)
	if !ok {
		t.Fatal("the competing header displaced the canonical root in the tracker")
	}
	ok, _ = tracker.IsValidRootForHeight(context.Background(), &rootB, 101)
	if ok {
		t.Fatal("the tracker answered true for a root the canonical chain does not commit to")
	}
	if h, _ := tracker.CurrentHeight(context.Background()); h != 101 {
		t.Fatalf("height moved to %d on a non-tip header", h)
	}
}
