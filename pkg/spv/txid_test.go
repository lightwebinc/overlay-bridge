package spv

import (
	"context"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/lightwebinc/overlay-bridge/internal/testpiece"
)

// TestParseKeysTransactionsByTheirTxID pins what verify relies on to skip a
// second hash of every transaction: each entry the parse returns is keyed
// by its transaction's txid, in every BEEF form the bridge reads.
func TestParseKeysTransactionsByTheirTxID(t *testing.T) {
	f := newFixture(t)
	atomic, err := f.child.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"BEEF V1":                   beefOf(t, f.child),
		"BEEF V2":                   atomic[36:],
		"Atomic BEEF":               atomic,
		"piece carrier (Atomic V2)": testpiece.New(t, 64<<10).BEEF,
	} {
		beef, err := parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		n := 0
		for key, btx := range beef.Transactions {
			if btx == nil || btx.Transaction == nil {
				continue
			}
			n++
			if got := *btx.Transaction.TxID(); got != key {
				t.Fatalf("%s: entry keyed %s holds transaction %s", name, key, got)
			}
		}
		if n < 2 {
			t.Fatalf("%s: %d transactions, want the source and its spend", name, n)
		}
	}
}

// TestVerifyHashesACarrierOnce bounds what one verification of a large
// carrier allocates. The parse copies the carrier's script out and
// serializes the carrier once for its txid, and the signature check
// serializes its outputs once: three times the carrier. Hashing it again
// for the graph made it four.
func TestVerifyHashesACarrierOnce(t *testing.T) {
	const size = 4 << 20
	p := testpiece.New(t, size)
	v := &Verifier{Tracker: p.Tracker, Network: NetworkAll}
	check(t, v.Verify(t.Context(), p.BEEF), ResultOK, ReasonNone)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	vd := v.Verify(t.Context(), p.BEEF)
	runtime.ReadMemStats(&after)
	check(t, vd, ResultOK, ReasonNone)
	if vd.TxID != p.TxID {
		t.Fatalf("txid %s, want the carrier %s", vd.TxID, p.TxID)
	}
	alloc := after.TotalAlloc - before.TotalAlloc
	if limit := uint64(7 * size / 2); alloc > limit {
		t.Fatalf("one verification of a %d-byte carrier allocated %d bytes, over %d: a transaction is serialized more than the parse and the signature check need", size, alloc, limit)
	}
}

// BenchmarkVerifyPiece is one verification of a 4 MiB piece carrier, the
// largest object the lane carries in volume (B/op is the copies of it).
func BenchmarkVerifyPiece(b *testing.B) {
	p := testpiece.New(b, 4<<20)
	v := &Verifier{Tracker: p.Tracker, Network: NetworkAll}
	b.SetBytes(int64(len(p.BEEF)))
	b.ReportAllocs()
	for b.Loop() {
		if vd := v.Verify(context.Background(), p.BEEF); vd.Result != ResultOK {
			b.Fatal(vd.Err)
		}
	}
}

// TestTransactionListedTwiceIsHashedOnUse covers the one transaction the
// parse's map does not hold: a source listed twice, whose earlier copy the
// parse linked to the spend between them while the map kept the later one.
// The graph hashes that copy on first use, and the object verifies.
func TestTransactionListedTwiceIsHashedOnUse(t *testing.T) {
	f := newFixture(t)
	b := binary.LittleEndian.AppendUint32(nil, transaction.BEEF_V2)
	b = append(b, 1) // one BUMP
	b = append(b, f.src.MerklePath.Bytes()...)
	b = append(b, 3) // the source, its spend, the source again
	b = append(b, byte(transaction.RawTxAndBumpIndex), 0)
	b = f.src.AppendBytes(b)
	b = append(b, byte(transaction.RawTx))
	b = f.child.AppendBytes(b)
	b = append(b, byte(transaction.RawTxAndBumpIndex), 0)
	b = f.src.AppendBytes(b)

	beef, err := parse(b)
	if err != nil {
		t.Fatal(err)
	}
	child := beef.Transactions[*f.child.TxID()].Transaction
	if child.Inputs[0].SourceTransaction == beef.Transactions[*f.src.TxID()].Transaction {
		t.Fatal("the parse linked the copy its map holds, so this object no longer reaches the hash-on-use path; the test needs another shape")
	}
	vd := (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(context.Background(), b)
	check(t, vd, ResultOK, ReasonNone)
	if vd.TxID != *f.child.TxID() {
		t.Fatalf("txid %s, want the spend %s", vd.TxID, f.child.TxID())
	}
}
