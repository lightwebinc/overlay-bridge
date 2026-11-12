package spv

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/lightwebinc/overlay-bridge/internal/testpiece"
)

// sameBeef fails unless got (read in place) is what go-sdk's parse returns
// for raw: the same entries under the same txids, each transaction byte for
// byte, the same proof and the same linked sources.
func sameBeef(t testing.TB, raw []byte, got *transaction.Beef) {
	t.Helper()
	want, err := parse(raw)
	if err != nil {
		t.Fatalf("read in place, but go-sdk refuses: %v", err)
	}
	if got.Version != want.Version || len(got.BUMPs) != len(want.BUMPs) || len(got.Transactions) != len(want.Transactions) {
		t.Fatalf("version %d/%d, BUMPs %d/%d, transactions %d/%d", got.Version, want.Version,
			len(got.BUMPs), len(want.BUMPs), len(got.Transactions), len(want.Transactions))
	}
	if (got.NewestTxID == nil) != (want.NewestTxID == nil) || (got.NewestTxID != nil && *got.NewestTxID != *want.NewestTxID) {
		t.Fatalf("newest %v, want %v", got.NewestTxID, want.NewestTxID)
	}
	index := func(b *transaction.Beef, mp *transaction.MerklePath) int {
		for i, x := range b.BUMPs {
			if x == mp {
				return i
			}
		}
		return -1
	}
	for i := range want.BUMPs {
		if !bytes.Equal(got.BUMPs[i].Bytes(), want.BUMPs[i].Bytes()) {
			t.Fatalf("BUMP %d differs", i)
		}
	}
	for id, w := range want.Transactions {
		g, ok := got.Transactions[id]
		if !ok {
			t.Fatalf("no entry %s", id)
		}
		if g.DataFormat != w.DataFormat || g.BumpIndex != w.BumpIndex || (g.KnownTxID == nil) != (w.KnownTxID == nil) {
			t.Fatalf("%s: format %d/%d, bump %d/%d", id, g.DataFormat, w.DataFormat, g.BumpIndex, w.BumpIndex)
		}
		if !bytes.Equal(g.Transaction.Bytes(), w.Transaction.Bytes()) {
			t.Fatalf("%s: transaction bytes differ", id)
		}
		if *g.Transaction.TxID() != id {
			t.Fatalf("%s: keyed under a txid that is not its own", id)
		}
		if index(got, g.Transaction.MerklePath) != index(want, w.Transaction.MerklePath) {
			t.Fatalf("%s: proof differs", id)
		}
		for vin, win := range w.Transaction.Inputs {
			gin := g.Transaction.Inputs[vin]
			if (gin.SourceTransaction == nil) != (win.SourceTransaction == nil) {
				t.Fatalf("%s input %d: source linked %v, want %v", id, vin, gin.SourceTransaction != nil, win.SourceTransaction != nil)
			}
			if gin.SourceTransaction != nil && got.Transactions[*gin.SourceTXID].Transaction != gin.SourceTransaction {
				t.Fatalf("%s input %d: linked to a transaction that is not its source's entry", id, vin)
			}
		}
	}
}

func corpus(t testing.TB) map[string][]byte {
	f := newFixture(t)
	atomic, err := f.child.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	v1, err := f.child.BEEF()
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{
		"BEEF V1":      v1,
		"BEEF V2":      atomic[36:],
		"Atomic BEEF":  atomic,
		"piece 64 KiB": testpiece.New(t, 64<<10).BEEF,
		"piece 1 MiB":  testpiece.New(t, 1<<20).BEEF,
	}
}

// TestReadInPlaceIsGoSDKsParse: every shape the bridge carries reads in place
// to the Beef go-sdk parses, and BEEF V1 is left to go-sdk.
func TestReadInPlaceIsGoSDKsParse(t *testing.T) {
	for name, raw := range corpus(t) {
		beef, _, ok := readInPlace(raw, nil)
		if name == "BEEF V1" {
			if ok {
				t.Fatalf("%s: read in place, want it left to go-sdk", name)
			}
			continue
		}
		if !ok {
			t.Fatalf("%s: not read in place", name)
		}
		sameBeef(t, raw, beef)
	}
}

// TestReadInPlaceLeavesTheRestToGoSDK: the shapes outside the in-place
// reading are declined, never refused, so go-sdk decides them as before.
func TestReadInPlaceLeavesTheRestToGoSDK(t *testing.T) {
	p := testpiece.New(t, 4<<10)
	raw := p.BEEF
	cases := map[string][]byte{
		"empty":          nil,
		"short":          raw[:20],
		"truncated":      raw[:len(raw)-1],
		"trailing byte":  append(bytes.Clone(raw), 0),
		"unknown format": nil,
		"wrong subject":  nil,
	}
	bad := bytes.Clone(raw)
	bad[4] ^= 1 // the Atomic subject
	cases["wrong subject"] = bad
	// A V2 body whose only entry is txid-only.
	b := binary.LittleEndian.AppendUint32(nil, transaction.BEEF_V2)
	b = append(b, 0, 1, byte(transaction.TxIDOnly))
	b = append(b, make([]byte, 32)...)
	cases["unknown format"] = b
	for name, c := range cases {
		if _, _, ok := readInPlace(c, nil); ok {
			t.Fatalf("%s: read in place", name)
		}
	}
	// A length written long: the same transaction to go-sdk, other bytes to
	// hash, so it is go-sdk's.
	long := nonMinimal(t, raw)
	if _, _, ok := readInPlace(long, nil); ok {
		t.Fatal("non-minimal length: read in place")
	}
}

// nonMinimal rewrites the first transaction's input count (0x01) as the
// three-byte form of the same number.
func nonMinimal(t *testing.T, raw []byte) []byte {
	t.Helper()
	beef, _, ok := readInPlace(raw, nil)
	if !ok {
		t.Fatal("piece not read in place")
	}
	var first []byte
	for _, btx := range beef.Transactions {
		if b := btx.Transaction.Bytes(); first == nil || bytes.Index(raw, b) < bytes.Index(raw, first) {
			first = b
		}
	}
	at := bytes.Index(raw, first) + 4
	if raw[at] != 1 {
		t.Fatalf("input count %d, want 1", raw[at])
	}
	out := append(bytes.Clone(raw[:at]), 0xfd, 1, 0)
	return append(out, raw[at+1:]...)
}

// TestVerdictIsTheSameEitherReading: over valid and broken objects, the
// verdict with the in-place reading is the verdict without it, and the
// marker it carries is Marker's.
func TestVerdictIsTheSameEitherReading(t *testing.T) {
	key := bytes.Repeat([]byte{7}, MinKeyBytes)
	p := testpiece.New(t, 256<<10)
	objects := map[string][]byte{"piece": p.BEEF, "non-minimal": nonMinimal(t, p.BEEF)}
	for _, at := range []int{40, 60, len(p.BEEF) / 2, len(p.BEEF) - 3, len(p.BEEF) - 40, len(p.BEEF) - 120} {
		b := bytes.Clone(p.BEEF)
		b[at] ^= 0x10
		objects["flip "+itoa(at)] = b
	}
	for i := 0; i < 400; i++ { // every early byte: the proof, the funding transaction, the carrier's head
		b := bytes.Clone(p.BEEF)
		b[i] ^= 1
		objects["head "+itoa(i)] = b
	}
	for name, raw := range objects {
		fast := (&Verifier{Tracker: p.Tracker, Network: NetworkAll, MarkerKey: key}).Verify(t.Context(), raw)
		slow := (&Verifier{Tracker: p.Tracker, Network: NetworkAll, noInPlace: true}).Verify(t.Context(), raw)
		if fast.Result != slow.Result || fast.Reason != slow.Reason || fast.TxID != slow.TxID ||
			fast.Transactions != slow.Transactions || fast.Scripts != slow.Scripts ||
			fast.Inputs != slow.Inputs || fast.Proofs != slow.Proofs || fast.Depth != slow.Depth {
			t.Fatalf("%s: in place %+v, go-sdk %+v", name, fast, slow)
		}
		if fast.Result == ResultOK && fast.Marker != "" && fast.Marker != Marker(key, raw) {
			t.Fatalf("%s: marker %s, want %s", name, fast.Marker, Marker(key, raw))
		}
		if fast.Result != ResultOK && fast.Marker != "" {
			t.Fatalf("%s: a marker with result %s", name, fast.Result)
		}
	}
	if vd := (&Verifier{Tracker: p.Tracker, Network: NetworkAll, MarkerKey: key}).Verify(t.Context(), p.BEEF); vd.Marker != Marker(key, p.BEEF) {
		t.Fatalf("piece: marker %q, want Marker's", vd.Marker)
	}
}

func itoa(n int) string {
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + n%10)
		if n /= 10; n == 0 {
			return string(b[i:])
		}
	}
}

// TestDigestsAcrossBlocks: a transaction that spans many traversal blocks,
// and ones that start and end on block edges, hash to their txids.
func TestDigestsAcrossBlocks(t *testing.T) {
	for _, size := range []int{hashBlock - 300, hashBlock, hashBlock + 1, 3*hashBlock + 17, 4 << 20} {
		p := testpiece.New(t, size)
		beef, marker, ok := readInPlace(p.BEEF, bytes.Repeat([]byte{1}, MinKeyBytes))
		if !ok {
			t.Fatalf("size %d: not read in place", size)
		}
		sameBeef(t, p.BEEF, beef)
		if marker != Marker(bytes.Repeat([]byte{1}, MinKeyBytes), p.BEEF) {
			t.Fatalf("size %d: marker differs", size)
		}
		if _, ok := beef.Transactions[p.TxID]; !ok {
			t.Fatalf("size %d: the carrier is not under its txid", size)
		}
	}
}

// FuzzReadInPlace: whatever bytes arrive, an object read in place is one
// go-sdk parses, to the same Beef.
func FuzzReadInPlace(f *testing.F) {
	for _, raw := range corpus(f) {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		beef, _, ok := readInPlace(raw, nil)
		if !ok {
			return
		}
		sameBeef(t, raw, beef)
	})
}

func BenchmarkVerifyPieceGoSDK(b *testing.B) {
	p := testpiece.New(b, 4<<20)
	v := &Verifier{Tracker: p.Tracker, Network: NetworkAll, noInPlace: true}
	key := bytes.Repeat([]byte{7}, MinKeyBytes)
	b.SetBytes(int64(len(p.BEEF)))
	b.ReportAllocs()
	for b.Loop() {
		if vd := v.Verify(b.Context(), p.BEEF); vd.Result != ResultOK {
			b.Fatal(vd.Err)
		}
		_ = Marker(key, p.BEEF)
	}
}

func BenchmarkVerifyPieceInPlace(b *testing.B) {
	p := testpiece.New(b, 4<<20)
	key := bytes.Repeat([]byte{7}, MinKeyBytes)
	v := &Verifier{Tracker: p.Tracker, Network: NetworkAll, MarkerKey: key}
	b.SetBytes(int64(len(p.BEEF)))
	b.ReportAllocs()
	for b.Loop() {
		if vd := v.Verify(b.Context(), p.BEEF); vd.Result != ResultOK || vd.Marker == "" {
			b.Fatal(vd.Err)
		}
	}
}
