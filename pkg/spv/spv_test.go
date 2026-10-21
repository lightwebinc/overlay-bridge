package spv

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter/scriptflag"
	sdkspv "github.com/bsv-blockchain/go-sdk/spv"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
)

const wif = "KznvCNc6Yf4iztSThoMH6oHWzH9EgjfodKxmeuUGPq5DEX5maspS"

// roots is a chain tracker over a fixed height->root map.
type roots struct {
	m   map[uint32]chainhash.Hash
	tip uint32
	err error
}

func (r *roots) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, h uint32) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	got, ok := r.m[h]
	return ok && got.IsEqual(root), nil
}

func (r *roots) CurrentHeight(context.Context) (uint32, error) { return r.tip, nil }

// fixture builds a mined source (alone in block 100, so its txid is the root)
// paying a P2PKH output, and an unmined child spending it.
type fixture struct {
	src, child *transaction.Transaction
	lock       *script.Script
	unlock     *p2pkh.P2PKH
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	priv, err := ec.PrivateKeyFromWif(wif)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), true)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := p2pkh.Unlock(priv, nil)
	if err != nil {
		t.Fatal(err)
	}
	src := transaction.NewTransaction()
	src.AddInput(&transaction.TransactionInput{
		SourceTXID: &chainhash.Hash{1}, UnlockingScript: &script.Script{}, SequenceNumber: transaction.DefaultSequenceNumber,
	})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: 100_000, LockingScript: lock})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: 100_000, LockingScript: lock})
	mp, err := transaction.NewMerklePathFromCoinbaseTxid(src.TxID(), 100)
	if err != nil {
		t.Fatal(err)
	}
	src.MerklePath = mp
	f := &fixture{src: src, lock: lock, unlock: unlock}
	f.child = f.spend(t, 0)
	return f
}

func (f *fixture) spend(t *testing.T, vout uint32) *transaction.Transaction {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(f.src, vout, f.unlock)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 99_000, LockingScript: f.lock})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	return tx
}

func (f *fixture) tracker() *roots {
	return &roots{m: map[uint32]chainhash.Hash{100: *f.src.TxID()}, tip: 200}
}

func beefOf(t *testing.T, tx *transaction.Transaction) []byte {
	t.Helper()
	b, err := tx.BEEF()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func check(t *testing.T, vd Verdict, res Result, reason Reason) {
	t.Helper()
	if vd.Result != res || vd.Reason != reason {
		t.Fatalf("verdict %s/%q, want %s/%q (err %v)", vd.Result, vd.Reason, res, reason, vd.Err)
	}
}

func TestGoodBEEF(t *testing.T) {
	f := newFixture(t)
	v := &Verifier{Tracker: f.tracker(), Network: NetworkAll}
	vd := v.Verify(t.Context(), beefOf(t, f.child))
	check(t, vd, ResultOK, ReasonNone)
	if vd.Transactions != 2 || vd.Scripts != 1 {
		t.Fatalf("transactions %d scripts %d, want 2 and 1", vd.Transactions, vd.Scripts)
	}
	if vd.TxID != *f.child.TxID() {
		t.Fatalf("txid %s, want the child", vd.TxID)
	}
}

func TestGoodAtomicBEEF(t *testing.T) {
	f := newFixture(t)
	b, err := f.child.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	check(t, (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(t.Context(), b), ResultOK, ReasonNone)
}

func TestBadScript(t *testing.T) {
	f := newFixture(t)
	bad := f.spend(t, 0)
	// A valid signature over a different transaction: the unlocking script of
	// a sibling spend of the other output, so the signature does not verify.
	other := f.spend(t, 1)
	bad.Inputs[0].UnlockingScript = other.Inputs[0].UnlockingScript
	check(t, (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(t.Context(), beefOf(t, bad)), ResultRefused, ReasonScript)
}

func TestBadMerklePath(t *testing.T) {
	f := newFixture(t)
	tr := f.tracker()
	tr.m[100] = chainhash.Hash{0xaa} // the chain commits to a different root
	check(t, (&Verifier{Tracker: tr, Network: NetworkAll}).Verify(t.Context(), beefOf(t, f.child)), ResultRefused, ReasonMerkle)
}

func TestUnknownRootBelowTip(t *testing.T) {
	f := newFixture(t)
	tr := &roots{m: map[uint32]chainhash.Hash{}, tip: 200}
	check(t, (&Verifier{Tracker: tr, Network: NetworkAll}).Verify(t.Context(), beefOf(t, f.child)), ResultRefused, ReasonMerkle)
}

func TestUnknownRootAboveTipIsNotARefusal(t *testing.T) {
	f := newFixture(t)
	tr := &roots{m: map[uint32]chainhash.Hash{}, tip: 50}
	check(t, (&Verifier{Tracker: tr, Network: NetworkAll}).Verify(t.Context(), beefOf(t, f.child)), ResultError, ReasonNotYet)
}

func TestTrackerFailureIsNotARefusal(t *testing.T) {
	f := newFixture(t)
	tr := f.tracker()
	tr.err = errors.New("header service down")
	check(t, (&Verifier{Tracker: tr, Network: NetworkAll}).Verify(t.Context(), beefOf(t, f.child)), ResultError, ReasonTracker)
}

func TestGarbageIsAParseRefusal(t *testing.T) {
	v := &Verifier{Tracker: &roots{}}
	check(t, v.Verify(t.Context(), []byte{0x01, 0x00, 0xbe, 0xef, 0xff}), ResultRefused, ReasonParse)
	check(t, v.Verify(t.Context(), nil), ResultRefused, ReasonParse)
}

func TestNoTrackerIsAnError(t *testing.T) {
	check(t, (&Verifier{}).Verify(t.Context(), []byte{1, 2, 3, 4}), ResultError, ReasonTracker)
}

// A BEEF with two tips, the first valid and the second not, is refused:
// every unproven transaction is verified, not only the one an engine would
// pick as the subject.
func TestEveryTipIsVerified(t *testing.T) {
	f := newFixture(t)
	bad := f.spend(t, 1)
	bad.Inputs[0].UnlockingScript = f.child.Inputs[0].UnlockingScript
	beef := transaction.NewBeefV2()
	for _, tx := range []*transaction.Transaction{f.src, bad, f.child} {
		if _, err := beef.MergeTransaction(tx); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := beef.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	vd := (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(t.Context(), raw)
	check(t, vd, ResultRefused, ReasonScript)
	if vd.TxID != *bad.TxID() {
		t.Fatalf("refused %s, want the bad tip %s", vd.TxID, bad.TxID())
	}
	if vd.Scripts < 1 {
		t.Fatalf("scripts %d, want the bad one run", vd.Scripts)
	}
}

func TestParseNetwork(t *testing.T) {
	for _, s := range []string{"main", "test", "all"} {
		if _, err := ParseNetwork(s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ParseNetwork("regtest"); err == nil {
		t.Fatal("regtest accepted")
	}
}

func TestMarker(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	body := []byte("object")
	m := Marker(key, body)
	if !CheckMarker(key, body, m) {
		t.Fatal("marker does not check")
	}
	if CheckMarker(key, []byte("other object"), m) {
		t.Fatal("marker moved to another body")
	}
	if CheckMarker([]byte("fedcba9876543210fedcba9876543210"), body, m) {
		t.Fatal("marker checked under another key")
	}
	if CheckMarker(key[:16], body, Marker(key[:16], body)) {
		t.Fatal("short key accepted")
	}
	for _, v := range []string{"", "v1=", "v1=zz", "v2=" + m[3:], m[3:]} {
		if CheckMarker(key, body, v) {
			t.Fatalf("malformed marker %q accepted", v)
		}
	}
}

func TestLoadKey(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "k")
	if err := os.WriteFile(p, []byte("0123456789abcdef0123456789abcdef0123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := LoadKey(p)
	if err != nil || len(k) != 36 {
		t.Fatalf("key %q err %v", k, err)
	}
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(p); err == nil {
		t.Fatal("world-readable key accepted")
	}
	short := filepath.Join(dir, "s")
	if err := os.WriteFile(short, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(short); err == nil {
		t.Fatal("short key accepted")
	}
}

// The engine (TypeScript, node:crypto) computes the same value: this vector
// is repeated in overlay-blueprints src/bridgetrust.test.ts.
func TestMarkerVector(t *testing.T) {
	const want = "v1=0d823c5f823e5058f96797f2c871875a949ecddf076b007a357fa120dd3ef038"
	if got := Marker([]byte("0123456789abcdef0123456789abcdef"), []byte("object")); got != want {
		t.Fatalf("marker %s, want %s", got, want)
	}
}

func BenchmarkVerify(b *testing.B) {
	t := &testing.T{}
	f := newFixture(t)
	raw := beefOf(t, f.child)
	v := &Verifier{Tracker: f.tracker(), Network: NetworkAll}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if vd := v.Verify(context.Background(), raw); vd.Result != ResultOK {
			b.Fatal(vd.Err)
		}
	}
}

// carrier spends output vout of the fixture's source as a committed-record
// carrier does: nLockTime 4102444800 and a non-final input.
func (f *fixture) carrier(t *testing.T, vout uint32, signer *p2pkh.P2PKH) *transaction.Transaction {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(f.src, vout, signer)
	tx.Inputs[0].SequenceNumber = 0
	tx.LockTime = 4102444800
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 99_000, LockingScript: f.lock})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestNonFinalCarrierPassesUnderIgnore(t *testing.T) {
	f := newFixture(t)
	raw := beefOf(t, f.carrier(t, 0, f.unlock))
	check(t, (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(t.Context(), raw), ResultOK, ReasonNone)
	vd := (&Verifier{Tracker: f.tracker(), Network: NetworkAll, Finality: FinalityRequire}).Verify(t.Context(), raw)
	check(t, vd, ResultRefused, ReasonStructure)
	if !errors.Is(vd.Err, sdkspv.ErrNonFinalTransaction) {
		t.Fatalf("err %v", vd.Err)
	}
	// go-sdk v1.7.2's own Verify refuses it: the reason this package exists.
	tx, err := transaction.NewTransactionFromBEEF(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := sdkspv.Verify(t.Context(), tx, sdkspv.WithActivationHeights(f.tracker(), scriptflag.ActivationHeights{}), nil); ok {
		t.Fatal("go-sdk accepted a non-final carrier; the policy may no longer be needed")
	}
}

func TestBadScriptInANonFinalTxFails(t *testing.T) {
	f := newFixture(t)
	bad := f.carrier(t, 0, f.unlock)
	bad.Inputs[0].UnlockingScript = f.carrier(t, 1, f.unlock).Inputs[0].UnlockingScript
	check(t, (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(t.Context(), beefOf(t, bad)), ResultRefused, ReasonScript)
}

// coinbaseFixture is a coinbase mined at height 100 and an unmined spend.
func coinbaseFixture(t *testing.T) (*fixture, *transaction.Transaction) {
	t.Helper()
	f := newFixture(t)
	cb := transaction.NewTransaction()
	cb.AddInput(&transaction.TransactionInput{
		SourceTXID: &chainhash.Hash{}, SourceTxOutIndex: 0xffffffff, UnlockingScript: &script.Script{0x51}, SequenceNumber: transaction.DefaultSequenceNumber,
	})
	cb.AddOutput(&transaction.TransactionOutput{Satoshis: 100_000, LockingScript: f.lock})
	mp, err := transaction.NewMerklePathFromCoinbaseTxid(cb.TxID(), 100)
	if err != nil {
		t.Fatal(err)
	}
	cb.MerklePath = mp
	f.src = cb
	return f, f.spend(t, 0)
}

func TestCoinbaseMaturityAsV172(t *testing.T) {
	f, spend := coinbaseFixture(t)
	raw := beefOf(t, spend)
	for _, c := range []struct {
		tip  uint32
		want Result
	}{{150, ResultRefused}, {198, ResultRefused}, {199, ResultOK}, {300, ResultOK}} {
		tr := &roots{m: map[uint32]chainhash.Hash{100: *f.src.TxID()}, tip: c.tip}
		vd := (&Verifier{Tracker: tr, Network: NetworkAll}).Verify(t.Context(), raw)
		if vd.Result != c.want {
			t.Fatalf("tip %d: %s (%v), want %s", c.tip, vd.Result, vd.Err, c.want)
		}
		// Same verdict as go-sdk v1.7.2 on this final transaction.
		tx, _ := transaction.NewTransactionFromBEEF(raw)
		ok, _ := sdkspv.Verify(t.Context(), tx, sdkspv.WithActivationHeights(tr, scriptflag.ActivationHeights{}), nil)
		if ok != (c.want == ResultOK) {
			t.Fatalf("tip %d: go-sdk says %v", c.tip, ok)
		}
	}
}

func TestP2SHOutputRefusedAsV172(t *testing.T) {
	f := newFixture(t)
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(f.src, 0, f.unlock)
	p2sh := script.Script(append(append([]byte{0xa9, 0x14}, make([]byte, 20)...), 0x87))
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &p2sh})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	vd := (&Verifier{Tracker: f.tracker(), Network: NetworkMain}).Verify(t.Context(), beefOf(t, tx))
	if vd.Result != ResultRefused || !errors.Is(vd.Err, sdkspv.ErrP2SHOutput) {
		t.Fatalf("%s %v", vd.Result, vd.Err)
	}
}

func TestOutputsExceedInputsRefused(t *testing.T) {
	f := newFixture(t)
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(f.src, 0, f.unlock)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 100_001, LockingScript: f.lock})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	vd := (&Verifier{Tracker: f.tracker(), Network: NetworkAll}).Verify(t.Context(), beefOf(t, tx))
	if !errors.Is(vd.Err, sdkspv.ErrOutputsExceedInputs) {
		t.Fatalf("%s %v", vd.Result, vd.Err)
	}
}
