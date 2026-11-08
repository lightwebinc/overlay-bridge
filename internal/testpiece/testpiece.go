// Package testpiece builds, for tests and benchmarks, a BEEF shaped as a
// storage piece carrier: an Atomic BEEF (BRC-95) over a BEEF V2 (BRC-96)
// holding one BUMP and two transactions, a mined funding transaction and the
// unmined carrier that spends it, whose one output carries the piece in a
// push. In a storage deployment these carry most of the lane's bytes, so
// they are the shape the bridge's per-object costs are measured on.
package testpiece

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
)

const wif = "KznvCNc6Yf4iztSThoMH6oHWzH9EgjfodKxmeuUGPq5DEX5maspS"

// Piece is one carrier BEEF and the chain tracker its funding proof
// verifies against.
type Piece struct {
	BEEF    []byte
	Tracker *Roots
	// TxID is the carrier's txid, the Atomic BEEF's subject.
	TxID chainhash.Hash
}

// Roots is a chain tracker over a fixed height->root map.
type Roots struct {
	M   map[uint32]chainhash.Hash
	Tip uint32
}

// IsValidRootForHeight reports whether root is the root held for height.
func (r *Roots) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, h uint32) (bool, error) {
	got, ok := r.M[h]
	return ok && got.IsEqual(root), nil
}

// CurrentHeight returns the fixed tip.
func (r *Roots) CurrentHeight(context.Context) (uint32, error) { return r.Tip, nil }

// New builds a piece carrier whose output pushes size bytes. The funding
// transaction is mined alone in block 100, so its txid is the root.
func New(tb testing.TB, size int) *Piece {
	tb.Helper()
	priv, err := ec.PrivateKeyFromWif(wif)
	if err != nil {
		tb.Fatal(err)
	}
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), true)
	if err != nil {
		tb.Fatal(err)
	}
	fund, err := p2pkh.Lock(addr)
	if err != nil {
		tb.Fatal(err)
	}
	unlock, err := p2pkh.Unlock(priv, nil)
	if err != nil {
		tb.Fatal(err)
	}
	src := transaction.NewTransaction()
	src.AddInput(&transaction.TransactionInput{
		SourceTXID: &chainhash.Hash{1}, UnlockingScript: &script.Script{}, SequenceNumber: transaction.DefaultSequenceNumber,
	})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: 100_000, LockingScript: fund})
	mp, err := transaction.NewMerklePathFromCoinbaseTxid(src.TxID(), 100)
	if err != nil {
		tb.Fatal(err)
	}
	src.MerklePath = mp

	// The carrier's output, as a piece carrier's: <key> OP_CHECKSIG
	// <record> <signature> OP_2DROP.
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 7)
	}
	lock := &script.Script{}
	for _, err := range []error{
		lock.AppendPushData(priv.PubKey().Compressed()),
		lock.AppendOpcodes(script.OpCHECKSIG),
		lock.AppendPushData(data),
		lock.AppendPushData(make([]byte, 71)),
		lock.AppendOpcodes(script.Op2DROP),
	} {
		if err != nil {
			tb.Fatal(err)
		}
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(src, 0, unlock)
	// A committed-record carrier is non-final by design.
	tx.Inputs[0].SequenceNumber = 0
	tx.LockTime = 4102444800
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 99_000, LockingScript: lock})
	if err := tx.Sign(); err != nil {
		tb.Fatal(err)
	}
	txid := *tx.TxID()

	b := binary.LittleEndian.AppendUint32(nil, transaction.ATOMIC_BEEF)
	b = append(b, txid[:]...)
	b = binary.LittleEndian.AppendUint32(b, transaction.BEEF_V2)
	b = append(b, 1) // one BUMP
	b = append(b, mp.Bytes()...)
	b = append(b, 2) // the funding transaction, proven by BUMP 0, then the carrier
	b = append(b, byte(transaction.RawTxAndBumpIndex), 0)
	b = src.AppendBytes(b)
	b = append(b, byte(transaction.RawTx))
	b = tx.AppendBytes(b)
	return &Piece{
		BEEF:    b,
		Tracker: &Roots{M: map[uint32]chainhash.Hash{100: *src.TxID()}, Tip: 200},
		TxID:    txid,
	}
}
