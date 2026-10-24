package spv

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/script/interpreter/scriptflag"
	sdkspv "github.com/bsv-blockchain/go-sdk/spv"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// FinalityPolicy says whether an unmined transaction must be final.
//

// The policies.
//
// FinalityIgnore is the overlay plane's policy and the default. Committed-
// record carriers are non-final BY DESIGN: nLockTime 4102444800 (2100-01-01)
// with a non-final input, so that they can never be mined (bbox docs/spec.md,
// the committed-record ruling of 2026-09-24). An overlay admits them when
// their scripts, proofs and amounts are valid, which is what the TypeScript
// engine does. go-sdk v1.7.2's spv.Verify applies bitcoin-sv's finality rule
// with no way to switch it off alone, so this package runs its checks itself
// from go-sdk primitives (owner ruling 2026-10-21, "own policy on v1.7.2").
//
// FinalityRequire applies bitcoin-sv's rule as v1.7.2 does: a transaction
// with a non-zero nLockTime and a non-final input must have a height-based
// lock at or below tip+1; a time-based lock is refused, since the tracker
// reports no median time past.
type FinalityPolicy string

const (
	FinalityIgnore  FinalityPolicy = "ignore"
	FinalityRequire FinalityPolicy = "require"
)

// ParseFinality reads a policy name; empty is FinalityIgnore.
func ParseFinality(s string) (FinalityPolicy, error) {
	switch FinalityPolicy(s) {
	case "", FinalityIgnore:
		return FinalityIgnore, nil
	case FinalityRequire:
		return FinalityRequire, nil
	}
	return "", fmt.Errorf("spv: finality %q is not ignore or require", s)
}

// Consensus constants, as go-sdk v1.7.2 spv (bitcoin-sv amount.h,
// consensus.h, script.h).
const (
	maxSatoshis          uint64 = 21_000_000 * 100_000_000
	coinbaseMaturity            = 100
	confiscationMaturity        = 1000
	lockTimeThreshold           = 500_000_000
	unminedHeight               = math.MaxUint32
)

var confiscationMarker = []byte{0x00, 0x6a, 0x04, 'c', 'f', 't', 'x'}

// graph is one verification's state: the txid and proven height of every
// transaction reached. It mirrors go-sdk v1.7.2 spv's txGraph, minus the
// finality gate when the policy ignores it.
type graph struct {
	ctx      context.Context //nolint:containedctx // one per verification
	tracker  chaintracker.ChainTracker
	heights  scriptflag.ActivationHeights
	finality FinalityPolicy
	// skipScripts leaves the interpreter out (Verifier.SkipScripts).
	skipScripts bool

	txids   map[*transaction.Transaction]chainhash.Hash
	mined   map[chainhash.Hash]uint32
	tip     *uint32
	scripts int
}

func (g *graph) txid(tx *transaction.Transaction) chainhash.Hash {
	if id, ok := g.txids[tx]; ok {
		return id
	}
	id := *tx.TxID()
	g.txids[tx] = id
	return id
}

func (g *graph) chainTip() (uint32, error) {
	if g.tip != nil {
		return *g.tip, nil
	}
	h, err := g.tracker.CurrentHeight(g.ctx)
	if err != nil {
		return 0, err
	}
	g.tip = &h
	return h, nil
}

// check verifies t and its unmined ancestors: first every merkle path (so no
// height is used unproven), then every unmined transaction.
func (g *graph) check(t *transaction.Transaction) error {
	if err := g.proveMined(t); err != nil {
		return err
	}
	done := map[*transaction.Transaction]bool{}
	for queue := []*transaction.Transaction{t}; len(queue) > 0; queue = queue[1:] {
		tx := queue[0]
		if done[tx] {
			continue
		}
		done[tx] = true
		if _, ok := g.mined[g.txid(tx)]; ok {
			continue
		}
		if err := g.checkTx(tx); err != nil {
			return err
		}
		for _, in := range tx.Inputs {
			if in.SourceTransaction != nil && !done[in.SourceTransaction] {
				queue = append(queue, in.SourceTransaction)
			}
		}
	}
	return nil
}

func (g *graph) proveMined(t *transaction.Transaction) error {
	seen := map[*transaction.Transaction]bool{t: true}
	for queue := []*transaction.Transaction{t}; len(queue) > 0; queue = queue[1:] {
		tx := queue[0]
		txid := g.txid(tx)
		if tx.MerklePath != nil {
			ok, err := tx.MerklePath.Verify(g.ctx, &txid, g.tracker)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w for transaction %s", sdkspv.ErrInvalidMerklePath, txid)
			}
			if h, dup := g.mined[txid]; dup && h != tx.MerklePath.BlockHeight {
				return fmt.Errorf("%w: %s proven at heights %d and %d", sdkspv.ErrInvalidMerklePath, txid, h, tx.MerklePath.BlockHeight)
			}
			g.mined[txid] = tx.MerklePath.BlockHeight
			continue
		}
		for vin, in := range tx.Inputs {
			src := in.SourceTransaction
			if src == nil {
				continue
			}
			if sid := g.txid(src); in.SourceTXID == nil || !sid.IsEqual(in.SourceTXID) {
				return fmt.Errorf("%w: input %d of %s", sdkspv.ErrSourceTransactionMismatch, vin, txid)
			}
			if !seen[src] {
				seen[src] = true
				queue = append(queue, src)
			}
		}
	}
	return nil
}

// checkTx is go-sdk v1.7.2 spv's verifyTx, with finality under policy.
func (g *graph) checkTx(tx *transaction.Transaction) error {
	txid := g.txid(tx)
	if isCoinbase(tx) {
		return fmt.Errorf("%w: %s", sdkspv.ErrUnminedCoinbase, txid)
	}
	if len(tx.Inputs) == 0 {
		return fmt.Errorf("%w: %s", sdkspv.ErrNoInputs, txid)
	}
	if len(tx.Outputs) == 0 {
		return fmt.Errorf("%w: %s", sdkspv.ErrNoOutputs, txid)
	}
	var outTotal uint64
	for vout, o := range tx.Outputs {
		var ok bool
		if outTotal, ok = addSats(outTotal, o.Satoshis); !ok {
			return fmt.Errorf("%w: output %d of %s", sdkspv.ErrValueOutOfRange, vout, txid)
		}
	}
	// The next block is always after Genesis, so it refuses a P2SH output.
	{
		for vout, o := range tx.Outputs {
			if o.LockingScript != nil && o.LockingScript.IsP2SH() {
				return fmt.Errorf("%w: output %d of %s", sdkspv.ErrP2SHOutput, vout, txid)
			}
		}
	}
	type outpoint struct {
		id   chainhash.Hash
		vout uint32
	}
	spent := make(map[outpoint]bool, len(tx.Inputs))
	for vin, in := range tx.Inputs {
		if in.SourceTXID == nil {
			return fmt.Errorf("%w: input %d of %s", sdkspv.ErrMissingSourceTransaction, vin, txid)
		}
		if isNullOutpoint(in) {
			return fmt.Errorf("%w: input %d of %s", sdkspv.ErrNullOutpoint, vin, txid)
		}
		op := outpoint{*in.SourceTXID, in.SourceTxOutIndex}
		if spent[op] {
			return fmt.Errorf("%w: input %d of %s", sdkspv.ErrDuplicateInput, vin, txid)
		}
		spent[op] = true
	}
	if g.finality == FinalityRequire {
		if err := g.final(tx, txid); err != nil {
			return err
		}
	}
	if isConfiscationTx(tx) {
		return fmt.Errorf("%w: %s", sdkspv.ErrConfiscationTransaction, txid)
	}
	prevouts := make([]*transaction.TransactionOutput, len(tx.Inputs))
	var inTotal uint64
	for vin, in := range tx.Inputs {
		p := spentOutput(in)
		if p == nil {
			return fmt.Errorf("%w: input %d of %s", sdkspv.ErrMissingSourceTransaction, vin, txid)
		}
		prevouts[vin] = p
		if src := in.SourceTransaction; src != nil && isCoinbase(src) {
			ok, err := g.deepEnough(in, coinbaseMaturity)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: input %d of %s", sdkspv.ErrPrematureCoinbaseSpend, vin, txid)
			}
		}
		if src := in.SourceTransaction; src != nil && isConfiscationTx(src) {
			ok, err := g.deepEnough(in, confiscationMaturity)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: input %d of %s", sdkspv.ErrPrematureConfiscationSpend, vin, txid)
			}
		}
		var ok bool
		if inTotal, ok = addSats(inTotal, p.Satoshis); !ok {
			return fmt.Errorf("%w: input %d of %s", sdkspv.ErrValueOutOfRange, vin, txid)
		}
	}
	if outTotal > inTotal {
		return fmt.Errorf("%w: %s pays %d and spends %d", sdkspv.ErrOutputsExceedInputs, txid, outTotal, inTotal)
	}
	if g.skipScripts {
		return nil
	}
	for vin, in := range tx.Inputs {
		coin := uint32(unminedHeight)
		if h, ok := g.mined[*in.SourceTXID]; ok {
			coin = h
		}
		coin = g.p2shCoinHeight(in, prevouts[vin], coin)
		g.scripts++
		if err := interpreter.NewEngine().Execute(
			interpreter.WithTx(tx, vin, prevouts[vin]),
			interpreter.WithFlags(g.heights.BlockValidationFlags(coin, unminedHeight)),
			interpreter.WithContext(g.ctx),
		); err != nil {
			return fmt.Errorf("%w: %w", sdkspv.ErrScriptVerificationFailed, err)
		}
	}
	return nil
}

// final is bitcoin-sv's IsFinalTx for the next block, as go-sdk v1.7.2.
func (g *graph) final(tx *transaction.Transaction, txid chainhash.Hash) error {
	if tx.LockTime == 0 {
		return nil
	}
	nonFinal := false
	for _, in := range tx.Inputs {
		if in.SequenceNumber != transaction.DefaultSequenceNumber {
			nonFinal = true
			break
		}
	}
	if !nonFinal {
		return nil
	}
	if tx.LockTime >= lockTimeThreshold {
		return fmt.Errorf("%w: %s: nLockTime %d is time-based and the tracker reports no median time past", sdkspv.ErrNonFinalTransaction, txid, tx.LockTime)
	}
	tip, err := g.chainTip()
	if err != nil {
		return err
	}
	if int64(tx.LockTime) <= int64(tip) {
		return nil
	}
	return fmt.Errorf("%w: %s: nLockTime %d above tip+1 (tip %d)", sdkspv.ErrNonFinalTransaction, txid, tx.LockTime, tip)
}

func (g *graph) deepEnough(in *transaction.TransactionInput, maturity int64) (bool, error) {
	h, ok := g.mined[*in.SourceTXID]
	if !ok {
		return false, nil
	}
	tip, err := g.chainTip()
	if err != nil {
		return false, err
	}
	return int64(tip)+1-int64(h) >= maturity, nil
}

// p2shCoinHeight: a P2SH-shaped output that is not a proven-mined
// coinbase's can only be pre-Genesis (go-sdk v1.7.2 spv).
func (g *graph) p2shCoinHeight(in *transaction.TransactionInput, prev *transaction.TransactionOutput, coin uint32) uint32 {
	if g.heights.Genesis == 0 || prev.LockingScript == nil || !prev.LockingScript.IsP2SH() {
		return coin
	}
	if src := in.SourceTransaction; src != nil && isCoinbase(src) {
		if _, ok := g.mined[*in.SourceTXID]; ok {
			return coin
		}
	}
	if coin >= g.heights.Genesis {
		return g.heights.Genesis - 1
	}
	return coin
}

func isNullOutpoint(in *transaction.TransactionInput) bool {
	return in.SourceTXID != nil && *in.SourceTXID == (chainhash.Hash{}) && in.SourceTxOutIndex == 0xffffffff
}

func isCoinbase(tx *transaction.Transaction) bool {
	return len(tx.Inputs) == 1 && isNullOutpoint(tx.Inputs[0])
}

func isConfiscationTx(tx *transaction.Transaction) bool {
	if len(tx.Outputs) == 0 || tx.Outputs[0] == nil || tx.Outputs[0].LockingScript == nil {
		return false
	}
	return bytes.HasPrefix(*tx.Outputs[0].LockingScript, confiscationMarker)
}

func spentOutput(in *transaction.TransactionInput) *transaction.TransactionOutput {
	src := in.SourceTransaction
	if src == nil {
		return in.SourceTxOutput()
	}
	if int(in.SourceTxOutIndex) >= len(src.Outputs) {
		return nil
	}
	return src.Outputs[in.SourceTxOutIndex]
}

func addSats(total, v uint64) (uint64, bool) {
	if v > maxSatoshis || total > maxSatoshis-v {
		return total, false
	}
	return total + v, true
}
