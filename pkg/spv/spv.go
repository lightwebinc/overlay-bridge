// Package spv verifies a BEEF object by SPV before an overlay engine admits
// it: every merkle path the object carries against a chain tracker, and the
// scripts, amounts and structure of every transaction in it that carries no
// path.
//
// It is the bridge's half of moving verification out of the TypeScript engine,
// and it is written for a second caller: a Go overlay engine. Nothing here
// knows about lanes, topics, HTTP or the bridge's header store; a caller hands
// it bytes and a chaintracker.ChainTracker and gets a Verdict back.
//
// # What is verified, and why every transaction
//
// An engine verifies the transaction it picks as the object's subject (the
// Atomic BEEF subject, or the last transaction of a plain BEEF) together with
// that transaction's unproven ancestors. This package verifies every
// transaction in the BEEF that no other transaction in it spends (the tips),
// and with them every unproven ancestor, which is every unproven transaction
// in the object. It therefore covers whichever transaction an engine picks as
// the subject, so an engine told "this object passed" can skip script
// verification for any transaction in it without depending on the two parsers
// agreeing on which one the subject is.
//
// The checks are go-sdk v1.7.2 spv.Verify's, run here from go-sdk's
// primitives (merkle path, script interpreter, activation heights) so that
// finality is a policy (FinalityPolicy, see check.go): merkle paths first, then for each
// unproven transaction bitcoin-sv's own rules (inputs and outputs present, no
// duplicate input, amounts in range, outputs not exceeding inputs, finality,
// coinbase maturity, scripts under the network's activation heights). That is
// STRICTER than the TypeScript SDK's verify, which checks paths, amounts and
// scripts only; a disagreement between the two is what shadow mode counts.
//
// # Three results, and only one of them is a refusal
//
//   - ResultOK: every path proved and every unproven transaction passed.
//   - ResultRefused: the object is invalid on its merits: it does not parse,
//     a merkle path proves a root the chain does not commit to, or a
//     transaction fails a script or structural check.
//   - ResultError: verification could not reach a verdict: the chain tracker
//     failed, a path names a height above the tracker's tip (the header has
//     not arrived yet), or the deadline ran out. A caller must not read this
//     as invalid. The bridge submits such an object WITHOUT its trust marker,
//     so the engine verifies it in full, exactly as before.
package spv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script/interpreter/scriptflag"
	sdkspv "github.com/bsv-blockchain/go-sdk/spv"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// Result is the outcome of one verification. Its values are metric labels.
type Result string

// The three results. See the package comment.
const (
	ResultOK      Result = "ok"
	ResultRefused Result = "refused"
	ResultError   Result = "error"
)

// Reason says why an object was refused or errored. Its values are bounded
// so they can be logged and labelled; the full error is in Verdict.Err.
type Reason string

// Reasons. ReasonNone accompanies ResultOK.
const (
	ReasonNone      Reason = ""
	ReasonParse     Reason = "parse"     // not a BEEF this package can read
	ReasonMerkle    Reason = "merkle"    // a path proves a root the chain does not commit to
	ReasonScript    Reason = "script"    // a script failed
	ReasonStructure Reason = "structure" // amounts, inputs, finality, maturity, a missing source
	ReasonTracker   Reason = "tracker"   // the chain tracker failed
	ReasonNotYet    Reason = "not_yet"   // a path names a height above the tracker's tip
	ReasonDeadline  Reason = "deadline"  // the context ended first
)

// Verdict is what Verify returns. It never carries a nil Result.
type Verdict struct {
	Result Result
	Reason Reason
	// TxID is the transaction the verdict is about: the first that failed,
	// or, on success, the last tip verified. Zero when the BEEF did not parse.
	TxID chainhash.Hash
	// Transactions is the number of transactions in the object; Scripts is
	// the number of input scripts run (inputs of unproven transactions).
	Transactions, Scripts int
	// Work counters (R2), set whenever the object parsed, whatever the
	// result. Inputs is every input of every transaction in the object.
	// Proofs is the number of transactions carrying a merkle path. Depth is
	// the object's ancestry depth: the longest chain of in-object parents
	// below any transaction, so a child of a proven parent is 1, and a
	// lone transaction is 0.
	Inputs, Proofs, Depth int
	Duration              time.Duration
	Err                   error
}

// Network selects the activation heights scripts are judged under.
type Network string

// Networks. NetworkAll makes every rule active from the first block, which is
// what a regtest-style chain (the lab's) needs, and is the closest match to
// the TypeScript SDK, which has no notion of eras at all.
const (
	NetworkMain Network = "main"
	NetworkTest Network = "test"
	NetworkAll  Network = "all"
)

// ParseNetwork reads a network name; empty is NetworkMain.
func ParseNetwork(s string) (Network, error) {
	switch Network(s) {
	case "":
		return NetworkMain, nil
	case NetworkMain, NetworkTest, NetworkAll:
		return Network(s), nil
	}
	return "", fmt.Errorf("spv: network %q is not main, test or all", s)
}

func (n Network) heights() scriptflag.ActivationHeights {
	switch n {
	case NetworkTest:
		return scriptflag.TestNetActivationHeights
	case NetworkAll:
		return scriptflag.ActivationHeights{}
	default:
		return scriptflag.MainNetActivationHeights
	}
}

// Verifier verifies BEEF objects. It is safe for concurrent use; it holds no
// state between calls.
type Verifier struct {
	// Tracker answers merkle roots and the chain tip. Required.
	Tracker chaintracker.ChainTracker
	// Network selects activation heights. Empty means NetworkMain.
	Network Network
	// Finality is the finality policy. Empty means FinalityIgnore, the
	// overlay plane's policy (see FinalityPolicy).
	Finality FinalityPolicy
	// Timeout bounds one verification. Zero means 5 s. A script that runs
	// past it ends the verification with ResultError, never ResultOK.
	Timeout time.Duration
}

// Verify verifies one BEEF object (BRC-62 V1, BRC-96 V2 or BRC-95 Atomic).
func (v *Verifier) Verify(ctx context.Context, beef []byte) Verdict {
	start := time.Now()
	vd := v.verify(ctx, beef)
	vd.Duration = time.Since(start)
	return vd
}

func (v *Verifier) verify(ctx context.Context, raw []byte) Verdict {
	if v.Tracker == nil {
		return Verdict{Result: ResultError, Reason: ReasonTracker, Err: errors.New("spv: no chain tracker")}
	}
	timeout := v.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	beef, err := parse(raw)
	if err != nil {
		return Verdict{Result: ResultRefused, Reason: ReasonParse, Err: err}
	}

	// Link every transaction to its in-object sources and proofs, and find
	// the tips: transactions no other transaction in the object spends.
	spent := make(map[chainhash.Hash]bool, len(beef.Transactions))
	var txids []chainhash.Hash
	for txid, btx := range beef.Transactions {
		if btx == nil || btx.Transaction == nil {
			continue
		}
		txids = append(txids, txid)
		for _, in := range btx.Transaction.Inputs {
			if in.SourceTXID != nil {
				spent[*in.SourceTXID] = true
			}
		}
	}
	vd := Verdict{Transactions: len(txids)}
	vd.Inputs, vd.Proofs, vd.Depth = work(beef)
	if len(txids) == 0 {
		vd.Result, vd.Reason, vd.Err = ResultRefused, ReasonParse, errors.New("spv: BEEF holds no transaction")
		return vd
	}
	linked := make(map[chainhash.Hash]*transaction.Transaction, len(txids))
	for _, id := range txids {
		id := id
		linked[id] = beef.FindAtomicTransactionByHash(&id)
	}
	tracker := &watchTracker{inner: v.Tracker}
	fin := v.Finality
	if fin == "" {
		fin = FinalityIgnore
	}
	g := &graph{
		ctx: ctx, tracker: tracker, heights: v.Network.heights(), finality: fin,
		txids: map[*transaction.Transaction]chainhash.Hash{}, mined: map[chainhash.Hash]uint32{},
	}
	for _, id := range txids {
		if spent[id] {
			continue // verified as an ancestor of the tip that spends it
		}
		tx := linked[id]
		if tx == nil {
			continue
		}
		vd.TxID = id
		if err := g.check(tx); err != nil {
			vd.Result, vd.Reason, vd.Err = classify(ctx, tracker, err)
			vd.Scripts = g.scripts
			return vd
		}
	}
	vd.Result = ResultOK
	vd.Scripts = g.scripts
	return vd
}

// classify turns a failed verification into a result. Anything the tracker
// could not answer is an ERROR, not a refusal: the object may be valid.
func classify(ctx context.Context, t *watchTracker, err error) (Result, Reason, error) {
	failed, notYet := t.state()
	switch {
	case ctx.Err() != nil:
		return ResultError, ReasonDeadline, err
	case failed:
		return ResultError, ReasonTracker, err
	case notYet:
		return ResultError, ReasonNotYet, err
	case errors.Is(err, sdkspv.ErrScriptVerificationFailed):
		return ResultRefused, ReasonScript, err
	case errors.Is(err, sdkspv.ErrInvalidMerklePath):
		return ResultRefused, ReasonMerkle, err
	default:
		return ResultRefused, ReasonStructure, err
	}
}

// work counts an object's inputs and proofs and measures its ancestry depth
// over the in-object parent links (input source txids that are themselves in
// the object). It reads the parsed object only, so it costs no verification.
func work(beef *transaction.Beef) (inputs, proofs, depth int) {
	parents := make(map[chainhash.Hash][]chainhash.Hash, len(beef.Transactions))
	for txid, btx := range beef.Transactions {
		if btx == nil || btx.Transaction == nil {
			continue
		}
		tx := btx.Transaction
		inputs += len(tx.Inputs)
		if tx.MerklePath != nil {
			proofs++
		}
		for _, in := range tx.Inputs {
			if in.SourceTXID == nil {
				continue
			}
			if p, ok := beef.Transactions[*in.SourceTXID]; ok && p != nil && p.Transaction != nil {
				parents[txid] = append(parents[txid], *in.SourceTXID)
			}
		}
	}
	// Longest parent chain, memoised. Txids are hashes of the transactions
	// that name their parents, so the links cannot form a cycle; the
	// in-progress mark is defence against a parser that ever let one through.
	memo := make(map[chainhash.Hash]int, len(parents))
	const visiting = -1
	var walk func(chainhash.Hash) int
	walk = func(id chainhash.Hash) int {
		if d, ok := memo[id]; ok {
			if d == visiting {
				return 0
			}
			return d
		}
		memo[id] = visiting
		best := 0
		for _, p := range parents[id] {
			if d := walk(p) + 1; d > best {
				best = d
			}
		}
		memo[id] = best
		return best
	}
	for id := range parents {
		if d := walk(id); d > depth {
			depth = d
		}
	}
	return inputs, proofs, depth
}

// parse reads any BEEF version into one Beef.
func parse(raw []byte) (*transaction.Beef, error) {
	if len(raw) < 4 {
		return nil, errors.New("spv: object shorter than a BEEF version")
	}
	return transaction.NewBeefFromBytes(raw)
}

// watchTracker records whether the tracker failed, or was asked for a height
// above its tip, during one verification, so a refusal caused by the tracker
// is not mistaken for an invalid object.
type watchTracker struct {
	inner chaintracker.ChainTracker

	mu             sync.Mutex
	failed, notYet bool
}

func (w *watchTracker) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	ok, err := w.inner.IsValidRootForHeight(ctx, root, height)
	if err != nil {
		w.mu.Lock()
		w.failed = true
		w.mu.Unlock()
		return ok, err
	}
	if !ok {
		if tip, terr := w.inner.CurrentHeight(ctx); terr == nil && height > tip {
			w.mu.Lock()
			w.notYet = true
			w.mu.Unlock()
		}
	}
	return ok, nil
}

func (w *watchTracker) CurrentHeight(ctx context.Context) (uint32, error) {
	h, err := w.inner.CurrentHeight(ctx)
	if err != nil {
		w.mu.Lock()
		w.failed = true
		w.mu.Unlock()
	}
	return h, err
}

func (w *watchTracker) state() (failed, notYet bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed, w.notYet
}
