package spv

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// The object read in place.
//
// go-sdk's parse copies every script out of the object into an allocation of
// its own and then serializes each transaction again to hash it for its txid:
// for a 4 MiB storage piece that is the piece zeroed, copied, zeroed, copied
// and hashed, and the trust marker hashes the object a second time. Memory
// traffic, not instructions, is what bounds a bridge that carries large
// objects, so this file reads the common shape of an object where it lies:
//
//   - scripts are slices of the object's own bytes (nothing is copied);
//   - a transaction's txid is the double SHA-256 of its bytes as they stand
//     in the object, which equals go-sdk's hash of its re-serialization
//     because every length inside the transaction is required to be minimally
//     encoded here (re-serializing writes minimal lengths);
//   - every txid and, when a key is given, the trust marker are taken in ONE
//     traversal of the object, a block at a time, so each block is read from
//     memory once and hashed for every digest while it is in cache.
//
// The result is the *transaction.Beef that go-sdk's own parse returns for the
// same bytes (inplace_test.go compares them field by field and fuzzes the
// comparison), so everything after the parse is unchanged.
//
// readInPlace never refuses an object. Anything outside the shape it reads
// exactly as go-sdk does (BEEF V1, a txid-only entry, a transaction listed
// twice, an extended-format transaction, a non-minimal length, trailing
// bytes, any malformation) returns ok=false and the caller parses with
// go-sdk, which then accepts or refuses as it always has.
//
// The caller must leave raw unchanged while the returned Beef is in use.

// hashBlock is the traversal's step: small enough to sit in a core's cache
// while every digest that covers it reads it.
const hashBlock = 256 << 10

// Sizes go-sdk's parse guards counts with (transaction/parse_guard.go).
const (
	minBUMPBytes   = 2
	minEntryBytes  = 11
	minInputBytes  = 41
	minOutputBytes = 9
)

// cursor reads raw[at:] without copying.
type cursor struct {
	raw []byte
	at  int
}

func (c *cursor) left() int { return len(c.raw) - c.at }

func (c *cursor) take(n int) ([]byte, bool) {
	if n < 0 || n > c.left() {
		return nil, false
	}
	b := c.raw[c.at : c.at+n : c.at+n]
	c.at += n
	return b, true
}

func (c *cursor) u32() (uint32, bool) {
	b, ok := c.take(4)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint32(b), true
}

// varint reads a CompactSize length. minimal reports whether it was written
// in its shortest form.
func (c *cursor) varint() (v uint64, minimal, ok bool) {
	b, ok := c.take(1)
	if !ok {
		return 0, false, false
	}
	switch b[0] {
	case 0xff:
		if b, ok = c.take(8); !ok {
			return 0, false, false
		}
		v = binary.LittleEndian.Uint64(b)
		return v, v > 0xffffffff, true
	case 0xfe:
		if b, ok = c.take(4); !ok {
			return 0, false, false
		}
		v = uint64(binary.LittleEndian.Uint32(b))
		return v, v > 0xffff, true
	case 0xfd:
		if b, ok = c.take(2); !ok {
			return 0, false, false
		}
		v = uint64(binary.LittleEndian.Uint16(b))
		return v, v >= 0xfd, true
	default:
		return uint64(b[0]), true, true
	}
}

// entry is one transaction of the object: where its bytes lie and what the
// object says of its proof.
type entry struct {
	start, end int
	bump       int // index into the BUMPs, or -1
	tx         *transaction.Transaction
}

// readTx reads one transaction in standard format at the cursor, its scripts
// aliasing the object.
func readTx(c *cursor) (*transaction.Transaction, bool) {
	version, ok := c.u32()
	if !ok {
		return nil, false
	}
	nIn, minimal, ok := c.varint()
	// No inputs is how go-sdk recognizes an empty or an extended-format
	// transaction: both are left to it.
	if !ok || !minimal || nIn == 0 || nIn > uint64(c.left()/minInputBytes) {
		return nil, false
	}
	tx := &transaction.Transaction{Version: version, Inputs: make([]*transaction.TransactionInput, 0, nIn)}
	for i := uint64(0); i < nIn; i++ {
		prev, ok := c.take(32)
		if !ok {
			return nil, false
		}
		id, err := chainhash.NewHash(prev)
		if err != nil {
			return nil, false
		}
		vout, ok := c.u32()
		if !ok {
			return nil, false
		}
		n, minimal, ok := c.varint()
		if !ok || !minimal || n > uint64(c.left()) {
			return nil, false
		}
		unlock, _ := c.take(int(n))
		seq, ok := c.u32()
		if !ok {
			return nil, false
		}
		tx.Inputs = append(tx.Inputs, &transaction.TransactionInput{
			SourceTXID: id, SourceTxOutIndex: vout, UnlockingScript: script.NewFromBytes(unlock), SequenceNumber: seq,
		})
	}
	nOut, minimal, ok := c.varint()
	if !ok || !minimal || nOut > uint64(c.left()/minOutputBytes) {
		return nil, false
	}
	tx.Outputs = make([]*transaction.TransactionOutput, 0, nOut)
	for i := uint64(0); i < nOut; i++ {
		b, ok := c.take(8)
		if !ok {
			return nil, false
		}
		n, minimal, ok := c.varint()
		if !ok || !minimal || n > uint64(c.left()) {
			return nil, false
		}
		lock, _ := c.take(int(n))
		tx.Outputs = append(tx.Outputs, &transaction.TransactionOutput{
			Satoshis: binary.LittleEndian.Uint64(b), LockingScript: script.NewFromBytes(lock),
		})
	}
	if tx.LockTime, ok = c.u32(); !ok {
		return nil, false
	}
	return tx, true
}

// readInPlace reads a BEEF V2, bare or under an Atomic prefix. marker is the
// trust marker for raw under markerKey ("" without a key).
func readInPlace(raw, markerKey []byte) (beef *transaction.Beef, marker string, ok bool) {
	c := &cursor{raw: raw}
	var subject *chainhash.Hash
	if v, ok := c.u32(); !ok {
		return nil, "", false
	} else if v == transaction.ATOMIC_BEEF {
		b, ok := c.take(32)
		if !ok {
			return nil, "", false
		}
		var err error
		if subject, err = chainhash.NewHash(b); err != nil {
			return nil, "", false
		}
	} else {
		c.at = 0
	}
	if v, ok := c.u32(); !ok || v != transaction.BEEF_V2 {
		return nil, "", false
	}

	// The proofs, by go-sdk's own reader: they are small, and nothing about
	// them is re-derived here.
	nBUMPs, _, ok := c.varint()
	if !ok || nBUMPs > uint64(c.left()/minBUMPBytes) {
		return nil, "", false
	}
	bumps := make([]*transaction.MerklePath, nBUMPs)
	r := bytes.NewReader(raw[c.at:])
	for i := range bumps {
		mp, err := transaction.NewMerklePathFromReader(r)
		if err != nil || len(mp.Path) == 0 {
			return nil, "", false
		}
		bumps[i] = mp
	}
	c.at = len(raw) - r.Len()

	nTx, _, ok := c.varint()
	if !ok || nTx == 0 || nTx > uint64(c.left()/minEntryBytes) {
		return nil, "", false
	}
	entries := make([]entry, 0, nTx)
	for i := uint64(0); i < nTx; i++ {
		format, ok := c.take(1)
		if !ok {
			return nil, "", false
		}
		e := entry{bump: -1}
		switch transaction.DataFormat(format[0]) {
		case transaction.RawTx:
		case transaction.RawTxAndBumpIndex:
			idx, _, ok := c.varint()
			if !ok || idx >= nBUMPs {
				return nil, "", false
			}
			e.bump = int(idx)
		default: // a txid-only entry, or no format at all
			return nil, "", false
		}
		e.start = c.at
		if e.tx, ok = readTx(c); !ok {
			return nil, "", false
		}
		e.end = c.at
		entries = append(entries, e)
	}
	if c.left() != 0 {
		return nil, "", false
	}

	ids, marker := digests(raw, entries, markerKey)

	// The entries as go-sdk's parse links them: in order, a proof by its
	// index, an input to a transaction that came before it in the object.
	txs := make(map[chainhash.Hash]*transaction.BeefTx, len(entries))
	for i := range entries {
		e := &entries[i]
		if _, dup := txs[ids[i]]; dup {
			return nil, "", false
		}
		btx := &transaction.BeefTx{DataFormat: transaction.RawTx, Transaction: e.tx}
		if e.bump >= 0 {
			btx.DataFormat, btx.BumpIndex = transaction.RawTxAndBumpIndex, e.bump
			e.tx.MerklePath = bumps[e.bump]
		}
		for _, in := range e.tx.Inputs {
			if src, ok := txs[*in.SourceTXID]; ok {
				in.SourceTransaction = src.Transaction
			}
		}
		txs[ids[i]] = btx
	}
	newest := ids[len(ids)-1]
	if subject != nil {
		// BRC-95, as go-sdk's NewBeefFromAtomicBytes: the subject is in the
		// object, and so is nothing its inputs do not lead to.
		if _, ok := txs[*subject]; !ok {
			return nil, "", false
		}
		reached := make(map[chainhash.Hash]bool, len(txs))
		for stack := []chainhash.Hash{*subject}; len(stack) > 0; {
			id := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			btx, ok := txs[id]
			if !ok || reached[id] {
				continue
			}
			reached[id] = true
			for _, in := range btx.Transaction.Inputs {
				stack = append(stack, *in.SourceTXID)
			}
		}
		if len(reached) != len(txs) {
			return nil, "", false
		}
		newest = *subject
	}
	return &transaction.Beef{Version: transaction.BEEF_V2, BUMPs: bumps, Transactions: txs, NewestTxID: &newest}, marker, true
}

// digests takes every entry's txid, and the marker when a key is given, in
// one traversal of raw.
func digests(raw []byte, entries []entry, markerKey []byte) ([]chainhash.Hash, string) {
	var mac hash.Hash
	if len(markerKey) >= MinKeyBytes {
		mac = hmac.New(sha256.New, markerKey)
		mac.Write([]byte(markerLabel))
	}
	hs := make([]hash.Hash, len(entries))
	for i := range hs {
		hs[i] = sha256.New()
	}
	first := 0 // the first entry not yet wholly hashed
	for at := 0; at < len(raw); at += hashBlock {
		end := min(at+hashBlock, len(raw))
		if mac != nil {
			mac.Write(raw[at:end])
		}
		for i := first; i < len(entries) && entries[i].start < end; i++ {
			e := &entries[i]
			hs[i].Write(raw[max(at, e.start):min(end, e.end)])
			if e.end <= end {
				first = i + 1
			}
		}
	}
	ids := make([]chainhash.Hash, len(entries))
	var sum [sha256.Size]byte
	for i, h := range hs {
		ids[i] = sha256.Sum256(h.Sum(sum[:0]))
	}
	marker := ""
	if mac != nil {
		marker = "v1=" + hex.EncodeToString(mac.Sum(nil))
	}
	return ids, marker
}
