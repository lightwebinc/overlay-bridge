package headers

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

func v1Prefix() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, objfmt.BEEFMarkerV1)
	return b
}

// TestGuardRejectsTheThirteenByteKiller is the case that ended a process with
// a fatal out-of-memory error seven times in one flood: a tiny object naming a
// leaf count no allocation could satisfy.
func TestGuardRejectsTheThirteenByteKiller(t *testing.T) {
	obj := v1Prefix()
	obj = append(obj, 0x01) // one BUMP
	obj = append(obj, 0x00) // block height 0
	obj = append(obj, 0x01) // tree height 1
	obj = append(obj, 0xFF) // VarInt 0xFF: an 8-byte leaf count follows
	obj = append(obj, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	if len(obj) > 16 {
		t.Fatalf("test object grew to %d bytes; the point is that it is tiny", len(obj))
	}
	err := GuardBUMPs(obj)
	if !errors.Is(err, ErrUnsatisfiableLength) {
		t.Fatalf("err = %v, want ErrUnsatisfiableLength", err)
	}
}

func TestGuardRejectsUnsatisfiableBUMPCount(t *testing.T) {
	obj := append(v1Prefix(), 0xFE, 0xFF, 0xFF, 0xFF, 0xFF) // ~4 billion BUMPs
	if err := GuardBUMPs(obj); !errors.Is(err, ErrUnsatisfiableLength) {
		t.Fatalf("err = %v, want ErrUnsatisfiableLength", err)
	}
}

func TestGuardRejectsAbsurdTreeHeight(t *testing.T) {
	obj := append(v1Prefix(), 0x01, 0x00, 0xFF) // one BUMP, height 0, tree height 255
	if err := GuardBUMPs(obj); !errors.Is(err, ErrUnsatisfiableLength) {
		t.Fatalf("err = %v, want ErrUnsatisfiableLength", err)
	}
}

func TestGuardRejectsTruncation(t *testing.T) {
	// Truncation is a different finding from an unsatisfiable length, and the
	// two must not be conflated: this object's declared counts all fit, and it
	// then simply stops in the middle of a leaf hash.
	obj := v1Prefix()
	obj = append(obj, 0x01, 0x64, 0x01, 0x01, 0x00, 0x00) // one leaf, hash follows
	obj = append(obj, make([]byte, 10)...)                // ...but only 10 of its 32 bytes
	if err := GuardBUMPs(obj); !errors.Is(err, ErrTruncated) {
		t.Fatalf("err = %v, want ErrTruncated", err)
	}

	// An object that stops immediately after declaring a BUMP is the other
	// finding: nothing remains, so the count cannot possibly be satisfied.
	if err := GuardBUMPs(append(v1Prefix(), 0x01)); !errors.Is(err, ErrUnsatisfiableLength) {
		t.Fatalf("err = %v, want ErrUnsatisfiableLength", err)
	}
}

// TestGuardAcceptsAWellFormedBUMP is the positive control. A guard that
// refused everything would pass every rejection test above and be useless.
func TestGuardAcceptsAWellFormedBUMP(t *testing.T) {
	obj := v1Prefix()
	obj = append(obj, 0x01) // one BUMP
	obj = append(obj, 0x64) // block height 100
	obj = append(obj, 0x01) // tree height 1
	obj = append(obj, 0x01) // one leaf at level 0
	obj = append(obj, 0x00) // offset 0
	obj = append(obj, 0x00) // flags: not a duplicate, a hash follows
	obj = append(obj, make([]byte, 32)...)
	obj = append(obj, 0x00) // no transactions follow

	if err := GuardBUMPs(obj); err != nil {
		t.Fatalf("well-formed BUMP rejected: %v", err)
	}
}

// TestGuardAcceptsADuplicateLeaf pins the flag branch: a duplicate carries no
// hash, so a guard that always demanded 32 bytes would reject valid objects.
func TestGuardAcceptsADuplicateLeaf(t *testing.T) {
	obj := v1Prefix()
	obj = append(obj, 0x01, 0x64, 0x01, 0x01, 0x00, 0x01) // flags bit 0 set
	obj = append(obj, 0x00)
	if err := GuardBUMPs(obj); err != nil {
		t.Fatalf("duplicate leaf rejected: %v", err)
	}
}

// TestGuardIgnoresWhatItCannotClassify pins that this is a bound, not a
// parser: anything it does not recognise passes to the real parser untouched.
func TestGuardIgnoresWhatItCannotClassify(t *testing.T) {
	for _, obj := range [][]byte{nil, {0x01}, {0xDE, 0xAD, 0xBE, 0xEF}, []byte("not an object")} {
		if err := GuardBUMPs(obj); err != nil {
			t.Fatalf("unclassifiable object %x was rejected: %v", obj, err)
		}
	}
}

func TestGuardHandlesAtomicPrefix(t *testing.T) {
	obj := make([]byte, 4)
	binary.LittleEndian.PutUint32(obj, objfmt.AtomicBEEFMarker)
	obj = append(obj, make([]byte, 32)...) // subject txid
	obj = append(obj, v1Prefix()...)       // embedded BEEF version word
	obj = append(obj, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
	if err := GuardBUMPs(obj); !errors.Is(err, ErrUnsatisfiableLength) {
		t.Fatalf("err = %v, want the bound applied inside an atomic object", err)
	}
}
