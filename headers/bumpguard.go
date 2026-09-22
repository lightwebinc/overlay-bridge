package headers

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/lightwebinc/shard-common/objfmt"
)

// GuardBUMPs bounds every declared length in an object's BUMP section against
// the bytes actually present, allocating nothing.
//
// # Why this exists
//
// A BEEF reader that sizes a merkle path straight from the wire can be made to
// demand an arbitrary allocation by a very small object: the declared leaf
// count is a claim about bytes, and nothing checks that claim against the
// bytes that remain. That failure is NOT catchable in the usual way. A count
// large enough to be representable but too large to satisfy ends the process
// with a fatal out-of-memory error that no recover sees.
//
// This lane is reachable by anyone who can put an object on the plane, so a
// verifying consumer is otherwise killable on demand for the cost of a few
// bytes. Call this before handing bytes to a parser. After it returns nil,
// every count the parser is about to allocate against has been bounded by the
// object's own length.
//
// # What this is not
//
// It is not a second BEEF parser and it makes no validity claim: it returns
// nil for anything it cannot classify as a BUMP-carrying encoding, and the
// real parser remains the authority on whether an object is valid. It is also
// not made redundant by pinning a fixed parser version. A pin fixes the known
// case; the rule it enforces is the general one, for any length-prefixed field
// read from an untrusted source. Bound first, then parse, and keep the recover
// as the third layer rather than the first.
func GuardBUMPs(obj []byte) error {
	c := &cursor{b: obj}
	word, ok := objfmt.BEEFVersionWord(obj)
	if !ok {
		return nil
	}
	switch word {
	case objfmt.AtomicBEEFMarker:
		// 4-byte marker, 32-byte subject txid, then an ordinary BEEF whose own
		// version word follows.
		if err := c.skip(4 + 32 + 4); err != nil {
			return err
		}
	case objfmt.BEEFMarkerV1, objfmt.BEEFMarkerV2:
		if err := c.skip(4); err != nil {
			return err
		}
	default:
		return nil
	}

	nBUMPs, err := c.varInt()
	if err != nil {
		return err
	}
	// A BUMP is at least a block height, a tree height and one level.
	if !c.fits(nBUMPs, 3) {
		return fmt.Errorf("%w: declares %d BUMPs, %d bytes remain", ErrUnsatisfiableLength, nBUMPs, c.remaining())
	}
	for i := uint64(0); i < nBUMPs; i++ {
		if _, err := c.varInt(); err != nil { // block height
			return err
		}
		treeHeight, err := c.byteAt()
		if err != nil {
			return err
		}
		if treeHeight > maxTreeHeight {
			return fmt.Errorf("%w: BUMP %d declares tree height %d, max %d", ErrUnsatisfiableLength, i, treeHeight, maxTreeHeight)
		}
		for lv := 0; lv < int(treeHeight); lv++ {
			nLeaves, err := c.varInt()
			if err != nil {
				return err
			}
			if !c.fits(nLeaves, minLeafBytes) {
				return fmt.Errorf("%w: BUMP %d level %d declares %d leaves, %d bytes remain",
					ErrUnsatisfiableLength, i, lv, nLeaves, c.remaining())
			}
			for lf := uint64(0); lf < nLeaves; lf++ {
				if _, err := c.varInt(); err != nil { // offset
					return err
				}
				flags, err := c.byteAt()
				if err != nil {
					return err
				}
				if flags&1 == 0 { // not a duplicate: a 32-byte hash follows
					if err := c.skip(32); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// maxTreeHeight bounds a merkle path's level count. A tree over the whole
// 64-bit offset space is 64 levels, and the wire field is a single byte, so
// this rejects only the absurd.
const maxTreeHeight = 64

// minLeafBytes is the smallest encoding of one path element: a 1-byte offset
// VarInt plus 1 byte of flags. A non-duplicate leaf also carries a 32-byte
// hash, so this is a floor, which is what makes the bound safe.
const minLeafBytes = 2

// ErrTruncated reports an object that ends mid-structure.
var ErrTruncated = errors.New("headers: object ends mid-structure")

// ErrUnsatisfiableLength reports a declared length the remaining bytes could
// not possibly encode: the signature of an allocation attack.
var ErrUnsatisfiableLength = errors.New("headers: declared length exceeds the bytes present")

type cursor struct {
	b   []byte
	pos int
}

func (c *cursor) remaining() int { return len(c.b) - c.pos }

func (c *cursor) skip(n int) error {
	if n < 0 || c.remaining() < n {
		return ErrTruncated
	}
	c.pos += n
	return nil
}

func (c *cursor) byteAt() (byte, error) {
	if c.remaining() < 1 {
		return 0, ErrTruncated
	}
	v := c.b[c.pos]
	c.pos++
	return v, nil
}

// fits reports whether n items of at least each bytes could be encoded in what
// remains. It is the single check this file exists to make.
func (c *cursor) fits(n uint64, each int) bool {
	if each <= 0 {
		return false
	}
	return n <= uint64(c.remaining()/each)
}

// varInt reads a Bitcoin VarInt. The wide forms are the whole point: they are
// how a handful of bytes names a number no allocation could satisfy.
func (c *cursor) varInt() (uint64, error) {
	p, err := c.byteAt()
	if err != nil {
		return 0, err
	}
	switch p {
	case 0xFD:
		if c.remaining() < 2 {
			return 0, ErrTruncated
		}
		v := uint64(binary.LittleEndian.Uint16(c.b[c.pos:]))
		c.pos += 2
		return v, nil
	case 0xFE:
		if c.remaining() < 4 {
			return 0, ErrTruncated
		}
		v := uint64(binary.LittleEndian.Uint32(c.b[c.pos:]))
		c.pos += 4
		return v, nil
	case 0xFF:
		if c.remaining() < 8 {
			return 0, ErrTruncated
		}
		v := binary.LittleEndian.Uint64(c.b[c.pos:])
		c.pos += 8
		return v, nil
	default:
		return uint64(p), nil
	}
}
