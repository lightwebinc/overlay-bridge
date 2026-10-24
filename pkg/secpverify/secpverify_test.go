package secpverify

// Differential tests: go-sdk's built-in verifier against Verify, at the
// function level (ec.Signature.Verify with the hook and without, and the
// interpreter's branch over every key encoding) and at the script level
// (OP_CHECKSIG under every flag regime, hooks off and on). Counts scale with
// SECPVERIFY_N (default 300).

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"math/big"
	mrand "math/rand/v2"
	"os"
	"strconv"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	crypto "github.com/bsv-blockchain/go-sdk/primitives/hash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/script/interpreter/scriptflag"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	decdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

var (
	curve = ec.S256()
	N     = curve.N
	P     = curve.P
	halfN = new(big.Int).Rsh(N, 1)
)

func count(def int) int {
	if v, err := strconv.Atoi(os.Getenv("SECPVERIFY_N")); err == nil && v > 0 {
		return v
	}
	if raceOn {
		return def / 5
	}
	return def
}

func randScalar() *big.Int {
	for {
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		k := new(big.Int).SetBytes(b)
		if k.Sign() > 0 && k.Cmp(N) < 0 {
			return k
		}
	}
}

func modN(x *big.Int) *big.Int { return new(big.Int).Mod(x, N) }
func inv(x *big.Int) *big.Int  { return new(big.Int).ModInverse(x, N) }
func mul(a, b *big.Int) *big.Int {
	return modN(new(big.Int).Mul(a, b))
}

func pubOf(d *big.Int) *ec.PublicKey {
	x, y := curve.ScalarBaseMult(d.Bytes())
	return &ec.PublicKey{Curve: curve, X: x, Y: y}
}

func b32(x *big.Int) []byte { return x.FillBytes(make([]byte, 32)) }

// rawDER is the canonical DER of (r, s) as given (no low-S normalization).
func rawDER(r, s *big.Int) []byte {
	enc := func(v *big.Int) []byte {
		b := v.Bytes()
		if len(b) == 0 {
			b = []byte{0}
		}
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	rb, sb := enc(r), enc(s)
	out := []byte{0x30, byte(4 + len(rb) + len(sb)), 0x02, byte(len(rb))}
	out = append(out, rb...)
	out = append(out, 0x02, byte(len(sb)))
	return append(out, sb...)
}

func encodings(p *ec.PublicKey) [][]byte {
	return [][]byte{p.Compressed(), p.Uncompressed(), p.Hybrid()}
}

type vcase struct {
	name string
	hash []byte
	r, s *big.Int
	pub  *ec.PublicKey
	encs [][]byte // key encodings; nil = encodings(pub)
	want int      // 1 true, 0 false, -1 unknown
}

var stats = map[string][2]int{}

// check compares go-sdk's built-in verifier with the decred hooks on one case.
func check(t *testing.T, c vcase) {
	t.Helper()
	sig := &ec.Signature{R: c.r, S: c.s}
	Uninstall()
	builtin := sig.Verify(c.hash, c.pub)
	ec.InjectExternalVerifySignatureFn(Verify)
	hooked := sig.Verify(c.hash, c.pub)
	Uninstall()
	if builtin != hooked {
		t.Fatalf("%s: Signature.Verify builtin %v hooked %v (hash %x r %x s %x pub %x)", c.name, builtin, hooked, c.hash, c.r, c.s, c.pub.Compressed())
	}
	if c.want >= 0 && builtin != (c.want == 1) {
		t.Fatalf("%s: builtin %v want %v", c.name, builtin, c.want == 1)
	}
	encs := c.encs
	if encs == nil {
		encs = encodings(c.pub)
	}
	der := rawDER(c.r, c.s)
	for _, enc := range encs {
		// The interpreter's built-in branch: ec.ParsePubKey, then Verify.
		want := false
		if k, err := ec.ParsePubKey(enc); err == nil {
			want = sig.Verify(c.hash, k)
		}
		got := Verify(c.hash, der, enc)
		if got != want {
			t.Fatalf("%s: key %x: interpreter builtin %v fast %v (hash %x r %x s %x)", c.name, enc, want, got, c.hash, c.r, c.s)
		}
	}
	st := stats[c.name]
	if builtin {
		st[0]++
	} else {
		st[1]++
	}
	stats[c.name] = st
}

func signWith(t *testing.T, d *big.Int, hash []byte) (*big.Int, *big.Int) {
	priv, _ := ec.PrivateKeyFromBytes(b32(d))
	sig, err := priv.Sign(hash)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return sig.R, sig.S
}

func TestDifferentialRandom(t *testing.T) {
	n := count(300)
	for i := 0; i < n; i++ {
		d := randScalar()
		pub := pubOf(d)
		h := sha256.Sum256([]byte(fmt.Sprint("m", i)))
		r, s := signWith(t, d, h[:])
		check(t, vcase{name: "valid", hash: h[:], r: r, s: s, pub: pub, want: 1})
		check(t, vcase{name: "valid-highS", hash: h[:], r: r, s: new(big.Int).Sub(N, s), pub: pub, want: 1})
		hb := append([]byte(nil), h[:]...)
		hb[mrand.IntN(32)] ^= 1 << mrand.IntN(8)
		check(t, vcase{name: "hash-flip", hash: hb, r: r, s: s, pub: pub, want: 0})
		check(t, vcase{name: "other-key", hash: h[:], r: r, s: s, pub: pubOf(randScalar()), want: 0})
		check(t, vcase{name: "r+1", hash: h[:], r: modN(new(big.Int).Add(r, big.NewInt(1))), s: s, pub: pub, want: -1})
		check(t, vcase{name: "s+1", hash: h[:], r: r, s: modN(new(big.Int).Add(s, big.NewInt(1))), pub: pub, want: -1})
		check(t, vcase{name: "random-rs", hash: h[:], r: randScalar(), s: randScalar(), pub: pub, want: 0})
	}
}

func TestDifferentialEdgeScalars(t *testing.T) {
	d := randScalar()
	pub := pubOf(d)
	h := sha256.Sum256([]byte("edge"))
	r0, s0 := signWith(t, d, h[:])
	one := big.NewInt(1)
	vals := []*big.Int{big.NewInt(0), one, big.NewInt(2), new(big.Int).Sub(N, one), N, new(big.Int).Add(N, one),
		halfN, new(big.Int).Add(halfN, one), new(big.Int).Sub(P, one), new(big.Int).Sub(new(big.Int).Lsh(one, 256), one), r0, s0}
	for _, r := range vals {
		for _, s := range vals {
			if r.Sign() == 0 || s.Sign() == 0 || r.BitLen() > 256 || s.BitLen() > 256 {
				continue // not representable as a go-sdk DER signature; Verify refuses before either path
			}
			check(t, vcase{name: "edge-scalars", hash: h[:], r: r, s: s, pub: pub, want: -1})
		}
	}
}

func TestDifferentialHashes(t *testing.T) {
	one := big.NewInt(1)
	hashes := [][]byte{
		make([]byte, 32), b32(N), b32(new(big.Int).Add(N, one)), b32(new(big.Int).Sub(N, one)),
		bytes.Repeat([]byte{0xff}, 32), {}, {0x01}, bytes.Repeat([]byte{0xab}, 20),
		append([]byte{0}, bytes.Repeat([]byte{0xcd}, 32)...), bytes.Repeat([]byte{0x11}, 33), bytes.Repeat([]byte{0x22}, 64),
	}
	for i, h := range hashes {
		for j := 0; j < count(20); j++ {
			d := randScalar()
			pub := pubOf(d)
			priv, _ := ec.PrivateKeyFromBytes(b32(d))
			sig, err := priv.Sign(h)
			if err != nil {
				// too large to sign: random scalars, both paths must refuse
				check(t, vcase{name: fmt.Sprint("hash-unsignable-", i), hash: h, r: randScalar(), s: randScalar(), pub: pub, want: 0})
				continue
			}
			want := 1
			if len(h) == 0 {
				want = 0 // crypto/ecdsa refuses an empty hash, whatever the signature
			}
			check(t, vcase{name: fmt.Sprint("hash-", i), hash: h, r: sig.R, s: sig.S, pub: pub, want: want})
			check(t, vcase{name: fmt.Sprint("hash-", i, "-highS"), hash: h, r: sig.R, s: new(big.Int).Sub(N, sig.S), pub: pub, want: want})
		}
	}
}

// x-coordinate wrap: the verifying point's x is in [N, P), so r = x - N.
func TestDifferentialXWrap(t *testing.T) {
	found := 0
	for found < count(64) {
		off := randScalar()
		off.Rsh(off, 130) // below P - N (about 2^128.3)
		x := new(big.Int).Add(N, off)
		if x.Cmp(P) >= 0 {
			continue
		}
		// y^2 = x^3 + 7
		y2 := new(big.Int).Exp(x, big.NewInt(3), P)
		y2.Add(y2, big.NewInt(7)).Mod(y2, P)
		y := new(big.Int).ModSqrt(y2, P)
		if y == nil {
			continue
		}
		found++
		r := new(big.Int).Sub(x, N)
		s := randScalar()
		e := randScalar()
		// Q = r^-1 (s R - e G)
		sRx, sRy := curve.ScalarMult(x, y, b32(s))
		eGx, eGy := curve.ScalarBaseMult(b32(e))
		negEGy := new(big.Int).Sub(P, eGy)
		qx, qy := curve.Add(sRx, sRy, eGx, negEGy)
		qx, qy = curve.ScalarMult(qx, qy, b32(inv(r)))
		pub := &ec.PublicKey{Curve: curve, X: qx, Y: qy}
		check(t, vcase{name: "xwrap", hash: b32(e), r: r, s: s, pub: pub, want: 1})
		check(t, vcase{name: "xwrap-highS", hash: b32(e), r: r, s: new(big.Int).Sub(N, s), pub: pub, want: 1})
		check(t, vcase{name: "xwrap-unreduced-r", hash: b32(e), r: x, s: s, pub: pub, want: 0})
	}
}

// u1 = 0, an infinite verifying point, and u1*G == u2*Q (doubling).
func TestDifferentialSpecialPoints(t *testing.T) {
	for i := 0; i < count(64); i++ {
		// e = 0: u1 = 0.
		d := randScalar()
		r, s := signWith(t, d, make([]byte, 32))
		check(t, vcase{name: "e-zero", hash: make([]byte, 32), r: r, s: s, pub: pubOf(d), want: 1})

		// infinity: e = -r d.
		d = randScalar()
		r = randScalar()
		s = randScalar()
		e := modN(new(big.Int).Neg(mul(r, d)))
		check(t, vcase{name: "infinity", hash: b32(e), r: r, s: s, pub: pubOf(d), want: 0})

		// doubling: u1 G = u2 Q = (k/2) G.
		k := randScalar()
		rx, _ := curve.ScalarBaseMult(b32(k))
		r = modN(rx)
		if r.Sign() == 0 {
			continue
		}
		s = randScalar()
		u1 := mul(k, inv(big.NewInt(2)))
		e = mul(u1, s)
		u2 := mul(r, inv(s))
		d = mul(u1, inv(u2))
		check(t, vcase{name: "doubling", hash: b32(e), r: r, s: s, pub: pubOf(d), want: 1})
		check(t, vcase{name: "doubling-highS", hash: b32(e), r: r, s: new(big.Int).Sub(N, s), pub: pubOf(d), want: 1})
	}
	// Q = G, -G, 2G, 3G.
	for _, dv := range []*big.Int{big.NewInt(1), new(big.Int).Sub(N, big.NewInt(1)), big.NewInt(2), big.NewInt(3)} {
		for i := 0; i < 16; i++ {
			h := sha256.Sum256([]byte(fmt.Sprint("small", dv, i)))
			r, s := signWith(t, dv, h[:])
			check(t, vcase{name: "small-d", hash: h[:], r: r, s: s, pub: pubOf(dv), want: 1})
		}
	}
}

// Malformed key encodings reach the interpreter's branch (the ec hook only
// ever sees compressed valid keys).
func TestDifferentialKeyEncodings(t *testing.T) {
	for i := 0; i < count(200); i++ {
		d := randScalar()
		pub := pubOf(d)
		h := sha256.Sum256([]byte(fmt.Sprint("k", i)))
		r, s := signWith(t, d, h[:])
		hyb := pub.Hybrid()
		hyb[0] ^= 1 // wrong parity
		unc := pub.Uncompressed()
		unc[64] ^= 1 // off the curve
		comp := pub.Compressed()
		comp[0] ^= 1 // the other y
		var bad [][]byte
		bad = append(bad, hyb, unc, comp)
		// x >= P: x' + P for a small x' on the curve (compressed and uncompressed).
		for xs := int64(1); xs < 50; xs++ {
			x := big.NewInt(xs)
			y2 := new(big.Int).Exp(x, big.NewInt(3), P)
			y2.Add(y2, big.NewInt(7)).Mod(y2, P)
			y := new(big.Int).ModSqrt(y2, P)
			if y == nil {
				continue
			}
			big := new(big.Int).Add(x, P)
			c := append([]byte{0x02 | byte(y.Bit(0))}, b32(big)...)
			u := append(append([]byte{0x04}, b32(big)...), b32(y)...)
			bad = append(bad, c, u)
			break
		}
		// y >= P on an uncompressed key.
		yb := new(big.Int).Add(pub.Y, P)
		if yb.BitLen() <= 256 {
			bad = append(bad, append(append([]byte{0x04}, b32(pub.X)...), b32(yb)...))
		}
		check(t, vcase{name: "bad-key-encodings", hash: h[:], r: r, s: s, pub: pub, encs: bad, want: -1})
		check(t, vcase{name: "good-key-encodings", hash: h[:], r: r, s: s, pub: pub, want: 1})
	}
}

// bip66 mirrors go-sdk's checkSignatureEncoding without the low-S rule.
func bip66(sig []byte) bool {
	n := len(sig)
	if n < 8 || n > 72 || sig[0] != 0x30 || int(sig[1]) != n-2 {
		return false
	}
	rLen := int(sig[3])
	sT := 4 + rLen
	if sT >= n || sT+1 >= n {
		return false
	}
	sLen := int(sig[sT+1])
	sO := sT + 2
	if sO+sLen != n || sig[2] != 0x02 || rLen == 0 || sig[4]&0x80 != 0 {
		return false
	}
	if rLen > 1 && sig[4] == 0 && sig[5]&0x80 == 0 {
		return false
	}
	if sig[sT] != 0x02 || sLen == 0 || sig[sO]&0x80 != 0 {
		return false
	}
	if sLen > 1 && sig[sO] == 0 && sig[sO+1]&0x80 == 0 {
		return false
	}
	return true
}

// Every BIP66-valid encoding parses the same under go-sdk's strict parser
// and decred's: both accept or both refuse, with the same (r, s).
func TestDERParseEquivalence(t *testing.T) {
	n := count(300) * 25
	seen := 0
	for i := 0; i < n; i++ {
		var r, s *big.Int
		switch i % 4 {
		case 0:
			r, s = randScalar(), randScalar()
		case 1:
			r, s = new(big.Int).Rsh(randScalar(), uint(mrand.IntN(256))), new(big.Int).Rsh(randScalar(), uint(mrand.IntN(256)))
		case 2:
			r, s = new(big.Int).Add(N, big.NewInt(int64(mrand.IntN(5)-2))), randScalar()
		default:
			r, s = randScalar(), new(big.Int).Add(N, big.NewInt(int64(mrand.IntN(5)-2)))
		}
		der := rawDER(r, s)
		// mutate some
		if i%3 == 0 && len(der) > 0 {
			j := mrand.IntN(len(der))
			der[j] ^= byte(1 << mrand.IntN(8))
		}
		if i%7 == 0 {
			der = append(der, byte(mrand.IntN(256)))
		}
		if i%11 == 0 && len(der) > 1 {
			der = der[:len(der)-1]
		}
		if !bip66(der) {
			continue
		}
		seen++
		gs, gerr := ec.ParseDERSignature(der)
		ds, derr := decdsa.ParseDERSignature(der)
		if (gerr == nil) != (derr == nil) {
			t.Fatalf("parse disagreement on %x: go-sdk %v decred %v", der, gerr, derr)
		}
		if gerr == nil {
			dr, dsv := ds.R(), ds.S()
			var rb, sb [32]byte
			dr.PutBytes(&rb)
			dsv.PutBytes(&sb)
			if gs.R.Cmp(new(big.Int).SetBytes(rb[:])) != 0 || gs.S.Cmp(new(big.Int).SetBytes(sb[:])) != 0 {
				t.Fatalf("parse values differ on %x", der)
			}
		}
	}
	t.Logf("BIP66-valid encodings compared: %d", seen)
}

// --- script level: OP_CHECKSIG under every flag regime, hooks off vs on ---

type spend struct {
	tx   *transaction.Transaction
	prev *transaction.TransactionOutput
	der  []byte // signature without the hash type
	pub  []byte
	shf  byte
}

func newSpend(t *testing.T, version uint32, shf sighash.Flag, compressed bool) spend {
	priv, _ := ec.NewPrivateKey()
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), true)
	if err != nil {
		t.Fatal(err)
	}
	lock, _ := p2pkh.Lock(addr)
	pubBytes := priv.PubKey().Compressed()
	if !compressed {
		pubBytes = priv.PubKey().Uncompressed()
		// P2PKH of the uncompressed key
		lb, _ := script.NewFromHex("76a914" + fmt.Sprintf("%x", hash160(pubBytes)) + "88ac")
		lock = lb
	}
	src := transaction.NewTransaction()
	src.AddOutput(&transaction.TransactionOutput{Satoshis: 5000, LockingScript: lock})
	tx := transaction.NewTransaction()
	tx.Version = version
	tx.AddInputFromTx(src, 0, nil)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 4000, LockingScript: lock})
	sh, err := tx.CalcInputSignatureHash(0, shf)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := priv.Sign(sh)
	if err != nil {
		t.Fatal(err)
	}
	return spend{tx: tx, prev: tx.Inputs[0].SourceTxOutput(), der: sig.Serialize(), pub: pubBytes, shf: byte(shf)}
}

func hash160(b []byte) []byte { return crypto.Ripemd160(crypto.Sha256(b)) }

func (sp spend) run(sigWithType, pub []byte, flags scriptflag.Flag) error {
	us := &script.Script{}
	_ = us.AppendPushData(sigWithType)
	_ = us.AppendPushData(pub)
	sp.tx.Inputs[0].UnlockingScript = us
	return interpreter.NewEngine().Execute(
		interpreter.WithTx(sp.tx, 0, sp.prev),
		interpreter.WithFlags(flags),
	)
}

func errKey(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

func mutateSig(der []byte, i int) []byte {
	out := append([]byte(nil), der...)
	switch i % 9 {
	case 0:
		return out
	case 1: // high S
		s, err := ec.ParseDERSignature(der)
		if err != nil {
			return out
		}
		return rawDER(s.R, new(big.Int).Sub(N, s.S))
	case 2: // bit flip
		j := mrand.IntN(len(out))
		out[j] ^= byte(1 << mrand.IntN(8))
		return out
	case 3: // trailing byte
		return append(out, 0x00)
	case 4: // extra padding on R
		s, _ := ec.ParseDERSignature(der)
		if s == nil {
			return out
		}
		rb := append([]byte{0, 0}, s.R.Bytes()...)
		sb := s.S.Bytes()
		if sb[0]&0x80 != 0 {
			sb = append([]byte{0}, sb...)
		}
		o := []byte{0x30, byte(4 + len(rb) + len(sb)), 0x02, byte(len(rb))}
		o = append(o, rb...)
		o = append(o, 0x02, byte(len(sb)))
		return append(o, sb...)
	case 5: // truncated
		return out[:len(out)-1]
	case 6: // r = 0
		s, _ := ec.ParseDERSignature(der)
		if s == nil {
			return out
		}
		return rawDER(big.NewInt(0), s.S)
	case 7: // s = N
		s, _ := ec.ParseDERSignature(der)
		if s == nil {
			return out
		}
		return rawDER(s.R, N)
	default: // random r, s
		return rawDER(randScalar(), randScalar())
	}
}

func TestScriptDifferential(t *testing.T) {
	main := scriptflag.MainNetActivationHeights.BlockValidationFlags(0xffffffff, 0xffffffff)
	all := scriptflag.ActivationHeights{}.BlockValidationFlags(0xffffffff, 0xffffffff)
	regimes := map[string]scriptflag.Flag{
		"spv-main":       main,
		"spv-all":        all,
		"spv-main-utxo0": scriptflag.MainNetActivationHeights.BlockValidationFlags(100, 0xffffffff),
		"dersig":         scriptflag.VerifyDERSignatures | scriptflag.EnableSighashForkID | scriptflag.UTXOAfterGenesis,
		"strictenc":      scriptflag.VerifyStrictEncoding | scriptflag.EnableSighashForkID | scriptflag.UTXOAfterGenesis,
		"lows":           scriptflag.VerifyLowS | scriptflag.EnableSighashForkID | scriptflag.UTXOAfterGenesis,
		"lax-forkid":     scriptflag.EnableSighashForkID | scriptflag.UTXOAfterGenesis,
		"lax-legacy":     scriptflag.UTXOAfterGenesis,
		"lax-pregenesis": 0,
	}
	n := count(300) * 2
	agree, okCount := 0, 0
	for name, flags := range regimes {
		for _, version := range []uint32{1, 2} {
			for _, compressed := range []bool{true, false} {
				shf := sighash.AllForkID
				if flags&scriptflag.EnableSighashForkID == 0 {
					shf = sighash.All
				}
				sp := newSpend(t, version, shf, compressed)
				for i := 0; i < n/8+9; i++ {
					sigb := mutateSig(sp.der, i)
					full := append(append([]byte(nil), sigb...), sp.shf)
					pub := sp.pub
					if i%13 == 12 && compressed {
						pk, _ := ec.ParsePubKey(sp.pub)
						pub = pk.Hybrid()
					}
					Uninstall()
					e0 := sp.run(full, pub, flags)
					Install()
					e1 := sp.run(full, pub, flags)
					Uninstall()
					if errKey(e0) != errKey(e1) {
						t.Fatalf("%s v%d comp=%v case %d sig %x: builtin %q hooked %q", name, version, compressed, i, full, errKey(e0), errKey(e1))
					}
					agree++
					if e0 == nil {
						okCount++
					}
				}
			}
		}
	}
	t.Logf("script cases agreeing: %d (%d accepted)", agree, okCount)
}
