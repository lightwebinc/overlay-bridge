//go:build race

package secpverify

// raceOn: the race detector slows the curve arithmetic about tenfold, so
// the differential tests run a fifth of their cases under it.
const raceOn = true
