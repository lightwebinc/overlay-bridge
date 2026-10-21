package spv

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// The trust marker: how a verifier tells an engine on the same machine that an
// object passed, so the engine can skip its own script verification.
//
// # The trust boundary
//
// The marker is an HMAC-SHA256, under a key shared by the verifier and the
// engine through a file only root can read, over a fixed label and the EXACT
// object bytes submitted. An engine honours it only when all of these hold:
//
//  1. the engine was configured to trust it (off by default);
//  2. the request arrived on a loopback address;
//  3. the engine holds the key, and the HMAC over the body it actually read
//     matches, compared in constant time.
//
// Loopback alone is not enough, because anything on the machine can reach
// loopback; the key alone is not enough, because a key that leaked off the
// machine must not let a remote submitter skip verification. The HMAC binds
// the marker to one body, so a marker observed on the wire cannot be moved to
// another object: replaying it re-submits bytes that already passed. A marker
// that fails any check is ignored, not refused: the engine verifies that
// object in full, as it would with no marker at all.
//
// What the marker waives is SCRIPT verification only. The engine still parses
// the object, checks every merkle path against its own chain tracker, checks
// amounts and runs its topic managers.

// MarkerHeader is the request header that carries the marker.
const MarkerHeader = "X-Overlay-Bridge-Verified"

// markerLabel domain-separates this HMAC from any other use of the key.
const markerLabel = "overlay-bridge-verified/v1\x00"

// MinKeyBytes is the shortest key accepted.
const MinKeyBytes = 32

// Marker returns the header value for body under key.
func Marker(key, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(markerLabel))
	m.Write(body)
	return "v1=" + hex.EncodeToString(m.Sum(nil))
}

// CheckMarker reports whether value is the marker for body under key.
// Constant time in the MAC comparison.
func CheckMarker(key, body []byte, value string) bool {
	got, ok := strings.CutPrefix(strings.TrimSpace(value), "v1=")
	if !ok || len(key) < MinKeyBytes {
		return false
	}
	mac, err := hex.DecodeString(got)
	if err != nil {
		return false
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte(markerLabel))
	m.Write(body)
	return hmac.Equal(mac, m.Sum(nil))
}

// LoadKey reads a marker key file: its bytes, surrounding whitespace removed,
// are the key (generate one with `openssl rand -hex 32`). The file must not be
// readable by group or others.
func LoadKey(path string) ([]byte, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("spv: marker key: %w", err)
	}
	if st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("spv: marker key %s is readable by group or others (mode %v); chmod 600 it", path, st.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("spv: marker key: %w", err)
	}
	b = bytes.TrimSpace(b)
	if len(b) < MinKeyBytes {
		return nil, fmt.Errorf("spv: marker key %s is %d bytes; at least %d are required", path, len(b), MinKeyBytes)
	}
	return b, nil
}
