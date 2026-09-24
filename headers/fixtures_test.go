package headers

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// regen rewrites the golden bodies. Run it deliberately, never in CI:
//
//	go test ./headers -run TestHeaderAPIFixtures -regen
var regen = flag.Bool("regen", false, "rewrite the header API golden bodies")

// TestHeaderAPIFixtures pins the header read API's response BODIES, not just
// its status codes.
//
// These bytes are a contract with other repositories, not an internal detail.
// A consumer that verifies SPV against this bridge implements a chain tracker
// without importing this module, so the shape has to be generated once here
// and vendored there rather than re-derived per repo from a prose description.
// A field renamed or a root rendered in the wrong byte order fails every
// comparison in a consumer while this service keeps answering 200, which is
// the failure this test exists to make loud.
func TestHeaderAPIFixtures(t *testing.T) {
	a, obs := apiWithOneHeader(t)
	h := a.Handler()

	// The fixture header's height comes from the store's own anchor, so the
	// paths are built from it rather than hard-coded. A hard-coded height
	// passed only by accident and broke the moment the anchor moved.
	at := strconv.FormatUint(uint64(obs.Height), 10)

	for _, tc := range []struct {
		name string
		path string
		code int
	}{
		{"v1_tip", "/v1/tip", http.StatusOK},
		{"v1_root_known", "/v1/root/" + at, http.StatusOK},
		{"v1_root_unknown", "/v1/root/999999", http.StatusNotFound},
		{"v1_header_known", "/v1/header/" + obs.Hash.String(), http.StatusOK},
		{"v1_header_unknown", "/v1/header/" + strings.Repeat("0", 63) + "1", http.StatusNotFound},
		{"chaintracks_height", "/chaintracks/v2/height", http.StatusOK},
		{"chaintracks_header_known", "/chaintracks/v2/header/height/" + at, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.code {
				t.Fatalf("%s = %d, want %d", tc.path, rec.Code, tc.code)
			}
			// Re-marshal through a map so the golden is stable whatever order
			// the handler's map iterated in.
			var parsed map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
				t.Fatalf("%s: body is not JSON: %v", tc.path, err)
			}
			got, err := json.MarshalIndent(parsed, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')

			path := filepath.Join("testdata", tc.name+".json")
			if *regen {
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
				t.Logf("wrote %s", path)
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%s: %v (run with -regen to create it)", path, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s changed.\n got: %s\nwant: %s", path, got, want)
			}
		})
	}

	// The tip and the header fixtures must describe the SAME header. A pair
	// that disagreed would send a consumer looking for a header that never
	// existed.
	tip, err := os.ReadFile(filepath.Join("testdata", "v1_tip.json"))
	if err != nil {
		t.Fatal(err)
	}
	hdr, err := os.ReadFile(filepath.Join("testdata", "chaintracks_header_known.json"))
	if err != nil {
		t.Fatal(err)
	}
	var tipBody, hdrBody map[string]any
	if err := json.Unmarshal(tip, &tipBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(hdr, &hdrBody); err != nil {
		t.Fatal(err)
	}
	if tipBody["hash"] != hdrBody["hash"] {
		t.Fatalf("tip hash %v is not the header fixture's hash %v", tipBody["hash"], hdrBody["hash"])
	}
}
