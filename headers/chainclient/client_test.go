package chainclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

func TestClientRoundTrip(t *testing.T) {
	root := chainhash.Hash{0x07}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tip":
			_, _ = w.Write([]byte(`{"height":812345,"hash":"` + root.String() + `"}`))
		case "/v1/root/101":
			_, _ = w.Write([]byte(`{"height":101,"merkleRoot":"` + root.String() + `"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := &Client{Base: srv.URL + "/v1"}

	h, err := c.CurrentHeight(context.Background())
	if err != nil || h != 812345 {
		t.Fatalf("CurrentHeight = %d/%v", h, err)
	}
	ok, err := c.IsValidRootForHeight(context.Background(), &root, 101)
	if err != nil || !ok {
		t.Fatalf("matching root = %v/%v", ok, err)
	}
	other := chainhash.Hash{0x08}
	ok, err = c.IsValidRootForHeight(context.Background(), &other, 101)
	if err != nil || ok {
		t.Fatalf("mismatched root = %v/%v, want false/nil", ok, err)
	}
}

// TestUnknownHeightIsNotAnError pins the distinction a caller depends on: a
// height the store does not hold means "cannot check this yet", not a fault.
func TestUnknownHeightIsNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := &Client{Base: srv.URL + "/v1"}
	root := chainhash.Hash{0x07}
	ok, err := c.IsValidRootForHeight(context.Background(), &root, 999)
	if err != nil {
		t.Fatalf("unknown height returned an error: %v", err)
	}
	if ok {
		t.Fatal("unknown height reported the root as valid")
	}
}

func TestServerErrorIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := &Client{Base: srv.URL + "/v1"}
	root := chainhash.Hash{0x07}
	if _, err := c.IsValidRootForHeight(context.Background(), &root, 1); err == nil {
		t.Fatal("a 500 was reported as a clean negative")
	}
	if _, err := c.CurrentHeight(context.Background()); err == nil {
		t.Fatal("a 500 on tip was reported as success")
	}
}
