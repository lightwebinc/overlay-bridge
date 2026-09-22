package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/lightwebinc/overlay-bridge/headers"
)

// anchorClient reads an operator-chosen header service for the initial anchor,
// for roots below it, and to re-anchor across a gap in the lane.
//
// This is the one component that reaches outside the bridge, and it is
// deliberately the operator's choice rather than a compiled-in default: a
// default here would quietly send a host's verification questions to a third
// party. Every call is bounded.
type anchorClient struct {
	base    string
	timeout time.Duration
	http    *http.Client
}

var (
	_ headers.RootSource   = (*anchorClient)(nil)
	_ headers.HeaderSource = (*anchorClient)(nil)
	_ headers.HeaderLookup = (*anchorClient)(nil)
)

func (a *anchorClient) client() *http.Client {
	if a.http != nil {
		return a.http
	}
	t := a.timeout
	if t == 0 {
		t = 5 * time.Second
	}
	return &http.Client{Timeout: t}
}

func (a *anchorClient) get(ctx context.Context, path string, out any) error {
	url := strings.TrimRight(a.base, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := a.client().Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type headerBody struct {
	Height     uint32 `json:"height"`
	Hash       string `json:"hash"`
	MerkleRoot string `json:"merkleRoot"`
}

func (a *anchorClient) tip(ctx context.Context) (headers.Anchor, error) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	var b headerBody
	if err := a.get(ctx, "/v1/tip", &b); err != nil {
		return headers.Anchor{}, err
	}
	h, err := chainhash.NewHashFromHex(b.Hash)
	if err != nil {
		return headers.Anchor{}, fmt.Errorf("anchor hash %q: %w", b.Hash, err)
	}
	return headers.Anchor{Hash: *h, Height: b.Height}, nil
}

func (a *anchorClient) RootAt(ctx context.Context, height uint32) (*chainhash.Hash, error) {
	var b headerBody
	if err := a.get(ctx, fmt.Sprintf("/v1/root/%d", height), &b); err != nil {
		return nil, err
	}
	return chainhash.NewHashFromHex(b.MerkleRoot)
}

// HeaderForHeight names the block at a height, so the read API can serve a
// pre-anchor height with its real hash rather than none.
func (a *anchorClient) HeaderForHeight(ctx context.Context, height uint32) (chainhash.Hash, chainhash.Hash, error) {
	var b headerBody
	if err := a.get(ctx, fmt.Sprintf("/v1/root/%d", height), &b); err != nil {
		return chainhash.Hash{}, chainhash.Hash{}, err
	}
	root, err := chainhash.NewHashFromHex(b.MerkleRoot)
	if err != nil {
		return chainhash.Hash{}, chainhash.Hash{}, fmt.Errorf("root at %d: %w", height, err)
	}
	if b.Hash == "" {
		return chainhash.Hash{}, chainhash.Hash{}, fmt.Errorf("header service names no block hash for height %d", height)
	}
	hash, err := chainhash.NewHashFromHex(b.Hash)
	if err != nil {
		return chainhash.Hash{}, chainhash.Hash{}, fmt.Errorf("hash at %d: %w", height, err)
	}
	return *hash, *root, nil
}

func (a *anchorClient) HeaderAt(ctx context.Context, hash chainhash.Hash) (uint32, chainhash.Hash, error) {
	var b headerBody
	if err := a.get(ctx, "/v1/header/"+hash.String(), &b); err != nil {
		return 0, chainhash.Hash{}, err
	}
	root, err := chainhash.NewHashFromHex(b.MerkleRoot)
	if err != nil {
		return 0, chainhash.Hash{}, err
	}
	return b.Height, *root, nil
}
