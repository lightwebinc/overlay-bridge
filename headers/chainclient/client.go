// Package chainclient is a chain tracker backed by an overlay-bridge header
// read API.
//
// It exists for GO consumers of the same header store that an engine reads: a
// command-line client verifying proofs, or a Go host passing it as its chain
// tracker. It imports only the SDK on purpose, so taking it costs a consumer
// nothing but this one small dependency.
//
// A TypeScript host cannot import it at all and does not need to: it reaches
// the same store over HTTP through the chaintracks shape the same API serves.
package chainclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// Client speaks the bridge's native /v1 shape.
type Client struct {
	// Base is the read API's origin including the version segment, for
	// example http://10.0.0.1:9178/v1
	Base string
	HTTP *http.Client
}

var _ chaintracker.ChainTracker = (*Client)(nil)

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (c *Client) get(ctx context.Context, path string, out any) (int, error) {
	url := strings.TrimRight(c.Base, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return resp.StatusCode, fmt.Errorf("chainclient: decode %s: %w", url, err)
	}
	return resp.StatusCode, nil
}

// IsValidRootForHeight reports whether root is the merkle root at height.
//
// A height the store does not hold answers 404, which is reported as "not
// valid" with no error: it means the proof cannot be checked yet, not that
// something is broken.
func (c *Client) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	if root == nil {
		return false, nil
	}
	var body struct {
		MerkleRoot string `json:"merkleRoot"`
	}
	code, err := c.get(ctx, fmt.Sprintf("/root/%d", height), &body)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK:
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("chainclient: root for height %d: unexpected status %d", height, code)
	}
	got, err := chainhash.NewHashFromHex(body.MerkleRoot)
	if err != nil {
		return false, fmt.Errorf("chainclient: bad merkleRoot %q: %w", body.MerkleRoot, err)
	}
	return got.IsEqual(root), nil
}

// CurrentHeight reports the tip height the bridge has reached.
func (c *Client) CurrentHeight(ctx context.Context) (uint32, error) {
	var body struct {
		Height uint32 `json:"height"`
	}
	code, err := c.get(ctx, "/tip", &body)
	if err != nil {
		return 0, err
	}
	if code != http.StatusOK {
		return 0, fmt.Errorf("chainclient: tip: unexpected status %d", code)
	}
	return body.Height, nil
}
