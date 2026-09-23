// Package topics reconciles the bridge's elected topic list against what its
// consumer is actually subscribed to.
//
// The gap this closes is a quiet one. A customer joins a topic in the portal;
// the broker projects it and the edge picks it up on its next snapshot poll,
// with no converge and no restart. The bridge's `-topics` is a PROCESS FLAG
// and does not follow. So the plane starts delivering a topic the bridge then
// discards, and every component reports itself healthy: the election is live,
// the edge joined the band, the frames arrive here, and the objects die one
// hop before they would have been useful.
//
// At runtime that shows up as a rising unknown_topic counter, which an alert
// watches. This is the same fact one step earlier, at startup, where an
// operator who just restarted the unit is actually looking.
//
// It WARNS AND CONTINUES, never refuses. Refusing would turn a customer's
// topic join into a bridge that will not start, which is a worse outage than a
// topic that is not bridged: the un-bridged topic loses one topic's objects,
// the refusal loses all of them.
package topics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ID is the projected topic id: SHA-256 of the topic NAME. The broker projects
// it so the edge never needs names, which is why a bridge holding names has to
// derive it to compare at all.
func ID(name string) [32]byte { return sha256.Sum256([]byte(name)) }

// Diff is what the bridge carries versus what its consumer is subscribed to.
type Diff struct {
	// Subscribed but NOT bridged. These objects arrive and are discarded.
	// This is the direction that loses data.
	Missing []string
	// Bridged but NOT subscribed. Harmless: nothing is delivered for them, so
	// the flag is merely wider than the election. Reported anyway, because it
	// is usually a leftover from a topic somebody left and it makes the flag
	// misleading to read.
	Extra []string
	// AllTopics reports the aggregator posture, in which the consumer receives
	// every topic on the plane and no finite -topics list can match it.
	AllTopics bool
}

// OK reports whether anything is being dropped.
func (d Diff) OK() bool { return len(d.Missing) == 0 && !d.AllTopics }

// Describe renders the diff for a log line.
func (d Diff) Describe() string {
	switch {
	case d.AllTopics:
		return "consumer is an AGGREGATOR (every topic on the plane); a finite -topics list cannot match it, " +
			"so objects outside the list are being discarded"
	case len(d.Missing) > 0 && len(d.Extra) > 0:
		return fmt.Sprintf("subscribed but NOT bridged: %s (their objects are being discarded); bridged but not subscribed: %s",
			strings.Join(d.Missing, ","), strings.Join(d.Extra, ","))
	case len(d.Missing) > 0:
		return fmt.Sprintf("subscribed but NOT bridged: %s (their objects are being discarded)",
			strings.Join(d.Missing, ","))
	case len(d.Extra) > 0:
		return fmt.Sprintf("bridged but not subscribed: %s (harmless, nothing is delivered for them)",
			strings.Join(d.Extra, ","))
	default:
		return "elected topics match the consumer's subscription"
	}
}

// Compare diffs the bridge's own names against the consumer's projected ids.
//
// It compares IDS, not names, because ids are what the broker actually
// projects and what the edge filters on. Comparing names would agree with a
// broker that had renamed a topic under the same id, which is precisely the
// case worth catching.
func Compare(bridged []string, subscribedIDs []string, allTopics bool) Diff {
	byID := make(map[string]string, len(bridged))
	for _, n := range bridged {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		id := ID(n)
		byID[hex.EncodeToString(id[:])] = n
	}
	sub := make(map[string]struct{}, len(subscribedIDs))
	for _, s := range subscribedIDs {
		sub[strings.ToLower(strings.TrimSpace(s))] = struct{}{}
	}

	d := Diff{AllTopics: allTopics}
	for id := range sub {
		if _, ok := byID[id]; !ok {
			// The bridge does not hold this topic's NAME, by definition: the
			// name map is the thing it is missing. Report the id, which is
			// what the operator will see in the edge snapshot anyway.
			d.Missing = append(d.Missing, id)
		}
	}
	for id, name := range byID {
		if _, ok := sub[id]; !ok {
			d.Extra = append(d.Extra, name)
		}
	}
	sort.Strings(d.Missing)
	sort.Strings(d.Extra)
	return d
}

// Subscription is the slice of a consumer record this check reads.
type Subscription struct {
	TopicIDs  []string
	AllTopics bool
}

// Fetch reads one consumer's projected subscription from the broker's state
// API.
//
// Read-only and best-effort by design: every failure here is reported to the
// caller and none of them is fatal, because a bridge that refused to start
// when the broker was briefly unreachable would convert a control-plane blip
// into a data-plane outage.
func Fetch(ctx context.Context, base, consumerID, token string, hc *http.Client) (Subscription, error) {
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	url := strings.TrimRight(base, "/") + "/v1/state"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Subscription{}, err
	}
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return Subscription{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return Subscription{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Subscription{}, fmt.Errorf("broker state: status %d", resp.StatusCode)
	}
	var state struct {
		Consumers []struct {
			ID           string `json:"id"`
			Subscription struct {
				BeefTopics    []string `json:"beefTopics"`
				AllBeefTopics bool     `json:"allBeefTopics"`
			} `json:"subscription"`
		} `json:"consumers"`
	}
	if err := json.Unmarshal(body, &state); err != nil {
		return Subscription{}, fmt.Errorf("broker state: %w", err)
	}
	for _, c := range state.Consumers {
		if c.ID != consumerID {
			continue
		}
		// The state API answers NAMES (it is the operator view); the edge
		// seam answers ids. Derive so this compares ids either way.
		ids := make([]string, 0, len(c.Subscription.BeefTopics))
		for _, n := range c.Subscription.BeefTopics {
			id := ID(n)
			ids = append(ids, hex.EncodeToString(id[:]))
		}
		return Subscription{TopicIDs: ids, AllTopics: c.Subscription.AllBeefTopics}, nil
	}
	return Subscription{}, fmt.Errorf("broker state: consumer %s is not there", consumerID)
}
