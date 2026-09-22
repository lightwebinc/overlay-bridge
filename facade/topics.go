package facade

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ParseTopics accepts the two x-topics wire forms our clients actually send.
//
//	tm_a,tm_b        the comma list: the Go engine's generated binder form, and
//	                 the TypeScript host's non-bracket branch
//	["tm_a","tm_b"]  the JSON array: the SDK facilitator's form, and the
//	                 TypeScript host's bracket branch
//
// Accepting both is what makes the facade a drop-in for a client written
// against either host, which is the whole point of serving the engine's own
// interface rather than a new one.
//
// Two deliberate differences from the Go engine's binder, which this replaces
// rather than imitates:
//
//   - Values are trimmed. That binder splits on commas without trimming, so a
//     single leading space turns a valid topic into an unknown one and fails
//     the whole submit.
//   - A repeated header is an error. That binder reads only the first value
//     and silently discards the rest, which loses topics without saying so.
func ParseTopics(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("missing x-topics header")
	}
	if len(values) > 1 {
		return nil, fmt.Errorf("x-topics sent %d times; send one header", len(values))
	}
	raw := strings.TrimSpace(values[0])
	if raw == "" {
		return nil, fmt.Errorf("empty x-topics header")
	}

	var names []string
	if strings.HasPrefix(raw, "[") {
		if err := json.Unmarshal([]byte(raw), &names); err != nil {
			return nil, fmt.Errorf("x-topics is not a JSON array: %w", err)
		}
	} else {
		names = strings.Split(raw, ",")
	}

	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			return nil, fmt.Errorf("x-topics contains an empty topic name")
		}
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("x-topics names no topics")
	}
	return out, nil
}
