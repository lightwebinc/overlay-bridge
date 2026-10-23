package feed

import (
	"fmt"
	"path"
	"strings"
)

// Route sends the topics a glob names to an engine of their own, with a
// shadow of their own, while every other topic keeps the bridge's default
// primary and shadow.
//
// It exists for a per-topic cutover: one application's topics move to a new
// engine while the others stay on the old one, and the old engine shadows
// the new one for those topics (parity read in reverse) until the port is
// trusted. The route is the unit of rollback: swap its primary and shadow
// and the topics are back, with nothing else touched.
type Route struct {
	// Pattern is a path.Match glob over the topic name (`tm_bsecret_*`).
	// A topic name holds no slash, so `*` matches the whole remainder.
	Pattern string
	// Submit is the route's primary. For the facade it should also be a
	// DetailSubmitter, so the answer keeps the engine's wrapper form.
	Submit Submitter
	// Shadow, when set, is offered every submit the route's primary is
	// handed, exactly as the default shadow is. Nil = no shadow for these
	// topics.
	Shadow *Shadow
}

// Matches reports whether the route carries the topic.
func (r *Route) Matches(topic string) bool {
	ok, err := path.Match(r.Pattern, topic)
	return err == nil && ok
}

// Name labels the route in metrics and logs: its pattern.
func (r *Route) Name() string { return r.Pattern }

// RouteFor picks the first route whose pattern matches the topic, or nil
// for the default engines.
func RouteFor(routes []*Route, topic string) *Route {
	for _, r := range routes {
		if r != nil && r.Matches(topic) {
			return r
		}
	}
	return nil
}

// DefaultRoute is the label of the unrouted engines in metrics.
const DefaultRoute = "default"

// ParseRoute parses one `-engine-route` value:
//
//	GLOB=PRIMARY[,shadow=SHADOW]
//
// PRIMARY and SHADOW are bare submit origins like -engine. It returns the
// parts; the caller builds the clients. A glob that path.Match rejects, a
// missing primary, a prefixed origin or a shadow equal to the primary is an
// error.
func ParseRoute(s string) (pattern, primary, shadow string, err error) {
	parts := strings.Split(s, ",")
	head := strings.TrimSpace(parts[0])
	glob, prim, ok := strings.Cut(head, "=")
	if !ok || strings.TrimSpace(glob) == "" || strings.TrimSpace(prim) == "" {
		return "", "", "", fmt.Errorf("engine route %q: want GLOB=PRIMARY[,shadow=SHADOW]", s)
	}
	pattern, primary = strings.TrimSpace(glob), strings.TrimSpace(prim)
	if _, err := path.Match(pattern, ""); err != nil {
		return "", "", "", fmt.Errorf("engine route %q: bad glob %q: %w", s, pattern, err)
	}
	for _, p := range parts[1:] {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch {
		case !ok || v == "":
			return "", "", "", fmt.Errorf("engine route %q: option %q is not key=value", s, p)
		case k == "shadow":
			shadow = v
		default:
			return "", "", "", fmt.Errorf("engine route %q: unknown option %q", s, k)
		}
	}
	for _, o := range []string{primary, shadow} {
		if strings.Contains(o, "/api/v1") {
			return "", "", "", fmt.Errorf("engine route %q: %s is a bare origin: it carries no /api/v1 prefix", s, o)
		}
	}
	if shadow != "" && strings.TrimRight(shadow, "/") == strings.TrimRight(primary, "/") {
		return "", "", "", fmt.Errorf("engine route %q: the shadow is the same origin as the primary: a shadow of itself compares nothing", s)
	}
	return pattern, primary, shadow, nil
}
