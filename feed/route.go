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
	// Also are the site's other local targets for these topics: each is
	// handed every object the primary is (see Also).
	Also []*Also
}

// OfferAlso hands an object to each of the route's Also targets. A nil
// route does nothing.
func (r *Route) OfferAlso(topic string, object []byte) {
	if r == nil {
		return
	}
	for _, a := range r.Also {
		a.Offer(topic, object)
	}
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

// RouteSpec is one parsed `-engine-route` value.
type RouteSpec struct {
	Pattern, Primary, Shadow string
	// Also are the origins of the route's other local targets, in order.
	Also []string
}

// ParseRoute parses one `-engine-route` value:
//
//	GLOB=PRIMARY[,shadow=SHADOW][,also=TARGET]...
//
// PRIMARY, SHADOW and each TARGET are bare submit origins like -engine. It
// returns the parts; the caller builds the clients. ParseRouteSpec also
// returns the also targets.
func ParseRoute(s string) (pattern, primary, shadow string, err error) {
	rs, err := ParseRouteSpec(s)
	return rs.Pattern, rs.Primary, rs.Shadow, err
}

// ParseRouteSpec parses one `-engine-route` value. A glob that path.Match
// rejects, a missing primary, a prefixed origin, a shadow equal to the
// primary, or an also target equal to the primary, the shadow or another
// also target is an error.
func ParseRouteSpec(s string) (RouteSpec, error) {
	var rs RouteSpec
	parts := strings.Split(s, ",")
	head := strings.TrimSpace(parts[0])
	glob, prim, ok := strings.Cut(head, "=")
	if !ok || strings.TrimSpace(glob) == "" || strings.TrimSpace(prim) == "" {
		return rs, fmt.Errorf("engine route %q: want GLOB=PRIMARY[,shadow=SHADOW][,also=TARGET]...", s)
	}
	rs.Pattern, rs.Primary = strings.TrimSpace(glob), strings.TrimSpace(prim)
	if _, err := path.Match(rs.Pattern, ""); err != nil {
		return RouteSpec{}, fmt.Errorf("engine route %q: bad glob %q: %w", s, rs.Pattern, err)
	}
	for _, p := range parts[1:] {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch {
		case !ok || v == "":
			return RouteSpec{}, fmt.Errorf("engine route %q: option %q is not key=value", s, p)
		case k == "shadow":
			rs.Shadow = v
		case k == "also":
			rs.Also = append(rs.Also, v)
		default:
			return RouteSpec{}, fmt.Errorf("engine route %q: unknown option %q", s, k)
		}
	}
	for _, o := range append([]string{rs.Primary, rs.Shadow}, rs.Also...) {
		if strings.Contains(o, "/api/v1") {
			return RouteSpec{}, fmt.Errorf("engine route %q: %s is a bare origin: it carries no /api/v1 prefix", s, o)
		}
	}
	norm := func(o string) string { return strings.TrimRight(o, "/") }
	if rs.Shadow != "" && norm(rs.Shadow) == norm(rs.Primary) {
		return RouteSpec{}, fmt.Errorf("engine route %q: the shadow is the same origin as the primary: a shadow of itself compares nothing", s)
	}
	seen := map[string]bool{norm(rs.Primary): true}
	if rs.Shadow != "" {
		seen[norm(rs.Shadow)] = true
	}
	for _, a := range rs.Also {
		if seen[norm(a)] {
			return RouteSpec{}, fmt.Errorf("engine route %q: also target %s is already the route's primary, shadow or another also target", s, a)
		}
		seen[norm(a)] = true
	}
	return rs, nil
}
