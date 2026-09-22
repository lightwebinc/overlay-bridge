package facade

import "testing"

func TestParseTopicsBothWireForms(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want []string
	}{
		{"comma list", []string{"tm_a,tm_b"}, []string{"tm_a", "tm_b"}},
		{"json array", []string{`["tm_a","tm_b"]`}, []string{"tm_a", "tm_b"}},
		{"single name", []string{"tm_a"}, []string{"tm_a"}},
		// The Go engine's binder does not trim, so a leading space there turns
		// a valid topic into an unknown one. This accepts it.
		{"spaced comma list", []string{" tm_a , tm_b "}, []string{"tm_a", "tm_b"}},
		{"spaced json array", []string{` ["tm_a", "tm_b"] `}, []string{"tm_a", "tm_b"}},
		{"duplicates collapse", []string{"tm_a,tm_a"}, []string{"tm_a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTopics(tc.in)
			if err != nil {
				t.Fatalf("ParseTopics(%q): %v", tc.in, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestParseTopicsRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
	}{
		{"absent", nil},
		{"empty", []string{""}},
		{"whitespace only", []string{"   "}},
		// A repeated header loses topics silently in the engine's own binder.
		{"repeated header", []string{"tm_a", "tm_b"}},
		{"empty name in list", []string{"tm_a,,tm_b"}},
		{"malformed json", []string{`["tm_a"`}},
		{"json with an empty name", []string{`["tm_a",""]`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := ParseTopics(tc.in); err == nil {
				t.Fatalf("ParseTopics(%q) = %v, want an error", tc.in, got)
			}
		})
	}
}
