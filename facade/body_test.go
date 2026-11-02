package facade

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestBodyWithContentLengthIsReadInOneAllocation pins the presized read. A
// client that declares its Content-Length (every Go client posting a
// bytes.Reader does) is read into one buffer of exactly that size plus the
// byte that proves nothing follows. ReadAll grew its buffer step by step:
// about thirty allocations and two and a half times the body in bytes for a
// 4 MiB piece (measured on Go 1.27), on the client's synchronous request path.
func TestBodyWithContentLengthIsReadInOneAllocation(t *testing.T) {
	body := bytes.Repeat([]byte{0xAB}, 1<<20)
	br := bytes.NewReader(body)
	req := httptest.NewRequest(http.MethodPost, "/submit", br)
	if req.ContentLength != int64(len(body)) {
		t.Fatalf("ContentLength = %d, want %d", req.ContentLength, len(body))
	}
	var got []byte
	allocs := testing.AllocsPerRun(10, func() {
		br.Reset(body)
		var err error
		if got, err = readBody(req, 4<<20); err != nil {
			t.Fatalf("readBody: %v", err)
		}
	})
	if allocs != 1 {
		t.Fatalf("allocations per read = %v, want exactly 1: the buffer is sized from Content-Length, not grown", allocs)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the body read back differs from the one sent")
	}
	if cap(got) != len(got)+1 {
		t.Fatalf("cap = %d, want len+1 = %d: one buffer of the declared size plus the probe byte", cap(got), len(got)+1)
	}
}

// TestBodyIsReadWholeWithOrWithoutContentLength: the object the engine and
// the plane get is byte for byte what the client sent, whether the request
// declared its length (the presized path) or not (the bounded ReadAll path a
// chunked body takes).
func TestBodyIsReadWholeWithOrWithoutContentLength(t *testing.T) {
	for _, tc := range []struct {
		name     string
		declared int64
	}{
		{"declared", int64(len(beefObj))},
		{"undeclared", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := &stubPublisher{}
			f, _ := newFacade(t, &stubEngine{}, pub, "tm_example")
			req := httptest.NewRequest(http.MethodPost, "/submit", bytes.NewReader(beefObj))
			req.ContentLength = tc.declared
			req.Header.Set("x-topics", "tm_example")
			rec := httptest.NewRecorder()
			f.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("code = %d: %s", rec.Code, rec.Body)
			}
			if len(pub.records) != 1 {
				t.Fatalf("published %d records, want 1", len(pub.records))
			}
			_, object, err := decodeRecordTopics(pub.records[0])
			if err != nil {
				t.Fatalf("decode published record: %v", err)
			}
			if !bytes.Equal(object, beefObj) {
				t.Fatalf("published object %x, want %x", object, beefObj)
			}
		})
	}
}

// TestBodyNotMatchingContentLengthIsMalformed: a body that ends before its
// declared length, or runs past it, is the 400 a body that cannot be read
// always was, and never reaches the engine.
func TestBodyNotMatchingContentLengthIsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		declared int64
	}{
		{"body ends early", int64(len(beefObj)) + 3},
		{"body runs past", int64(len(beefObj)) - 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := &stubEngine{}
			f, _ := newFacade(t, eng, &stubPublisher{}, "tm_example")
			req := httptest.NewRequest(http.MethodPost, "/submit", bytes.NewReader(beefObj))
			req.ContentLength = tc.declared
			req.Header.Set("x-topics", "tm_example")
			rec := httptest.NewRecorder()
			f.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("code = %d, want 400: %s", rec.Code, rec.Body)
			}
			if eng.calls != 0 {
				t.Fatal("a malformed body reached the engine")
			}
			if st := f.Stats(); st.Malformed != 1 {
				t.Fatalf("stats = %+v", st)
			}
		})
	}
}

// TestOversizeIsRefusedHoweverItIsDeclared: the ceiling answers 413 whether
// the body declared its length (past the bound, so the bounded fallback) or
// not, and the refusal reads no more than one byte past the ceiling.
func TestOversizeIsRefusedHoweverItIsDeclared(t *testing.T) {
	big := append([]byte{0x01, 0x01, 0x01, 0x01}, make([]byte, 64)...)
	for _, tc := range []struct {
		name     string
		declared int64
	}{
		{"declared", int64(len(big))},
		{"undeclared", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := &stubEngine{}
			f, _ := newFacade(t, eng, &stubPublisher{}, "tm_example")
			f.cfg.MaxObject = 16
			br := bytes.NewReader(big)
			req := httptest.NewRequest(http.MethodPost, "/submit", br)
			req.ContentLength = tc.declared
			req.Header.Set("x-topics", "tm_example")
			rec := httptest.NewRecorder()
			f.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("code = %d, want 413: %s", rec.Code, rec.Body)
			}
			if left := br.Len(); left != len(big)-17 {
				t.Fatalf("%d bytes unread, want %d: the refusal reads one byte past the ceiling and no more", left, len(big)-17)
			}
			if eng.calls != 0 {
				t.Fatal("an oversize object reached the engine")
			}
		})
	}
}
