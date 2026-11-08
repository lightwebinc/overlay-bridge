package feed

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/overlay-bridge/internal/testpiece"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/lanes"
)

// The per-object cost of the lane's largest objects in volume: 4 MiB piece
// carriers on a blob topic, verified in enforce mode with the marker, and
// submitted to a target that reads the body (as a storage provider fed from
// the bridge does).

const pieceTopic = "tm_bstoreblob_bench"

// pieceRecord is a delivery record carrying a piece as the plane delivers
// one: the publisher's submission record, then the object.
func pieceRecord(tb testing.TB, p *testpiece.Piece) []byte {
	tb.Helper()
	payload, err := objfmt.EncodeBEEFRecord([]string{pieceTopic}, p.BEEF)
	if err != nil {
		tb.Fatal(err)
	}
	return objfmt.EncodeBEEFDelivery(objfmt.TopicID(pieceTopic), payload)
}

// readingTarget reads every submit's body and answers output 0 admitted.
func readingTarget(tb testing.TB) *httptest.Server {
	tb.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"`+r.Header.Get("x-topics")+`":{"outputsToAdmit":[0],"coinsToRetain":[]}}`)
	}))
	tb.Cleanup(srv.Close)
	return srv
}

// pieceFeed is a synchronous feed (Workers 0: Handle does the whole job) in
// enforce mode with a marker key.
func pieceFeed(tb testing.TB, p *testpiece.Piece) *Feed {
	tb.Helper()
	return &Feed{
		Topics:        NewTopicMap([]string{pieceTopic}),
		Submit:        &Client{Base: readingTarget(tb).URL, Timeout: 30 * time.Second},
		Guard:         guard.New(time.Minute, 1<<16),
		SubmitRetries: -1,
		VerifyMode:    VerifyEnforce,
		Verify:        &spv.Verifier{Tracker: p.Tracker, Network: spv.NetworkAll},
		MarkerKey:     make([]byte, spv.MinKeyBytes),
	}
}

func submittedAll(tb testing.TB, f *Feed, n int) {
	tb.Helper()
	if s := f.Stats(); s.Submitted != uint64(n) || s.EngineError != 0 || s.SPVRefused != 0 {
		tb.Fatalf("submitted %d of %d (engine errors %d, SPV refused %d)", s.Submitted, n, s.EngineError, s.SPVRefused)
	}
}

// BenchmarkHandlePiece is the bridge's work on one delivered piece once the
// lane holds it: the decode, the copy and its identity, the SPV check, the
// marker and the submit.
func BenchmarkHandlePiece(b *testing.B) {
	p := testpiece.New(b, 4<<20)
	rec := pieceRecord(b, p)
	f := pieceFeed(b, p)
	b.SetBytes(int64(len(rec)))
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		if err := f.Handle(context.Background(), rec); err != nil {
			b.Fatal(err)
		}
		n++
	}
	submittedAll(b, f, n)
}

// BenchmarkLanePiece is the same work from the lane's socket: records
// written to a BEEF lane over loopback TCP, read by the lane and handed to
// the feed. ns/op is the time per 4 MiB record; alloc-B/lane-B is the bytes
// the process allocated per byte the lane carried.
func BenchmarkLanePiece(b *testing.B) {
	p := testpiece.New(b, 4<<20)
	rec := pieceRecord(b, p)
	f := pieceFeed(b, p)
	lane := &lanes.Lane{
		Name: "beef", Class: objfmt.ClassBEEFDelivery, Addr: "127.0.0.1:0",
		Handle: f.Handle, Log: slog.New(slog.DiscardHandler),
		MaxObject: objfmt.DefaultMaxObject + objfmt.BEEFDeliveryHeaderSize,
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- lane.Serve(ctx) }()
	exited := false
	defer func() {
		cancel()
		if exited {
			return
		}
		if err := <-served; err != nil {
			b.Error(err)
		}
	}()
	for deadline := time.Now().Add(10 * time.Second); !lane.Bound() || lane.ListenerAddr() == nil; {
		select {
		case err := <-served:
			exited = true
			b.Fatalf("the lane stopped before it bound: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			b.Fatal("the lane did not bind within 10 s")
		}
		time.Sleep(time.Millisecond)
	}
	conn, err := net.Dial("tcp", lane.ListenerAddr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	send := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := conn.Write(rec); err != nil {
				b.Fatal(err)
			}
		}
	}
	// waitSubmitted waits for n submits, and fails rather than waits on
	// when any record is refused or lost on the way, or past a deadline.
	waitSubmitted := func(n int) {
		deadline := time.Now().Add(time.Minute + time.Duration(n)*time.Second)
		for {
			s := f.Stats()
			if s.Submitted >= uint64(n) {
				return
			}
			l := lane.Stats()
			if s.EngineError+s.SPVRefused+s.ParseError+s.Rejected+s.UnknownTopic+s.Shed+l.Errors+l.Dropped > 0 || time.Now().After(deadline) {
				b.Fatalf("submitted %d of %d: engine errors %d, SPV refused %d, parse errors %d, rejected %d, unknown topic %d, shed %d; lane errors %d, connections dropped %d",
					s.Submitted, n, s.EngineError, s.SPVRefused, s.ParseError, s.Rejected, s.UnknownTopic, s.Shed, l.Errors, l.Dropped)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}
	// One record first, outside the measurement: the lane's read buffer
	// grows to its size and the submit client opens its connection.
	send(1)
	waitSubmitted(1)

	b.SetBytes(int64(len(rec)))
	b.ReportAllocs()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	send(b.N)
	waitSubmitted(1 + b.N)
	b.StopTimer()
	runtime.ReadMemStats(&after)
	submittedAll(b, f, 1+b.N)
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N*len(rec)), "alloc-B/lane-B")
}
