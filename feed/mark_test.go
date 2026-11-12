package feed

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/lightwebinc/overlay-bridge/guard"
	"github.com/lightwebinc/overlay-bridge/internal/testpiece"
	"github.com/lightwebinc/overlay-bridge/pkg/spv"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// The loop guard is told of a delivered object before any submit, under the
// object's content identifier, whichever of these carried the identity: the
// verifier's digest, taken with the txids, or a hash made for it.

func marked(t *testing.T, f *Feed, object []byte, topic string) {
	t.Helper()
	if dir, ok := f.Guard.Lookup(objfmt.ContentID(object), objfmt.TopicID(topic)); !ok || dir != registry.Delivered {
		t.Fatalf("guard holds %v/%v for the object, want Delivered", dir, ok)
	}
}

// TestVerifiedObjectIsMarkedBeforeItsSubmit: the real verifier, reading a
// piece in place with ObjectDigest set, and the mark seen from inside the
// submit.
func TestVerifiedObjectIsMarkedBeforeItsSubmit(t *testing.T) {
	p := testpiece.New(t, 300<<10)
	g := guard.New(time.Minute, 1024)
	topicID := objfmt.TopicID(pieceTopic)
	var during bool
	sub := &stubSubmitterFunc{fn: func(context.Context, string, []byte) (overlay.Steak, error) {
		_, during = g.Lookup(objfmt.ContentID(p.BEEF), topicID)
		return admitted(pieceTopic), nil
	}}
	v := &spv.Verifier{Tracker: p.Tracker, Network: spv.NetworkAll, ObjectDigest: true}
	if vd := v.Verify(t.Context(), p.BEEF); !vd.HasObjectSHA256 || vd.ObjectSHA256 != sha256.Sum256(p.BEEF) {
		t.Fatalf("verdict digest set %v, equal %v", vd.HasObjectSHA256, vd.ObjectSHA256 == sha256.Sum256(p.BEEF))
	}
	f := &Feed{Topics: NewTopicMap([]string{pieceTopic}), Submit: sub, Guard: g, SubmitRetries: -1,
		VerifyMode: VerifyEnforce, Verify: v}
	if err := f.Handle(t.Context(), objfmt.EncodeBEEFDelivery(topicID, p.BEEF)); err != nil {
		t.Fatal(err)
	}
	if !during {
		t.Fatal("guard was not marked before the engine submit")
	}
	marked(t, f, p.BEEF, pieceTopic)
}

// TestObjectIsMarkedWithoutAVerifierDigest: a verifier that carries no
// digest (as when it declines the in-place read) leaves the identity to the
// feed.
func TestObjectIsMarkedWithoutAVerifierDigest(t *testing.T) {
	sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	f := verifyFeed(t, sub, VerifyEnforce, spv.ResultOK)
	if err := deliver(t, f); err != nil {
		t.Fatal(err)
	}
	marked(t, f, beefObj, "tm_example")
}

// TestRefusedObjectIsStillMarked: the plane delivered the bytes whatever the
// verdict, so a refused object is marked as it was before.
func TestRefusedObjectIsStillMarked(t *testing.T) {
	sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	f := verifyFeed(t, sub, VerifyEnforce, spv.ResultRefused)
	if err := deliver(t, f); err == nil {
		t.Fatal("a refused object was not reported")
	}
	if len(sub.calls) != 0 {
		t.Fatal("a refused object reached the engine")
	}
	marked(t, f, beefObj, "tm_example")
}

// TestShedObjectIsStillMarked: an object the queue has no room for never
// reaches a worker, so Handle marks it.
func TestShedObjectIsStillMarked(t *testing.T) {
	sub := &markingSubmitter{stubSubmitter: stubSubmitter{steak: admitted("tm_example")}}
	f := verifyFeed(t, sub, VerifyEnforce, spv.ResultOK)
	f.Workers = 1 // never started: every delivery is shed
	if err := deliver(t, f); !errors.Is(err, ErrShed) {
		t.Fatalf("err %v, want shed", err)
	}
	marked(t, f, beefObj, "tm_example")
}

// TestQueuedObjectIsMarkedByItsWorker: through the queue, marked by the time
// the engine is asked.
func TestQueuedObjectIsMarkedByItsWorker(t *testing.T) {
	g := guard.New(time.Minute, 1024)
	topic := "tm_example"
	seen := make(chan bool, 1)
	sub := &stubSubmitterFunc{fn: func(context.Context, string, []byte) (overlay.Steak, error) {
		_, ok := g.Lookup(objfmt.ContentID(beefObj), objfmt.TopicID(topic))
		seen <- ok
		return admitted(topic), nil
	}}
	f := &Feed{Topics: NewTopicMap([]string{topic}), Submit: sub, Guard: g, SubmitRetries: -1, Workers: 2,
		VerifyMode: VerifyShadow, Verify: fixedVerifier{spv.Verdict{Result: spv.ResultOK}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = f.Start(ctx) }()
	for !f.started.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := f.Handle(ctx, objfmt.EncodeBEEFDelivery(objfmt.TopicID(topic), beefObj)); err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-seen:
		if !ok {
			t.Fatal("guard was not marked before the engine submit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the object was never submitted")
	}
}
