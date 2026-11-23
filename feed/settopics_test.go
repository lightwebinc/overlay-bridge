package feed

import (
	"context"
	"errors"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/lanes"
)

// A topic the bridge did not carry is refused; once SetTopics adds it, its
// next delivery is named and submitted, with no restart. A topic SetTopics
// drops is refused again.
func TestSetTopicsChangesWhatIsNamed(t *testing.T) {
	sub := &stubSubmitter{steak: admitted("tm_new")}
	f := newFeed(t, sub, "tm_old")
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_new"), beefObj)

	if err := f.Handle(context.Background(), rec); !errors.Is(err, lanes.ErrReject) {
		t.Fatalf("a topic not carried: err %v, want ErrReject", err)
	}
	if got := f.Stats().UnknownTopic; got != 1 {
		t.Fatalf("unknown_topic = %d, want 1", got)
	}

	f.SetTopics(NewTopicMap([]string{"tm_old", "tm_new"}))
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("after SetTopics: %v", err)
	}
	if len(sub.calls) != 1 || sub.calls[0] != "tm_new" {
		t.Fatalf("submitted to %v, want [tm_new]", sub.calls)
	}
	if _, ok := f.TopicMap()[objfmt.TopicID("tm_old")]; !ok {
		t.Fatal("TopicMap lost a topic SetTopics kept")
	}

	f.SetTopics(NewTopicMap([]string{"tm_old"}))
	if err := f.Handle(context.Background(), rec); !errors.Is(err, lanes.ErrReject) {
		t.Fatalf("a topic dropped: err %v, want ErrReject", err)
	}
	if got := f.Stats().UnknownTopic; got != 2 {
		t.Fatalf("unknown_topic = %d, want 2", got)
	}
}

// The other names a record carries are taken from the same map as its first,
// the one in use when the record is handled.
func TestSetTopicsNamesTheOtherTopicsToo(t *testing.T) {
	sub := &stubSubmitter{}
	f := newFeed(t, sub, "tm_a")
	f.SetTopics(NewTopicMap([]string{"tm_a", "tm_b"}))
	sr, err := objfmt.EncodeBEEFRecord([]string{"tm_a", "tm_b"}, beefObj)
	if err != nil {
		t.Fatal(err)
	}
	rec := objfmt.EncodeBEEFDelivery(objfmt.TopicID("tm_a"), sr)
	if err := f.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sub.calls) != 2 {
		t.Fatalf("submitted to %v, want tm_a and tm_b", sub.calls)
	}
}
