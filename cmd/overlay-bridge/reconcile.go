package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/lightwebinc/overlay-bridge/topics"
)

// reconcileTopics compares -topics against the consumer's actual election and
// says so, loudly, at startup.
//
// WARN AND CONTINUE, never refuse. A mismatch loses one topic's objects; a
// refusal loses every topic's, and it converts a customer's self-service topic
// join into a bridge that will not start. The runtime counter and its alert
// remain the authority; this is the same fact delivered at the moment an
// operator is actually looking at the unit.
//
// When the check is not configured it says THAT too. A silent absence and a
// silent pass look identical in a log, and the whole point of this function is
// that a silent discard is what goes unnoticed.
func reconcileTopics(ctx context.Context, c config, names []string, log *slog.Logger) {
	token := c.subToken
	if token == "" {
		token = os.Getenv("OVERLAY_BRIDGE_SUBSCRIPTION_TOKEN")
	}
	if c.subURL == "" || c.subID == "" {
		log.Info("topic reconciliation is OFF",
			"why", "-subscription-url and -subscription-consumer are not both set",
			"consequence", "a topic joined after this process started will be discarded silently until the unknown_topic alert fires",
			"topics", len(names))
		return
	}

	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	sub, err := topics.Fetch(rctx, c.subURL, c.subID, token, nil)
	if err != nil {
		// Not fatal: a control-plane blip must not become a data-plane outage.
		log.Warn("topic reconciliation could not read the broker",
			"err", err, "consumer", c.subID,
			"consequence", "starting anyway with -topics as given, unreconciled")
		return
	}

	d := topics.Compare(names, sub.TopicIDs, sub.AllTopics)
	if d.OK() {
		log.Info("topics reconciled with the consumer's election",
			"consumer", c.subID, "topics", len(names), "detail", d.Describe())
		return
	}
	log.Warn("TOPIC MISMATCH: this bridge is discarding objects the plane is delivering",
		"consumer", c.subID,
		"detail", d.Describe(),
		"fix", "add the topic to -topics and restart this unit",
		"alert", "OverlayBridgeUnknownTopic")
}
