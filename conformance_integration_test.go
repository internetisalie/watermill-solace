//go:build integration

package solace_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	sdk "solace.dev/go/messaging/pkg/solace"

	solace "code.internetisalie.net/watermill-solace"
)

// TestConformance runs Watermill's universal Pub/Sub suite against the default
// mode (durable shared queues): every implementation is expected to pass it.
// TestReconnect is skipped here (no RestartServiceCommand) and run on its own
// by TestConformanceReconnect, because it restarts the shared broker.
func TestConformance(t *testing.T) {
	conn := suiteConnection(t)
	pubSub := newPubSubConstructor(conn)
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups: true,
			// A shared queue load-balances, so order holds per subscriber only.
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return pubSub(t, solace.SubscriberConfig{})
		},
		func(t *testing.T, group string) (message.Publisher, message.Subscriber) {
			return pubSub(t, solace.SubscriberConfig{QueueName: solace.GroupQueueName(group)})
		},
	)
}

// TestConformanceReconnect runs Watermill's TestReconnect, which restarts the
// broker while 100 publishers are mid-flight and expects every message through.
// It restarts with plain `docker restart`: the test container's host port is
// bound explicitly, so the broker comes back at the address the connection
// holds. It also checks that the connection really dropped and recovered, so a
// restart that never happened cannot pass.
//
// Skipped under -race: Watermill v1.5.3's TestReconnect races on its own
// publishedMessages slice (test_pubsub.go:1086 vs :1120), outside this package.
func TestConformanceReconnect(t *testing.T) {
	if raceEnabled {
		t.Skip("watermill v1.5.3 TestReconnect has a data race in its own test code")
	}
	conn := suiteConnection(t)
	pubSub := newPubSubConstructor(conn)

	// The SDK's own events, not IsConnected sampling: a poll can miss an outage.
	var attempts, reconnects atomic.Int32
	svc := conn.Service()
	attemptID := svc.AddReconnectionAttemptListener(func(sdk.ServiceEvent) { attempts.Add(1) })
	reconnectID := svc.AddReconnectionListener(func(sdk.ServiceEvent) { reconnects.Add(1) })
	defer svc.RemoveReconnectionAttemptListener(attemptID)
	defer svc.RemoveReconnectionListener(reconnectID)

	features := tests.Features{
		ConsumerGroups:                      true,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RestartServiceCommand:               []string{"docker", "restart", brokerName},
	}
	t.Run("TestReconnect", func(t *testing.T) {
		tests.TestReconnect(t, tests.TestContext{
			TestID:   tests.NewTestID(),
			Features: features,
		}, func(t *testing.T) (message.Publisher, message.Subscriber) {
			return pubSub(t, solace.SubscriberConfig{})
		})
	})

	if attempts.Load() == 0 {
		t.Fatal("the connection never dropped: the broker was not restarted")
	}
	if reconnects.Load() == 0 || !conn.IsConnected() {
		t.Fatalf("the connection did not recover (%d attempts, %d reconnects)", attempts.Load(), reconnects.Load())
	}
}

// suiteConnection is one connection shared by a whole suite: its tests run in
// parallel and the broker caps connections.
func suiteConnection(t *testing.T) *solace.Connection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := solace.Connect(ctx, connConfig(brokerHost), logger())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newPubSubConstructor(conn *solace.Connection) func(*testing.T, solace.SubscriberConfig) (message.Publisher, message.Subscriber) {
	return func(t *testing.T, cfg solace.SubscriberConfig) (message.Publisher, message.Subscriber) {
		pub, err := solace.NewPublisher(conn, solace.PublisherConfig{}, logger())
		if err != nil {
			t.Fatal(err)
		}
		sub, err := solace.NewSubscriber(conn, cfg, logger())
		if err != nil {
			t.Fatal(err)
		}
		return pub, sub
	}
}
