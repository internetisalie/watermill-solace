//go:build integration

package solace_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	solace "code.internetisalie.net/watermill-solace"
)

func drain(ch <-chan *message.Message) {
	for range ch {
	}
}

func TestUnsubscribeStopsAttractingButKeepsTheBacklog(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	pub := newPublisher(t, conn, solace.PublisherConfig{})
	if err := sub.SubscribeInitialize(topic); err != nil {
		t.Fatal(err)
	}
	before := message.NewMessage(watermill.NewUUID(), []byte("before"))
	publish(t, pub, topic, before)

	if err := sub.Unsubscribe(topic); err != nil { // no local consumer: short-lived receiver path
		t.Fatalf("Unsubscribe: %v", err)
	}
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("after")))

	ch, _ := subscribe(t, sub, topic)
	got := receive(t, ch)
	got.Ack()
	if got.UUID != before.UUID {
		t.Fatalf("got %q, want the message published before Unsubscribe", got.Payload)
	}
	expectNothing(t, ch, time.Second)
}

func TestUnsubscribeThroughAnActiveSubscription(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	pub := newPublisher(t, conn, solace.PublisherConfig{})
	ch, _ := subscribe(t, sub, topic)
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("1")))
	receive(t, ch).Ack()

	if err := sub.Unsubscribe(topic); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("2")))
	expectNothing(t, ch, time.Second)
}

// An exclusive queue already has an active consumer elsewhere, so the
// short-lived receiver Unsubscribe binds is only a standby.
func TestUnsubscribeExclusiveQueueFromAStandby(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	cfg := solace.SubscriberConfig{Mode: solace.DurableExclusiveQueue}
	consumer := newSubscriber(t, conn, cfg)
	admin := newSubscriber(t, conn, cfg)
	pub := newPublisher(t, conn, solace.PublisherConfig{})
	ch, _ := subscribe(t, consumer, topic)

	if err := admin.Unsubscribe(topic); err != nil {
		t.Fatalf("Unsubscribe from standby: %v", err)
	}
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), nil))
	expectNothing(t, ch, time.Second)
}

func TestUnsubscribeMissingQueueFails(t *testing.T) {
	sub := newSubscriber(t, connect(t), solace.SubscriberConfig{})
	if err := sub.Unsubscribe(uniqueTopic(t)); err == nil {
		t.Fatal("Unsubscribe of a queue that does not exist succeeded")
	}
}

func TestDeleteQueueDropsQueueAndBacklog(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	pub := newPublisher(t, conn, solace.PublisherConfig{})
	if err := sub.SubscribeInitialize(topic); err != nil {
		t.Fatal(err)
	}
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("doomed")))

	if err := sub.DeleteQueue(topic); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if err := sub.DeleteQueue(topic); err != nil {
		t.Fatalf("DeleteQueue of a missing queue: %v", err)
	}
	if err := sub.Unsubscribe(topic); err == nil {
		t.Fatal("queue still exists after DeleteQueue")
	}

	ch, _ := subscribe(t, sub, topic) // recreates an empty queue
	expectNothing(t, ch, time.Second)
}

func TestDeleteQueueRefusesWhileConsuming(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, cancel := subscribe(t, sub, topic)

	if err := sub.DeleteQueue(topic); !errors.Is(err, solace.ErrQueueInUse) {
		t.Fatalf("DeleteQueue while consuming = %v, want ErrQueueInUse", err)
	}
	cancel()
	drain(ch)
	if err := sub.DeleteQueue(topic); err != nil {
		t.Fatalf("DeleteQueue after the subscription ended: %v", err)
	}
}

func TestQueueAdminRefusesNonDurableModes(t *testing.T) {
	conn := connect(t)
	for _, cfg := range []solace.SubscriberConfig{
		{Mode: solace.TemporaryQueue},
		{Mode: solace.Direct},
		{Mode: solace.DirectShared, ShareName: "g"},
	} {
		sub := newSubscriber(t, conn, cfg)
		if err := sub.Unsubscribe("x"); !errors.Is(err, solace.ErrNotDurable) {
			t.Errorf("%v Unsubscribe = %v, want ErrNotDurable", cfg.Mode, err)
		}
		if err := sub.DeleteQueue("x"); !errors.Is(err, solace.ErrNotDurable) {
			t.Errorf("%v DeleteQueue = %v, want ErrNotDurable", cfg.Mode, err)
		}
	}
}

// SubscribeInitialize binds a receiver that may be handed the existing backlog.
// It must neither fail nor stall on it, and must leave the backlog intact.
func TestSubscribeInitializeOnAQueueWithABacklog(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	if err := sub.SubscribeInitialize(topic); err != nil {
		t.Fatal(err)
	}
	sent := message.NewMessage(watermill.NewUUID(), []byte("backlog"))
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, sent)

	start := time.Now()
	if err := sub.SubscribeInitialize(topic); err != nil {
		t.Fatalf("second SubscribeInitialize: %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("SubscribeInitialize took %v on a queue with a backlog", took)
	}

	ch, _ := subscribe(t, sub, topic)
	got := receive(t, ch)
	got.Ack()
	if got.UUID != sent.UUID {
		t.Fatalf("UUID = %q, want %q", got.UUID, sent.UUID)
	}
}
