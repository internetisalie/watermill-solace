//go:build integration

package solace_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	solace "code.internetisalie.net/watermill-solace"
)

// uniqueTopic returns a topic no other test or run uses, so durable queues left
// on a shared broker can never feed one test another's messages.
func uniqueTopic(t *testing.T) string {
	return "wmtest/" + t.Name() + "/" + watermill.NewShortUUID()
}

func newPublisher(t *testing.T, conn *solace.Connection, cfg solace.PublisherConfig) *solace.Publisher {
	t.Helper()
	pub, err := solace.NewPublisher(conn, cfg, logger())
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

func newSubscriber(t *testing.T, conn *solace.Connection, cfg solace.SubscriberConfig) *solace.Subscriber {
	t.Helper()
	sub, err := solace.NewSubscriber(conn, cfg, logger())
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func subscribe(t *testing.T, sub *solace.Subscriber, topic string) (<-chan *message.Message, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := sub.Subscribe(ctx, topic)
	if err != nil {
		cancel()
		t.Fatalf("Subscribe(%q): %v", topic, err)
	}
	t.Cleanup(cancel)
	return ch, cancel
}

func receive(t *testing.T, ch <-chan *message.Message) *message.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("subscription channel closed before a message arrived")
		}
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
	return nil
}

func expectNothing(t *testing.T, ch <-chan *message.Message, wait time.Duration) {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if ok {
			t.Fatalf("unexpected message %s (%q)", msg.UUID, msg.Payload)
		}
	case <-time.After(wait):
	}
}

func publish(t *testing.T, pub message.Publisher, topic string, msgs ...*message.Message) {
	t.Helper()
	if err := pub.Publish(topic, msgs...); err != nil {
		t.Fatalf("Publish(%q): %v", topic, err)
	}
}

func TestRoundTripPreservesUUIDMetadataAndPayload(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, _ := subscribe(t, sub, topic)

	sent := message.NewMessage(watermill.NewUUID(), []byte(`{"hello":"world"}`))
	sent.Metadata.Set("type", "com.example.greeting")
	sent.Metadata.Set("tenantid", "t-1")
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, sent)

	got := receive(t, ch)
	defer got.Ack()

	if got.UUID != sent.UUID {
		t.Errorf("UUID = %q, want %q", got.UUID, sent.UUID)
	}
	if string(got.Payload) != string(sent.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, sent.Payload)
	}
	for k, v := range sent.Metadata {
		if got.Metadata.Get(k) != v {
			t.Errorf("Metadata[%q] = %q, want %q", k, got.Metadata.Get(k), v)
		}
	}
	if solace.RedeliveredFromCtx(got.Context()) {
		t.Error("first delivery reported as redelivered")
	}
	if in, ok := solace.InboundMessageFromCtx(got.Context()); !ok || in.GetDestinationName() != topic {
		t.Errorf("inbound message in ctx: ok=%v", ok)
	}
}

func TestNackIsRedeliveredByTheBrokerFlaggedAsRedelivery(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, _ := subscribe(t, sub, topic)
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, message.NewMessage(watermill.NewUUID(), []byte("x")))

	first := receive(t, ch)
	if solace.RedeliveredFromCtx(first.Context()) {
		t.Fatal("first delivery reported as redelivered")
	}
	first.Nack()

	second := receive(t, ch)
	if second.UUID != first.UUID {
		t.Fatalf("redelivered UUID = %q, want %q", second.UUID, first.UUID)
	}
	if !solace.RedeliveredFromCtx(second.Context()) {
		t.Error("broker redelivery not flagged as redelivered")
	}
	second.Ack()
	expectNothing(t, ch, time.Second)
}

func TestUnsettledMessageSurvivesSubscriptionEnd(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, cancel := subscribe(t, sub, topic)
	sent := message.NewMessage(watermill.NewUUID(), []byte("x"))
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, sent)

	_ = receive(t, ch) // neither acked nor nacked
	cancel()
	for range ch { // channel must close once the subscription ends
	}

	ch2, _ := subscribe(t, sub, topic)
	got := receive(t, ch2)
	got.Ack()
	if got.UUID != sent.UUID {
		t.Fatalf("UUID = %q, want %q", got.UUID, sent.UUID)
	}
}

func TestAckedMessageIsNotRedelivered(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, cancel := subscribe(t, sub, topic)
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, message.NewMessage(watermill.NewUUID(), []byte("x")))
	receive(t, ch).Ack()
	cancel()
	for range ch {
	}

	ch2, _ := subscribe(t, sub, topic)
	expectNothing(t, ch2, 2*time.Second)
}

func TestSubscribeInitializeKeepsMessagesPublishedBeforeSubscribe(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	if err := sub.SubscribeInitialize(topic); err != nil {
		t.Fatalf("SubscribeInitialize: %v", err)
	}
	sent := message.NewMessage(watermill.NewUUID(), []byte("early"))
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, sent)

	ch, _ := subscribe(t, sub, topic)
	got := receive(t, ch)
	got.Ack()
	if got.UUID != sent.UUID {
		t.Fatalf("UUID = %q, want %q", got.UUID, sent.UUID)
	}
}

func TestDeliveryIsSequentialAndOrdered(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, _ := subscribe(t, sub, topic)

	var sent []*message.Message
	for i := 0; i < 20; i++ {
		sent = append(sent, message.NewMessage(watermill.NewUUID(), []byte{byte(i)}))
	}
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, sent...)

	for i, want := range sent {
		got := receive(t, ch)
		// The next message must not be handed out while this one is unsettled.
		expectNothing(t, ch, 50*time.Millisecond)
		got.Ack()
		if got.UUID != want.UUID {
			t.Fatalf("message %d: UUID = %q, want %q", i, got.UUID, want.UUID)
		}
	}
}

func TestSharedQueueSplitsAndGroupsEachGetEverything(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	group := func(name string) *solace.Subscriber {
		return newSubscriber(t, conn, solace.SubscriberConfig{QueueName: solace.GroupQueueName(name + watermill.NewShortUUID())})
	}
	a, b := group("a"), group("b")
	for _, s := range []*solace.Subscriber{a, b} {
		if err := s.SubscribeInitialize(topic); err != nil {
			t.Fatal(err)
		}
	}
	a1, _ := subscribe(t, a, topic)
	a2, _ := subscribe(t, a, topic)
	b1, _ := subscribe(t, b, topic)

	const n = 20
	for i := 0; i < n; i++ {
		publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, message.NewMessage(watermill.NewUUID(), nil))
	}

	countA := map[int]int{}
	for got := 0; got < n; {
		select {
		case m := <-a1:
			m.Ack()
			countA[1]++
			got++
		case m := <-a2:
			m.Ack()
			countA[2]++
			got++
		case <-time.After(10 * time.Second):
			t.Fatalf("group a received %d of %d", got, n)
		}
	}
	if countA[1] == 0 || countA[2] == 0 {
		t.Errorf("group a was not load-balanced: %v", countA)
	}
	for i := 0; i < n; i++ {
		receive(t, b1).Ack()
	}
	expectNothing(t, a1, 500*time.Millisecond)
}

func TestDurableExclusiveQueueFailsOverToStandby(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{Mode: solace.DurableExclusiveQueue})
	c0, cancel0 := subscribe(t, sub, topic)
	c1, cancel1 := subscribe(t, sub, topic)
	chans := []<-chan *message.Message{c0, c1}
	cancels := []context.CancelFunc{cancel0, cancel1}
	pub := newPublisher(t, conn, solace.PublisherConfig{})

	// Whichever subscription the broker made active gets every message.
	var active int
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("1")))
	select {
	case m := <-c0:
		m.Ack()
		active = 0
	case m := <-c1:
		m.Ack()
		active = 1
	case <-time.After(10 * time.Second):
		t.Fatal("no subscription received the first message")
	}
	standby := 1 - active
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("2")))
	receive(t, chans[active]).Ack()
	expectNothing(t, chans[standby], 500*time.Millisecond)

	// End the active subscription; the standby must take over.
	cancels[active]()
	publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), []byte("3")))
	if got := receive(t, chans[standby]); string(got.Payload) != "3" {
		t.Fatalf("standby received %q, want %q", got.Payload, "3")
	} else {
		got.Ack()
	}
}

func TestTemporaryQueueFansOutToEverySubscription(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{Mode: solace.TemporaryQueue})
	c1, _ := subscribe(t, sub, topic)
	c2, _ := subscribe(t, sub, topic)
	sent := message.NewMessage(watermill.NewUUID(), nil)
	publish(t, newPublisher(t, conn, solace.PublisherConfig{}), topic, sent)
	for _, ch := range []<-chan *message.Message{c1, c2} {
		got := receive(t, ch)
		got.Ack()
		if got.UUID != sent.UUID {
			t.Fatalf("UUID = %q, want %q", got.UUID, sent.UUID)
		}
	}
}

func TestDirectFansOutAndNackRedeliversLocally(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{Mode: solace.Direct})
	c1, _ := subscribe(t, sub, topic)
	c2, _ := subscribe(t, sub, topic)
	sent := message.NewMessage(watermill.NewUUID(), []byte("d"))
	sent.Metadata.Set("k", "v")
	publish(t, newPublisher(t, conn, solace.PublisherConfig{Direct: true}), topic, sent)

	receive(t, c1).Ack()
	m := receive(t, c2)
	m.Nack()
	again := receive(t, c2)
	again.Ack()
	if again.UUID != sent.UUID || again.Metadata.Get("k") != "v" {
		t.Fatalf("local redelivery lost identity: uuid %q metadata %v", again.UUID, again.Metadata)
	}
}

func TestDirectSharedSplitsTheStream(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{Mode: solace.DirectShared, ShareName: "g" + watermill.NewShortUUID()})
	c1, _ := subscribe(t, sub, topic)
	c2, _ := subscribe(t, sub, topic)
	time.Sleep(200 * time.Millisecond) // direct subscriptions are not acknowledged by the broker synchronously

	pub := newPublisher(t, conn, solace.PublisherConfig{Direct: true})
	const n = 40
	for i := 0; i < n; i++ {
		publish(t, pub, topic, message.NewMessage(watermill.NewUUID(), nil))
	}
	counts := [2]int{}
	for got := 0; got < n; got++ {
		select {
		case m := <-c1:
			m.Ack()
			counts[0]++
		case m := <-c2:
			m.Ack()
			counts[1]++
		case <-time.After(10 * time.Second):
			t.Fatalf("received %d of %d (%v)", got, n, counts)
		}
	}
	if counts[0] == 0 || counts[1] == 0 {
		t.Errorf("share group was not load-balanced: %v", counts)
	}
}

func TestPartitionKeyIsCarriedAsGroupID(t *testing.T) {
	conn := connect(t)
	topic := uniqueTopic(t)
	sub := newSubscriber(t, conn, solace.SubscriberConfig{})
	ch, _ := subscribe(t, sub, topic)
	pub := newPublisher(t, conn, solace.PublisherConfig{Marshaler: solace.DefaultMarshaler{PartitionKeyMetadata: "tenantid"}})
	sent := message.NewMessage(watermill.NewUUID(), nil)
	sent.Metadata.Set("tenantid", "t-42")
	publish(t, pub, topic, sent)

	got := receive(t, ch)
	defer got.Ack()
	in, _ := solace.InboundMessageFromCtx(got.Context())
	if v, ok := in.GetProperty("JMSXGroupID"); !ok || v != "t-42" {
		t.Errorf("JMSXGroupID = %v (ok=%v), want t-42", v, ok)
	}
	if _, ok := got.Metadata["JMSXGroupID"]; ok {
		t.Error("partition key leaked into metadata")
	}
}

func TestClosedSubscriberClosesChannelsAndRefusesSubscribe(t *testing.T) {
	conn := connect(t)
	sub, err := solace.NewSubscriber(conn, solace.SubscriberConfig{}, logger())
	if err != nil {
		t.Fatal(err)
	}
	ch, err := sub.Subscribe(context.Background(), uniqueTopic(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("channel delivered after Close")
	}
	if _, err := sub.Subscribe(context.Background(), uniqueTopic(t)); err == nil {
		t.Fatal("Subscribe after Close succeeded")
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
