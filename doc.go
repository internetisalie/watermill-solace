// Package solace is a Watermill Pub/Sub backed by Solace PubSub+ via the
// official Go API (solace.dev/go/messaging).
//
// It is deliberately generic: it moves *message.Message values to and from
// Solace topics and queues and knows nothing about CloudEvents, envelopes or
// any particular service's naming conventions. Those belong in a layer above.
//
// # Shape
//
// One [Connection] wraps one Solace MessagingService and is shared by any
// number of publishers and subscribers. Closing a [Publisher] or [Subscriber]
// never closes the connection; the caller that created it does.
//
//	conn, _ := solace.Connect(ctx, solace.ConnectionConfig{Host: "tcp://broker:55555", VPN: "default"}, logger)
//	defer conn.Close()
//	pub, _ := solace.NewPublisher(conn, solace.PublisherConfig{}, logger)
//	sub, _ := solace.NewSubscriber(conn, solace.SubscriberConfig{Mode: solace.DurableSharedQueue, QueueName: solace.GroupQueueName("billing")}, logger)
//
// # Delivery
//
// Each Subscribe call delivers sequentially: the next message is not handed
// out until the previous one is acked or nacked. Effective concurrency is
// therefore one in-flight message per subscription, which matches the
// at-least-once, in-order behaviour of a single Solace flow. Run more
// subscriptions (on a shared queue) to scale out.
//
// For queue modes, Ack settles the message as ACCEPTED (removed from the
// queue) and Nack settles it as FAILED, so the broker redelivers it subject to
// the queue's own max-redelivery and dead-message-queue configuration. A
// message that is neither acked nor nacked when its subscription closes stays
// on the queue and is redelivered to the next consumer.
//
// Direct modes have no broker-side persistence: Ack is a no-op and Nack
// redelivers the same message locally, as Watermill's GoChannel does.
//
// # Durable queues outlive their subscriptions
//
// Subscribe and SubscribeInitialize create a durable queue and subscribe it to
// the topic. Ending the subscription removes only the consumer: the queue keeps
// attracting and spooling messages, so a restart loses nothing. Retiring a
// topic for good is an explicit act: [Subscriber.Unsubscribe] stops the queue
// attracting the topic and keeps its backlog; [Subscriber.DeleteQueue] deletes
// the queue and its backlog. Both act on the broker, for every consumer.
//
// # Metadata
//
// Message metadata travels as Solace user properties and the Watermill UUID
// as the application message ID. Solace-specific facts about an inbound
// message (redelivered flag, sender timestamp, the raw SDK message) are
// exposed through the message context rather than injected into metadata;
// see [InboundMessageFromCtx] and [RedeliveredFromCtx].
package solace
