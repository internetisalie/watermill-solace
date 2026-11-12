# watermill-solace

A [Watermill](https://watermill.io) Pub/Sub for [Solace PubSub+](https://solace.com),
built on the official Go API (`solace.dev/go/messaging`).

```sh
go get code.internetisalie.net/watermill-solace
```

It moves `*message.Message` values to and from Solace topics and queues and
nothing more: no envelopes, CloudEvents or service-specific naming.

The Solace Go API wraps a C library, so building needs **cgo** and a C compiler.

## Use

```go
conn, err := solace.Connect(ctx, solace.ConnectionConfig{
    Host: "tcp://broker:55555", VPN: "default", Username: "app", Password: pw,
}, logger)
defer conn.Close()

pub, err := solace.NewPublisher(conn, solace.PublisherConfig{}, logger)
sub, err := solace.NewSubscriber(conn, solace.SubscriberConfig{
    QueueName: solace.GroupQueueName("billing"),
}, logger)
```

One `Connection` is shared by any number of publishers and subscribers, and
closing either never closes it. It reconnects on its own.

## Consumption modes

| `Mode` | Binding | Delivery |
|---|---|---|
| `DurableSharedQueue` (default) | durable non-exclusive queue | load-balanced, persistent |
| `DurableExclusiveQueue` | durable exclusive queue | one active consumer, others stand by |
| `TemporaryQueue` | non-durable exclusive queue | per-subscriber fan-out while subscribed |
| `Direct` | direct topic subscription | fan-out, not persistent |
| `DirectShared` | `#share/<ShareName>/<topic>` | load-balanced, not persistent |

Queue names come from `SubscriberConfig.QueueName`, a `QueueNamer`
(`func(topic string) string`). The default, `TopicQueueName`, names the queue
after the topic with `*` and `>` encoded as `_STR_` and `_GT_`.
`GroupQueueName(group)` gives `<group>/<topic>`, and `GroupQueueNameWith` takes
your own topic encoding. Any function will do.

## Delivery semantics

- Each `Subscribe` delivers **sequentially**: the next message waits for the
  previous one's Ack or Nack. Scale out with more subscriptions on a shared queue.
- On queues, Ack settles ACCEPTED and Nack settles FAILED, so the broker
  redelivers under the queue's own redelivery and dead-message-queue settings. A
  message unsettled when its subscription ends stays on the queue.
- Direct modes have no persistence: Ack is a no-op and Nack redelivers locally.
- Metadata travels as Solace user properties and the UUID as the application
  message ID. Solace-specific facts are on the message context
  (`InboundMessageFromCtx`, `RedeliveredFromCtx`).

## Durable queues outlive their subscriptions

Ending a subscription removes only the consumer; the queue keeps attracting
messages, so a restart loses nothing. Retiring a topic is explicit:
`Subscriber.Unsubscribe(topic)` stops the queue attracting it and keeps the
backlog, and `Subscriber.DeleteQueue(topic)` deletes the queue and its backlog.

## Tests

```sh
go test ./...                                   # unit tests
go test -tags integration -race ./...           # against a real broker
```

Integration tests start `solace/solace-pubsub-standard` through
[`code.internetisalie.net/containers`](https://code.internetisalie.net/containers),
so they need Docker. The broker wants 4 GB of memory, 1 GB of `/dev/shm` and a
`nofile` hard limit of 1048576, and takes about 35 s to start. They include
Watermill's universal conformance suite. `TestConformanceReconnect` restarts the
broker mid-stream and is skipped under `-race`, because Watermill v1.5.3's own
`TestReconnect` has a data race. Set `SOLACE_TEST_DEBUG=1` for verbose logs.

## Releases

Same as `containers`: `vMAJOR.MINOR.PATCH` tags cut from `main` with
`glimmer git tag`; the tag runs `.gitea/workflows/release.yml`, which publishes a
source-only Gitea release.
