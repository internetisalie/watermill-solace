package solace

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	solmsg "solace.dev/go/messaging/pkg/solace/message"
)

// Marshaler converts between Watermill messages and Solace messages.
type Marshaler interface {
	// Marshal builds the outbound Solace message for msg. The builder is a
	// fresh one from the connection; the topic is informational.
	Marshal(topic string, msg *message.Message, builder solace.OutboundMessageBuilder) (solmsg.OutboundMessage, error)
	// Unmarshal converts an inbound Solace message.
	Unmarshal(in solmsg.InboundMessage) (*message.Message, error)
}

// reservedPropertyPrefix is the namespace the SDK reads as configuration, not
// as user properties. Metadata keys in it would be misinterpreted.
const reservedPropertyPrefix = "solace.messaging."

// DefaultMarshaler carries the UUID as the application message ID, metadata as
// string user properties, and the payload as a binary attachment.
type DefaultMarshaler struct {
	// PartitionKeyMetadata, when set, names a metadata key whose value is also
	// written as the queue partition key (JMSXGroupID), so partitioned queues
	// keep messages with the same key in order. Empty disables it.
	PartitionKeyMetadata string
}

// Marshal implements Marshaler.
func (m DefaultMarshaler) Marshal(_ string, msg *message.Message, b solace.OutboundMessageBuilder) (solmsg.OutboundMessage, error) {
	b = b.WithApplicationMessageID(msg.UUID)
	for k, v := range msg.Metadata {
		if strings.HasPrefix(k, reservedPropertyPrefix) {
			return nil, fmt.Errorf("solace: metadata key %q uses the reserved %q prefix", k, reservedPropertyPrefix)
		}
		b = b.WithProperty(config.MessageProperty(k), v)
	}
	if m.PartitionKeyMetadata != "" {
		if key := msg.Metadata.Get(m.PartitionKeyMetadata); key != "" {
			b = b.WithProperty(config.MessageProperty(config.QueuePartitionKey), key)
		}
	}
	out, err := b.BuildWithByteArrayPayload(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("solace: building message %s: %w", msg.UUID, err)
	}
	return out, nil
}

// Unmarshal implements Marshaler. Non-string user properties (set by
// non-Watermill publishers) are formatted with %v. A message without an
// application message ID gets a fresh UUID.
func (DefaultMarshaler) Unmarshal(in solmsg.InboundMessage) (*message.Message, error) {
	id, ok := in.GetApplicationMessageID()
	if !ok || id == "" {
		id = watermill.NewUUID()
	}
	payload, _ := in.GetPayloadAsBytes()
	if payload == nil {
		// Publishers outside Watermill commonly send text attachments.
		if s, ok := in.GetPayloadAsString(); ok {
			payload = []byte(s)
		}
	}
	msg := message.NewMessage(id, payload)
	for k, v := range in.GetProperties() {
		if k == config.QueuePartitionKey {
			continue // transport detail, re-derived from metadata on publish
		}
		if s, ok := v.(string); ok {
			msg.Metadata.Set(k, s)
		} else {
			msg.Metadata.Set(k, fmt.Sprint(v))
		}
	}
	return msg, nil
}

type ctxKey int

const inboundKey ctxKey = iota

func withInbound(ctx context.Context, in solmsg.InboundMessage) context.Context {
	return context.WithValue(ctx, inboundKey, in)
}

// InboundMessageFromCtx returns the raw Solace message a Watermill message was
// built from, for fields this package does not surface (sender timestamp,
// replication group message ID, priority, ...). Use msg.Context().
//
// Only read it while handling the message: the SDK owns it and frees it after
// settlement.
func InboundMessageFromCtx(ctx context.Context) (solmsg.InboundMessage, bool) {
	in, ok := ctx.Value(inboundKey).(solmsg.InboundMessage)
	return in, ok
}

// RedeliveredFromCtx reports whether the broker flagged the message as a
// redelivery. Solace exposes only this flag, not a delivery count.
func RedeliveredFromCtx(ctx context.Context) bool {
	in, ok := InboundMessageFromCtx(ctx)
	return ok && in.IsRedelivered()
}
