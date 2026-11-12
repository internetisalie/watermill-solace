package solace

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// Direct publishes best-effort direct messages instead of persistent
	// (guaranteed) ones. Direct messages are never spooled to queues.
	Direct bool

	// AckTimeout bounds how long a persistent Publish waits for the broker to
	// confirm each message. Zero means 30s. Ignored for Direct.
	AckTimeout time.Duration

	// BufferSize is the SDK's outbound buffer. When it is full a publish
	// fails rather than blocking. Zero means 256.
	BufferSize uint

	// Marshaler converts messages. Nil means DefaultMarshaler{}.
	Marshaler Marshaler

	// CloseTimeout bounds how long Close waits for buffered messages to drain.
	// Zero means 10s.
	CloseTimeout time.Duration
}

func (c *PublisherConfig) setDefaults() {
	if c.AckTimeout <= 0 {
		c.AckTimeout = 30 * time.Second
	}
	if c.BufferSize == 0 {
		c.BufferSize = 256
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = 10 * time.Second
	}
}

// Publisher implements message.Publisher. Persistent publishing is
// synchronous: Publish returns only after the broker has confirmed every
// message (or one of them fails), so a nil error means the messages are
// spooled.
type Publisher struct {
	conn   *Connection
	cfg    PublisherConfig
	logger watermill.LoggerAdapter

	persistent solace.PersistentMessagePublisher
	direct     solace.DirectMessagePublisher

	mu     sync.RWMutex
	closed bool
}

var _ message.Publisher = (*Publisher)(nil)

// NewPublisher starts a publisher on conn.
func NewPublisher(conn *Connection, cfg PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if conn == nil {
		return nil, errors.New("solace: NewPublisher: nil connection")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	cfg.setDefaults()
	p := &Publisher{conn: conn, cfg: cfg, logger: logger}

	svc := conn.svc
	if cfg.Direct {
		pub, err := svc.CreateDirectMessagePublisherBuilder().OnBackPressureReject(cfg.BufferSize).Build()
		if err != nil {
			return nil, fmt.Errorf("solace: building direct publisher: %w", err)
		}
		if err := pub.Start(); err != nil {
			return nil, fmt.Errorf("solace: starting direct publisher: %w", err)
		}
		p.direct = pub
	} else {
		pub, err := svc.CreatePersistentMessagePublisherBuilder().OnBackPressureReject(cfg.BufferSize).Build()
		if err != nil {
			return nil, fmt.Errorf("solace: building persistent publisher: %w", err)
		}
		if err := pub.Start(); err != nil {
			return nil, fmt.Errorf("solace: starting persistent publisher: %w", err)
		}
		p.persistent = pub
	}
	return p, nil
}

// Publish implements message.Publisher. topic must be a concrete Solace topic
// (no wildcards). Messages are sent in order; the first failure stops the
// batch and is returned.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("solace: publisher is closed")
	}

	dest := resource.TopicOf(topic)
	for _, msg := range messages {
		out, err := p.cfg.Marshaler.Marshal(topic, msg, p.conn.svc.MessageBuilder())
		if err != nil {
			return err
		}
		if p.direct != nil {
			err = p.direct.Publish(out, dest)
		} else {
			err = p.persistent.PublishAwaitAcknowledgement(out, dest, p.cfg.AckTimeout, nil)
		}
		if err != nil {
			return fmt.Errorf("solace: publishing %s to %q: %w", msg.UUID, topic, err)
		}
		p.logger.Trace("Message published", watermill.LogFields{"uuid": msg.UUID, "topic": topic})
	}
	return nil
}

// Close stops the publisher, waiting up to CloseTimeout for buffered messages
// to drain. It does not close the connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true

	var err error
	if p.direct != nil {
		err = p.direct.Terminate(p.cfg.CloseTimeout)
	} else {
		err = p.persistent.Terminate(p.cfg.CloseTimeout)
	}
	if err != nil {
		return fmt.Errorf("solace: closing publisher: %w", err)
	}
	return nil
}
