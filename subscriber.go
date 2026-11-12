package solace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	solmsg "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ConsumptionMode selects how a Subscriber binds to the broker.
type ConsumptionMode int

const (
	// DurableSharedQueue consumes from a durable non-exclusive queue that
	// subscribes to the topic. Every subscription on the same queue name
	// shares the stream (a consumer group); messages are spooled while nobody
	// is subscribed. This is the default.
	DurableSharedQueue ConsumptionMode = iota

	// DurableExclusiveQueue consumes from a durable exclusive queue. Only one
	// subscription receives at a time; others on the same queue name are hot
	// standbys that take over when it closes. Order is preserved across
	// fail-over.
	DurableExclusiveQueue

	// TemporaryQueue consumes from a non-durable exclusive queue that the
	// broker deletes once the subscription ends. Each subscription receives
	// every message published while it is open (fan-out), with guaranteed
	// delivery while it lasts.
	TemporaryQueue

	// Direct receives direct (non-persistent) messages on the topic. Each
	// subscription receives every message (fan-out); nothing is spooled.
	Direct

	// DirectShared receives direct messages through a shared subscription
	// ("#share/<ShareName>/<topic>"): subscriptions with the same ShareName
	// split the stream. Nothing is spooled.
	DirectShared
)

func (m ConsumptionMode) String() string {
	switch m {
	case DurableSharedQueue:
		return "DurableSharedQueue"
	case DurableExclusiveQueue:
		return "DurableExclusiveQueue"
	case TemporaryQueue:
		return "TemporaryQueue"
	case Direct:
		return "Direct"
	case DirectShared:
		return "DirectShared"
	}
	return fmt.Sprintf("ConsumptionMode(%d)", int(m))
}

func (m ConsumptionMode) queued() bool {
	return m == DurableSharedQueue || m == DurableExclusiveQueue || m == TemporaryQueue
}

// QueueNamer derives the queue name for a subscribed topic.
type QueueNamer func(topic string) string

// SanitizeTopic makes a topic subscription usable inside a queue name by
// replacing the wildcard characters queue names may not contain.
func SanitizeTopic(topic string) string {
	return strings.NewReplacer("*", "_STR_", ">", "_GT_").Replace(topic)
}

// TopicQueueName names the queue after the (sanitized) topic, so every
// subscriber of a topic shares one queue.
func TopicQueueName(topic string) string { return SanitizeTopic(topic) }

// GroupQueueName returns a namer for a consumer group: "<group>/<topic>".
// Subscribers in the same group share a queue per topic; different groups each
// get every message.
func GroupQueueName(group string) QueueNamer {
	return GroupQueueNameWith(group, SanitizeTopic)
}

// GroupQueueNameWith is GroupQueueName with the topic encoding supplied:
// "<group>/<sanitize(topic)>". Use it to keep group semantics under a naming
// scheme of your own, such as one an existing deployment's queues already use.
func GroupQueueNameWith(group string, sanitize func(topic string) string) QueueNamer {
	return func(topic string) string { return group + "/" + sanitize(topic) }
}

// SubscriberConfig configures a Subscriber.
type SubscriberConfig struct {
	// Mode selects the binding. The zero value is DurableSharedQueue.
	Mode ConsumptionMode

	// QueueName names the queue for queue modes. Nil means TopicQueueName.
	// For TemporaryQueue, nil means a broker-generated name.
	QueueName QueueNamer

	// ShareName is the share group for DirectShared. Required for that mode.
	ShareName string

	// DoNotCreateQueues requires durable queues to exist already instead of
	// creating them on subscribe. Set it where queues are provisioned by
	// operations with their own redelivery and dead-message-queue settings.
	DoNotCreateQueues bool

	// Marshaler converts messages. Nil means DefaultMarshaler{}.
	Marshaler Marshaler
}

func (c *SubscriberConfig) setDefaults() {
	if c.QueueName == nil && c.Mode != TemporaryQueue {
		c.QueueName = TopicQueueName
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

func (c SubscriberConfig) validate() error {
	switch c.Mode {
	case DurableSharedQueue, DurableExclusiveQueue, TemporaryQueue, Direct:
	case DirectShared:
		if c.ShareName == "" {
			return errors.New("solace: SubscriberConfig.ShareName is required for DirectShared")
		}
	default:
		return fmt.Errorf("solace: unknown consumption mode %v", c.Mode)
	}
	return nil
}

func (c SubscriberConfig) queue(topic string) *resource.Queue {
	switch c.Mode {
	case DurableExclusiveQueue:
		return resource.QueueDurableExclusive(c.QueueName(topic))
	case TemporaryQueue:
		if c.QueueName == nil {
			return resource.QueueNonDurableExclusiveAnonymous()
		}
		return resource.QueueNonDurableExclusive(c.QueueName(topic))
	default:
		return resource.QueueDurableNonExclusive(c.QueueName(topic))
	}
}

// Subscriber implements message.Subscriber and message.SubscribeInitializer.
type Subscriber struct {
	conn   *Connection
	cfg    SubscriberConfig
	logger watermill.LoggerAdapter

	mu      sync.Mutex
	closed  bool
	closing chan struct{}
	subs    sync.WaitGroup
	// active holds the running queue receivers per subscribed topic, so
	// Unsubscribe can act through one and DeleteQueue can refuse while one runs.
	active map[string][]solace.PersistentMessageReceiver
}

var (
	_ message.Subscriber           = (*Subscriber)(nil)
	_ message.SubscribeInitializer = (*Subscriber)(nil)
)

// NewSubscriber creates a subscriber on conn. No broker resources are touched
// until Subscribe or SubscribeInitialize.
func NewSubscriber(conn *Connection, cfg SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if conn == nil {
		return nil, errors.New("solace: NewSubscriber: nil connection")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	cfg.setDefaults()
	return &Subscriber{
		conn:    conn,
		cfg:     cfg,
		logger:  logger,
		closing: make(chan struct{}),
		active:  map[string][]solace.PersistentMessageReceiver{},
	}, nil
}

// receiver is the part of the SDK's direct and persistent receivers this
// package needs.
type receiver interface {
	Start() error
	ReceiveAsync(solace.MessageHandler) error
	Terminate(time.Duration) error
}

func (s *Subscriber) buildReceiver(topic string) (receiver, solace.PersistentMessageReceiver, error) {
	svc := s.conn.svc
	sub := resource.TopicSubscriptionOf(topic)

	if s.cfg.Mode.queued() {
		b := svc.CreatePersistentMessageReceiverBuilder().
			WithMessageClientAcknowledgement().
			WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome).
			WithSubscriptions(sub)
		if !s.cfg.DoNotCreateQueues {
			b = b.WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources)
		}
		r, err := b.Build(s.cfg.queue(topic))
		if err != nil {
			return nil, nil, fmt.Errorf("solace: building queue receiver for %q: %w", topic, err)
		}
		return r, r, nil
	}

	b := svc.CreateDirectMessageReceiverBuilder().WithSubscriptions(sub)
	var (
		r   solace.DirectMessageReceiver
		err error
	)
	if s.cfg.Mode == DirectShared {
		r, err = b.BuildWithShareName(resource.ShareNameOf(s.cfg.ShareName))
	} else {
		r, err = b.Build()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("solace: building direct receiver for %q: %w", topic, err)
	}
	return r, nil, nil
}

// SubscribeInitialize implements message.SubscribeInitializer: for durable
// queue modes it creates the queue and its topic subscription without
// consuming, so messages published before the first Subscribe are kept. It is
// a no-op for other modes and when DoNotCreateQueues is set.
func (s *Subscriber) SubscribeInitialize(topic string) error {
	if s.cfg.DoNotCreateQueues || (s.cfg.Mode != DurableSharedQueue && s.cfg.Mode != DurableExclusiveQueue) {
		return nil
	}
	r, _, err := s.buildReceiver(topic)
	if err != nil {
		return err
	}
	if err := r.Start(); err != nil {
		return fmt.Errorf("solace: provisioning queue for %q: %w", topic, err)
	}
	if err := releaseIdle(r); err != nil {
		return fmt.Errorf("solace: releasing provisioning receiver for %q: %w", topic, err)
	}
	return nil
}

// Subscribe implements message.Subscriber. The returned channel is closed when
// ctx is done or the Subscriber is closed.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("solace: subscriber is closed")
	}
	s.subs.Add(1)
	s.mu.Unlock()

	r, persistent, err := s.buildReceiver(topic)
	if err != nil {
		s.subs.Done()
		return nil, err
	}
	if err := r.Start(); err != nil {
		s.subs.Done()
		return nil, fmt.Errorf("solace: starting %v receiver for %q: %w", s.cfg.Mode, topic, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ctx:        ctx,
		out:        make(chan *message.Message),
		done:       make(chan struct{}),
		persistent: persistent,
		marshaler:  s.cfg.Marshaler,
		logger:     s.logger.With(watermill.LogFields{"topic": topic, "mode": s.cfg.Mode.String()}),
	}
	if err := r.ReceiveAsync(sub.handle); err != nil {
		cancel()
		_ = releaseIdle(r)
		s.subs.Done()
		return nil, fmt.Errorf("solace: registering handler for %q: %w", topic, err)
	}

	if persistent != nil {
		s.track(topic, persistent)
	}
	go func() {
		defer s.subs.Done()
		select {
		case <-ctx.Done():
		case <-s.closing:
		}
		cancel()
		if persistent != nil {
			s.untrack(topic, persistent)
		}
		sub.shutdown(r)
	}()
	return sub.out, nil
}

// Close ends every subscription, waits for their channels to close, and
// releases receivers. It does not close the connection.
func (s *Subscriber) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.closing)
	s.mu.Unlock()

	s.subs.Wait()
	return nil
}

// subscription delivers one receiver's messages to one output channel, one at
// a time.
type subscription struct {
	ctx        context.Context
	out        chan *message.Message
	persistent solace.PersistentMessageReceiver // nil for direct modes
	marshaler  Marshaler
	logger     watermill.LoggerAdapter

	mu       sync.Mutex
	closed   bool
	done     chan struct{}
	inflight sync.WaitGroup
}

// enter registers an SDK callback as in flight, unless the subscription is
// shutting down.
func (s *subscription) enter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.inflight.Add(1)
	return true
}

// handle is the SDK callback. Blocking here until the message is settled is
// what makes delivery sequential: the SDK does not dispatch the next message
// for this receiver until the callback returns.
func (s *subscription) handle(in solmsg.InboundMessage) {
	if !s.enter() {
		return // shutting down: leave it unsettled so the broker redelivers it
	}
	defer s.inflight.Done()

	for {
		msg, err := s.marshaler.Unmarshal(in)
		if err != nil {
			s.logger.Error("Cannot unmarshal message, rejecting it", err, nil)
			s.settle(in, config.PersistentReceiverRejectedOutcome)
			return
		}
		msgCtx, cancel := context.WithCancel(withInbound(s.ctx, in))
		msg.SetContext(msgCtx)

		select {
		case s.out <- msg:
		case <-s.done:
			cancel()
			return
		}

		select {
		case <-msg.Acked():
			cancel()
			s.settle(in, config.PersistentReceiverAcceptedOutcome)
			return
		case <-msg.Nacked():
			cancel()
			if s.persistent != nil {
				s.settle(in, config.PersistentReceiverFailedOutcome)
				return
			}
			// Direct: nothing to settle; hand the same message out again.
			s.logger.Trace("Message nacked, redelivering locally", watermill.LogFields{"uuid": msg.UUID})
		case <-s.done:
			cancel()
			return
		}
	}
}

func (s *subscription) settle(in solmsg.InboundMessage, outcome config.MessageSettlementOutcome) {
	if s.persistent == nil {
		return
	}
	if err := s.persistent.Settle(in, outcome); err != nil {
		s.logger.Error("Cannot settle message", err, watermill.LogFields{"outcome": string(outcome)})
	}
}

// shutdown stops delivery, waits for in-flight callbacks to return, terminates
// the receiver and closes the output channel. Messages handed out but not yet
// settled stay unsettled and are redelivered by the broker.
func (s *subscription) shutdown(r receiver) {
	s.mu.Lock()
	s.closed = true
	close(s.done)
	s.mu.Unlock()

	s.inflight.Wait()
	// Delivery has stopped, so buffered messages can only be left unsettled
	// for the broker to redeliver; there is nothing to wait for.
	if err := r.Terminate(0); err != nil {
		if isUndelivered(err) {
			s.logger.Debug("Receiver terminated with unsettled messages; the broker redelivers them", watermill.LogFields{"cause": err.Error()})
		} else {
			s.logger.Error("Cannot terminate receiver", err, nil)
		}
	}
	close(s.out)
}
