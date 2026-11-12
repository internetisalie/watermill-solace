package solace

import (
	"errors"
	"fmt"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ErrNotDurable is returned by Unsubscribe and DeleteQueue for consumption
// modes that leave nothing behind on the broker: direct subscriptions end with
// their receiver and temporary queues are deleted by the broker.
var ErrNotDurable = errors.New("solace: not a durable queue mode; nothing outlives the subscription")

// ErrQueueInUse is returned by DeleteQueue while this Subscriber is still
// consuming from the queue.
var ErrQueueInUse = errors.New("solace: queue has an active subscription on this subscriber")

// releaseIdle terminates a receiver that was bound only to provision or edit a
// queue and never had a handler. The broker may already have pushed it part of
// the queue's backlog; those messages were never handed out or settled, so the
// broker redelivers them. That is reported as *IncompleteMessageDeliveryError,
// which here is the expected outcome rather than a failure, and there is
// nothing to wait for, so there is no grace period.
func releaseIdle(r receiver) error {
	err := r.Terminate(0)
	if isUndelivered(err) {
		return nil
	}
	return err
}

func isUndelivered(err error) bool {
	var undelivered *solace.IncompleteMessageDeliveryError
	return errors.As(err, &undelivered)
}

func (s *Subscriber) durable() bool {
	return s.cfg.Mode == DurableSharedQueue || s.cfg.Mode == DurableExclusiveQueue
}

func (s *Subscriber) track(topic string, r solace.PersistentMessageReceiver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[topic] = append(s.active[topic], r)
}

func (s *Subscriber) untrack(topic string, r solace.PersistentMessageReceiver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := s.active[topic]
	for i := range rs {
		if rs[i] == r {
			rs = append(rs[:i], rs[i+1:]...)
			break
		}
	}
	if len(rs) == 0 {
		delete(s.active, topic)
	} else {
		s.active[topic] = rs
	}
}

func (s *Subscriber) activeReceiver(topic string) solace.PersistentMessageReceiver {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rs := s.active[topic]; len(rs) > 0 {
		return rs[0]
	}
	return nil
}

// Unsubscribe removes topic's subscription from its durable queue, so the
// queue stops attracting new messages on that topic. The queue and anything
// already on it stay; consumers keep draining it.
//
// Ending a subscription (cancelling its context, or Close) deliberately does
// not do this, because that also happens on every restart and deploy. Call
// Unsubscribe when a topic is retired for good. It acts on the broker, so it
// affects every consumer of the queue, not just this process.
//
// The queue must exist. It returns ErrNotDurable for the other modes.
func (s *Subscriber) Unsubscribe(topic string) error {
	if !s.durable() {
		return ErrNotDurable
	}
	sub := resource.TopicSubscriptionOf(topic)

	if r := s.activeReceiver(topic); r != nil {
		if err := r.RemoveSubscription(sub); err != nil {
			return fmt.Errorf("solace: removing %q from queue %q: %w", topic, s.cfg.QueueName(topic), err)
		}
		return nil
	}

	// No local consumer: bind a short-lived receiver just to edit the queue's
	// subscriptions. It never creates the queue.
	r, err := s.conn.svc.CreatePersistentMessageReceiverBuilder().
		WithMessageClientAcknowledgement().
		WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).
		Build(s.cfg.queue(topic))
	if err != nil {
		return fmt.Errorf("solace: building receiver to unsubscribe %q: %w", topic, err)
	}
	if err := r.Start(); err != nil {
		return fmt.Errorf("solace: binding queue %q to unsubscribe %q: %w", s.cfg.QueueName(topic), topic, err)
	}
	removeErr := r.RemoveSubscription(sub)
	termErr := releaseIdle(r)
	if removeErr != nil {
		return fmt.Errorf("solace: removing %q from queue %q: %w", topic, s.cfg.QueueName(topic), removeErr)
	}
	if termErr != nil {
		return fmt.Errorf("solace: releasing receiver after unsubscribing %q: %w", topic, termErr)
	}
	return nil
}

// DeleteQueue deletes the durable queue Subscribe would use for topic,
// together with its subscriptions and every message still on it. Deleting a
// queue that does not exist succeeds.
//
// Like Unsubscribe it acts on the broker for every consumer, and nothing calls
// it implicitly. It refuses with ErrQueueInUse while this Subscriber still
// consumes from the queue; consumers in other processes are disconnected by
// the broker. It returns ErrNotDurable for the other modes.
func (s *Subscriber) DeleteQueue(topic string) error {
	if !s.durable() {
		return ErrNotDurable
	}
	if s.activeReceiver(topic) != nil {
		return ErrQueueInUse
	}
	name := s.cfg.QueueName(topic)
	if err := s.conn.svc.EndpointProvisioner().Deprovision(name, true); err != nil {
		return fmt.Errorf("solace: deleting queue %q: %w", name, err)
	}
	return nil
}
