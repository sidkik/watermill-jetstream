package jetstream

import (
	"context"
	"fmt"
	internalSync "github.com/ThreeDotsLabs/watermill/pubsub/sync"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberConfig struct {
	// URL is the URL to the broker
	URL string

	// ClusterID is the NATS Streaming cluster ID.
	ClusterID string

	// ClientID is the NATS Streaming client ID to connect with.
	// ClientID can contain only alphanumeric and `-` or `_` characters.
	//
	// Using DurableName causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	ClientID string

	// QueueGroup is the NATS Streaming queue group.
	//
	// All subscriptions with the same queue name (regardless of the connection they originate from)
	// will form a queue group. Each message will be delivered to only one subscriber per queue group,
	// using queuing semantics.
	//
	// It is recommended to set it with DurableName.
	// For non durable queue subscribers, when the last member leaves the group,
	// that group is removed. A durable queue group (DurableName) allows you to have all members leave
	// but still maintain state. When a member re-joins, it starts at the last position in that group.
	//
	// When QueueGroup is empty, subscribe without QueueGroup will be used.
	QueueGroup string

	// DurableName is the NATS streaming durable name.
	//
	// Subscriptions may also specify a “durable name” which will survive client restarts.
	// Durable subscriptions cause the server to track the last acknowledged message
	// sequence number for a client and durable name. When the client restarts/resubscribes,
	// and uses the same client ID and durable name, the server will resume delivery beginning
	// with the earliest unacknowledged message for this durable subscription.
	//
	// Doing this causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	DurableName string

	// SubscribersCount determines wow much concurrent subscribers should be started.
	SubscribersCount int

	// CloseTimeout determines how long subscriber will wait for Ack/Nack on close.
	// When no Ack/Nack is received after CloseTimeout, subscriber will be closed.
	CloseTimeout time.Duration

	// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
	// It is mapped to stan.AckWait option.
	AckWaitTimeout time.Duration

	// SubscribeTimeout determines how long subscriber will wait for a successful subscription
	SubscribeTimeout time.Duration

	// NatsOptions are custom []nats.Option passed to the connection.
	// It is also used to provide connection parameters, for example:
	// 		nats.URL("nats://localhost:4222")
	NatsOptions []nats.Option

	// Unmarshaler is an unmarshaler used to unmarshaling messages from NATS format to Watermill format.
	Unmarshaler Unmarshaler
}

type SubscriberSubscriptionConfig struct {
	// Unmarshaler is an unmarshaler used to unmarshaling messages from NATS format to Watermill format.
	Unmarshaler Unmarshaler
	// QueueGroup is the NATS Streaming queue group.
	//
	// All subscriptions with the same queue name (regardless of the connection they originate from)
	// will form a queue group. Each message will be delivered to only one subscriber per queue group,
	// using queuing semantics.
	//
	// It is recommended to set it with DurableName.
	// For non durable queue subscribers, when the last member leaves the group,
	// that group is removed. A durable queue group (DurableName) allows you to have all members leave
	// but still maintain state. When a member re-joins, it starts at the last position in that group.
	//
	// When QueueGroup is empty, subscribe without QueueGroup will be used.
	QueueGroup string

	// DurableName is the NATS streaming durable name.
	//
	// Subscriptions may also specify a “durable name” which will survive client restarts.
	// Durable subscriptions cause the server to track the last acknowledged message
	// sequence number for a client and durable name. When the client restarts/resubscribes,
	// and uses the same client ID and durable name, the server will resume delivery beginning
	// with the earliest unacknowledged message for this durable subscription.
	//
	// Doing this causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	DurableName string

	// SubscribersCount determines wow much concurrent subscribers should be started.
	SubscribersCount int

	// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
	// It is mapped to stan.AckWait option.
	AckWaitTimeout time.Duration

	// CloseTimeout determines how long subscriber will wait for Ack/Nack on close.
	// When no Ack/Nack is received after CloseTimeout, subscriber will be closed.
	CloseTimeout time.Duration

	// SubscribeTimeout determines how long subscriber will wait for a successful subscription
	SubscribeTimeout time.Duration
}

func (c *SubscriberConfig) GetStreamingSubscriberSubscriptionConfig() SubscriberSubscriptionConfig {
	return SubscriberSubscriptionConfig{
		Unmarshaler:      c.Unmarshaler,
		QueueGroup:       c.QueueGroup,
		DurableName:      c.DurableName,
		SubscribersCount: c.SubscribersCount,
		AckWaitTimeout:   c.AckWaitTimeout,
		CloseTimeout:     c.CloseTimeout,
		SubscribeTimeout: c.SubscribeTimeout,
	}
}

func (c *SubscriberSubscriptionConfig) setDefaults() {
	if c.SubscribersCount <= 0 {
		c.SubscribersCount = 1
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = time.Second * 30
	}
	if c.AckWaitTimeout <= 0 {
		c.AckWaitTimeout = time.Second * 30
	}
	if c.SubscribeTimeout <= 0 {
		c.SubscribeTimeout = time.Second * 30
	}
}

func (c *SubscriberSubscriptionConfig) Validate() error {
	if c.Unmarshaler == nil {
		return errors.New("SubscriberConfig.Unmarshaler is missing")
	}

	if c.QueueGroup == "" && c.SubscribersCount > 1 {
		return errors.New(
			"to set SubscriberConfig.SubscribersCount " +
				"you need to also set SubscriberConfig.QueueGroup, " +
				"in other case you will receive duplicated messages",
		)
	}

	return nil
}

type Subscriber struct {
	conn   *nats.Conn
	logger watermill.LoggerAdapter

	config SubscriberSubscriptionConfig

	subsLock sync.RWMutex

	closed  bool
	closing chan struct{}

	outputsWg sync.WaitGroup
	js        nats.JetStreamContext
}

// NewSubscriber creates a new Subscriber.
//
// When using custom NATS hostname, you should pass it by options SubscriberConfig.NatsOptions:
//		// ...
//		NatsOptions: []nats.Option{
//			nats.URL("nats://your-nats-hostname:4222"),
//		}
//		// ...
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to NATS")
	}
	return NewSubscriberWithNatsConn(conn, config.GetStreamingSubscriberSubscriptionConfig(), logger)
}

func NewSubscriberWithNatsConn(conn *nats.Conn, config SubscriberSubscriptionConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	js, err := conn.JetStream()

	if err != nil {
		return nil, err
	}

	return &Subscriber{
		conn:    conn,
		logger:  logger,
		config:  config,
		closing: make(chan struct{}),
		js:      js,
	}, nil
}

// Subscribe subscribes messages from NATS Streaming.
//
// Subscribe will spawn SubscribersCount goroutines making subscribe.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)

	s.outputsWg.Add(1)
	outputWg := &sync.WaitGroup{}

	for i := 0; i < s.config.SubscribersCount; i++ {
		outputWg.Add(1)

		subscriberLogFields := watermill.LogFields{
			"subscriber_num": i,
			"topic":          topic,
		}

		s.logger.Debug("Starting subscriber", subscriberLogFields)

		sub, err := s.subscribe(topic, func(msg *nats.Msg) {
			s.processMessage(ctx, msg, output, subscriberLogFields)
		})
		if err != nil {
			return nil, errors.Wrap(err, "cannot subscribe")
		}

		go func(subscriber *nats.Subscription, subscriberLogFields watermill.LogFields) {
			defer outputWg.Done()
			select {
			case <-s.closing:
				// unblock
			case <-ctx.Done():
				// unblock
			}

			if err := sub.Unsubscribe(); err != nil {
				s.logger.Error("Cannot unsubscribe", err, subscriberLogFields)
			}
		}(sub, subscriberLogFields)
	}

	go func() {
		defer s.outputsWg.Done()
		outputWg.Wait()
		close(output)
	}()

	return output, nil
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	err := initStream(s.js, topic)

	if err != nil {
		return errors.Wrap(err, "cannot initialize subscribe")
	}

	//TODO: revisit
	sub, err := s.subscribe(topic, func(msg *nats.Msg) {
		s.logger.Trace("message received in subscribe initialize will nak", nil)
		err := msg.Nak()
		if err != nil {
			s.logger.Error("error naking message received on init", err, nil)
		}
	})
	if err != nil {
		return errors.Wrap(err, "cannot initialize subscribe")
	}

	return errors.Wrap(sub.Unsubscribe(), "cannot close after subscribe initialize")
}

func (s *Subscriber) subscribe(topic string, cb nats.MsgHandler) (*nats.Subscription, error) {
	subTopic := fmt.Sprintf("%s.*", topic)

	opts := make([]nats.SubOpt, 0)

	if s.config.DurableName != "" {
		opts = append(opts, nats.Durable(s.config.DurableName))
	} else {
		opts = append(opts, nats.BindStream(subTopic))
	}

	if s.config.QueueGroup != "" {
		return s.js.QueueSubscribe(
			subTopic,
			s.config.QueueGroup,
			cb,
			opts...,
		)
	}

	return s.js.Subscribe(
		subTopic,
		cb,
		opts...,
	)
}

func (s *Subscriber) processMessage(
	ctx context.Context,
	m *nats.Msg,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	if s.isClosed() {
		return
	}

	s.logger.Trace("Received message", logFields)

	msg, err := s.config.Unmarshaler.Unmarshal(m)
	if err != nil {
		s.logger.Error("Cannot unmarshal message", err, logFields)
		return
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	messageLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", messageLogFields)

	select {
	case output <- msg:
		s.logger.Trace("Message sent to consumer", messageLogFields)
	case <-s.closing:
		s.logger.Trace("Closing, message discarded", messageLogFields)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded", messageLogFields)
		return
	}

	select {
	case <-msg.Acked():
		if err := m.Ack(); err != nil {
			s.logger.Error("Cannot send ack", err, messageLogFields)
			return
		}
		s.logger.Trace("Message Acked", messageLogFields)
	case <-msg.Nacked():
		if err := m.Nak(); err != nil {
			s.logger.Error("Cannot send nak", err, messageLogFields)
			return
		}
		s.logger.Trace("Message Nacked", messageLogFields)
		return
	case <-time.After(s.config.AckWaitTimeout):
		s.logger.Trace("Ack timeout", messageLogFields)
		return
	case <-s.closing:
		s.logger.Trace("Closing, message discarded before ack", messageLogFields)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded before ack", messageLogFields)
		return
	}
}

func (s *Subscriber) Close() error {
	s.subsLock.Lock()
	defer s.subsLock.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	s.logger.Debug("Closing subscriber", nil)
	defer s.logger.Info("Subscriber closed", nil)

	close(s.closing)

	if internalSync.WaitGroupTimeout(&s.outputsWg, s.config.CloseTimeout) {
		return errors.New("output wait group did not finish")
	}

	if err := s.conn.Drain(); err != nil {
		return errors.Wrap(err, "cannot close conn")
	}

	return nil
}

func (s *Subscriber) isClosed() bool {
	s.subsLock.RLock()
	defer s.subsLock.RUnlock()

	return s.closed
}