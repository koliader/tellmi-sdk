package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

var UserUpdatedQueue = "updateUser"
var UserCreatedQueue = "userCreated"

const (
	dlqSuffix   = ".dlq"
	dlxSuffix   = ".dlx"
	retrySuffix = ".retry"

	retryCountHeader = "x-retry-count"
)

var ErrReject = errors.New("reject rabbitmq message")

const (
	defaultHeartbeat        = 10 * time.Second
	reconnectBackoffInitial = 500 * time.Millisecond
	reconnectBackoffMax     = 10 * time.Second
)

var defaultRetryDelays = []time.Duration{
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

type queueSpec struct {
	kind   string // "main" or "retry"
	ttl    time.Duration
	target string // retry queues dead-letter back to their main queue
}

type Client struct {
	mu       sync.Mutex
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	confirms <-chan amqp091.Confirmation
	sendMu   sync.Mutex
	url      string
	config   amqp091.Config

	retryDelays []time.Duration
	declared    map[string]queueSpec

	closing atomic.Bool
	done    chan struct{}
	wg      sync.WaitGroup
}

type Option func(*Client)

func WithHeartbeat(interval time.Duration) Option {
	return func(c *Client) {
		c.config.Heartbeat = interval
	}
}

func WithConnectionTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.config.Dial = amqp091.DefaultDial(timeout)
	}
}

// WithRetryDelays configures the tiered retry schedule applied to transiently
// failing messages. Each delay maps to a dedicated retry queue; a message that
// keeps failing is dead-lettered to the DLQ once it has passed every tier.
func WithRetryDelays(delays []time.Duration) Option {
	return func(c *Client) {
		if len(delays) > 0 {
			c.retryDelays = append([]time.Duration(nil), delays...)
		}
	}
}

func NewClient(url string, opts ...Option) (*Client, error) {
	c := &Client{
		url:         url,
		config:      amqp091.Config{Heartbeat: defaultHeartbeat},
		retryDelays: append([]time.Duration(nil), defaultRetryDelays...),
		declared:    make(map[string]queueSpec),
		done:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}

	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("error connecting to RabbitMQ: %w", err)
	}

	c.wg.Add(1)
	go c.reconnectLoop()

	return c, nil
}

func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := amqp091.DialConfig(c.url, c.config)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	for name, spec := range c.declared {
		var derr error
		switch spec.kind {
		case "main":
			derr = c.declareQueueWithDlq(ch, name)
		case "retry":
			derr = c.declareRetryQueue(ch, spec.target, name, spec.ttl)
		}
		if derr != nil {
			conn.Close()
			return fmt.Errorf("error re-declaring queue %s: %w", name, derr)
		}
	}

	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("error enabling publisher confirms: %w", err)
	}
	// Register a single long-lived confirmation listener here. Creating a new
	// NotifyPublish channel per publish fills up the previous listener's buffer
	// and stalls the confirmation fan-out for all later publishes.
	confirms := ch.NotifyPublish(make(chan amqp091.Confirmation, 1024))

	c.conn = conn
	c.ch = ch
	c.confirms = confirms
	return nil
}

func (c *Client) declareQueueWithDlq(ch *amqp091.Channel, queueName string) error {
	dlxName := queueName + dlxSuffix
	dlqName := queueName + dlqSuffix

	if err := ch.ExchangeDeclare(dlxName, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("error declaring dead-letter exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("error declaring dead-letter queue: %w", err)
	}
	if err := ch.QueueBind(dlqName, queueName, dlxName, false, nil); err != nil {
		return fmt.Errorf("error binding dead-letter queue: %w", err)
	}

	args := amqp091.Table{"x-dead-letter-exchange": dlxName}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, args); err != nil {
		return fmt.Errorf("error declaring queue on RabbitMQ: %w", err)
	}
	return nil
}

func (c *Client) declareRetryQueue(ch *amqp091.Channel, mainQueue, retryQueue string, ttl time.Duration) error {
	args := amqp091.Table{
		"x-message-ttl":             int32(ttl / time.Millisecond),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": mainQueue,
	}
	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("error declaring retry queue: %w", err)
	}
	return nil
}

func (c *Client) reconnectLoop() {
	defer c.wg.Done()

	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			select {
			case <-c.done:
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		closed := conn.NotifyClose(make(chan *amqp091.Error, 1))
		select {
		case <-c.done:
			return
		case err := <-closed:
			log.Warn().Err(err).Msg("rabbitmq: connection lost, attempting reconnect")
		}

		backoff := reconnectBackoffInitial
		for {
			if c.closing.Load() {
				return
			}
			if err := c.connect(); err != nil {
				c.mu.Lock()
				c.conn = nil
				c.ch = nil
				c.mu.Unlock()
				log.Warn().Err(err).Dur("retry_in", backoff).Msg("rabbitmq: reconnect attempt failed")
				select {
				case <-c.done:
					return
				case <-time.After(backoff):
				}
				if backoff < reconnectBackoffMax {
					backoff *= 2
				}
				continue
			}
			log.Info().Msg("rabbitmq: reconnected")
			break
		}
	}
}

func (c *Client) DeclareQueue(queueName string) error {
	if c == nil {
		return fmt.Errorf("rabbitmq client is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.declared[queueName]; ok {
		return nil
	}
	if c.ch == nil {
		return fmt.Errorf("rabbitmq channel is nil")
	}
	if err := c.declareQueueWithDlq(c.ch, queueName); err != nil {
		return err
	}
	c.declared[queueName] = queueSpec{kind: "main"}

	// declare the tiered retry queues for this queue so the topology is stable
	for _, delay := range c.retryDelays {
		rq := retryQueueName(queueName, delay)
		if _, ok := c.declared[rq]; ok {
			continue
		}
		if err := c.declareRetryQueue(c.ch, queueName, rq, delay); err != nil {
			return err
		}
		c.declared[rq] = queueSpec{kind: "retry", ttl: delay, target: queueName}
	}
	return nil
}

func (c *Client) SendMessage(ctx context.Context, queueName string, message []byte) error {
	if c == nil {
		return fmt.Errorf("rabbitmq client is nil")
	}
	if err := c.DeclareQueue(queueName); err != nil {
		return err
	}
	return c.publishConfirmed(ctx, queueName, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    uuid.New().String(),
		Timestamp:    time.Now(),
		Body:         message,
	})
}

func (c *Client) publishConfirmed(ctx context.Context, queueName string, msg amqp091.Publishing) error {
	// Serialize sends so the shared confirmation stream stays ordered per publish.
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	ch := c.ch
	confirms := c.confirms
	c.mu.Unlock()
	if ch == nil || confirms == nil {
		return fmt.Errorf("rabbitmq channel is nil")
	}

	if err := ch.PublishWithContext(ctx, "", queueName, false, false, msg); err != nil {
		return fmt.Errorf("error publishing message to RabbitMQ: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("publisher confirm cancelled: %w", ctx.Err())
	case conf, ok := <-confirms:
		if !ok {
			return fmt.Errorf("publisher confirmation channel closed")
		}
		if !conf.Ack {
			return fmt.Errorf("broker rejected message %s", queueName)
		}
		return nil
	}
}

func (c *Client) Consume(ctx context.Context, queueName string, handler func([]byte) error) error {
	if c == nil {
		return fmt.Errorf("rabbitmq client is nil")
	}
	if err := c.DeclareQueue(queueName); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := c.readyConnection(ctx) // waiting for live connection
		if err != nil {
			return err
		}

		// each consumer gets its own channel; sharing one channel across
		// consumers races Qos/Consume and triggers 503 "unexpected command"
		ch, err := conn.Channel()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		if err := ch.Qos(1, 0, false); err != nil { // 1 message on handle
			ch.Close()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		for {
			select {
			case <-ctx.Done():
				ch.Close()
				return ctx.Err()
			case delivery, ok := <-msgs:
				if !ok {
					ch.Close()
					goto resubscribe
				}
				c.handleDelivery(ctx, delivery, handler)
			}
		}

	resubscribe:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (c *Client) readyConnection(ctx context.Context) (*amqp091.Connection, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Client) handleDelivery(ctx context.Context, delivery amqp091.Delivery, handler func([]byte) error) {
	err := handler(delivery.Body)
	switch {
	case err == nil:
		if ackErr := delivery.Ack(false); ackErr != nil {
			log.Error().Err(ackErr).Str("message_id", delivery.MessageId).Str("queue", delivery.RoutingKey).
				Msg("rabbitmq: failed to ack message")
		}
	case errors.Is(err, ErrReject):
		// permanent failure: reject without requeue, broker routes it to the DLQ
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			log.Error().Err(nackErr).Str("message_id", delivery.MessageId).Str("queue", delivery.RoutingKey).
				Msg("rabbitmq: failed to reject message")
		}
	default:
		c.retryOrDeadLetter(ctx, delivery)
	}
}

func (c *Client) retryOrDeadLetter(ctx context.Context, delivery amqp091.Delivery) {
	attempt := retryCount(delivery.Headers)
	if attempt >= len(c.retryDelays) {
		log.Warn().Str("message_id", delivery.MessageId).Str("queue", delivery.RoutingKey).
			Int("retries", attempt).Msg("rabbitmq: message exceeded retry tiers, dead-lettering")
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			log.Error().Err(nackErr).Str("message_id", delivery.MessageId).Str("queue", delivery.RoutingKey).
				Msg("rabbitmq: failed to dead-letter message")
		}
		return
	}

	retryQueue := retryQueueName(delivery.RoutingKey, c.retryDelays[attempt])
	if err := c.publishConfirmed(ctx, retryQueue, amqp091.Publishing{
		ContentType:  delivery.ContentType,
		DeliveryMode: delivery.DeliveryMode,
		MessageId:    delivery.MessageId,
		Timestamp:    delivery.Timestamp,
		Headers:      withRetryCount(delivery.Headers, attempt+1),
		Body:         delivery.Body,
	}); err != nil {
		// Publish failed: do not ack the original. Requeue it so the broker
		// redelivers it later instead of stalling this consumer (QoS 1).
		log.Error().Err(err).Str("message_id", delivery.MessageId).Str("queue", retryQueue).
			Msg("rabbitmq: failed to publish message to retry queue")
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			log.Error().Err(nackErr).Str("message_id", delivery.MessageId).Str("queue", delivery.RoutingKey).
				Msg("rabbitmq: failed to requeue message")
		}
		return
	}

	// retry copy is safely in the retry queue: only now ack the original
	if ackErr := delivery.Ack(false); ackErr != nil {
		log.Error().Err(ackErr).Str("message_id", delivery.MessageId).Str("queue", delivery.RoutingKey).
			Msg("rabbitmq: failed to ack message after retry")
	}
}

// withRetryCount returns a copy of headers with x-retry-count bumped to count.
func withRetryCount(headers amqp091.Table, count int) amqp091.Table {
	out := make(amqp091.Table, len(headers)+1)
	maps.Copy(out, headers)
	out[retryCountHeader] = int32(count)
	return out
}

// retryCount reads the x-retry-count header, defaulting to 0 for messages that
// have never been retried.
func retryCount(headers amqp091.Table) int {
	v, ok := headers[retryCountHeader]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func retryQueueName(queue string, delay time.Duration) string {
	return fmt.Sprintf("%s%s.%s", queue, retrySuffix, formatDuration(delay))
}

func formatDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", int(d/time.Second))
	default:
		return d.String()
	}
}

func (c *Client) IsReady() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.conn.IsClosed() && c.ch != nil && !c.ch.IsClosed()
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if !c.closing.Swap(true) {
		close(c.done)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if c.ch != nil {
		if closeErr := c.ch.Close(); closeErr != nil {
			err = fmt.Errorf("error closing RabbitMQ channel: %w", closeErr)
		}
	}
	if c.conn != nil {
		if closeErr := c.conn.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("%v; error closing RabbitMQ connection: %w", err, closeErr)
			} else {
				err = fmt.Errorf("error closing RabbitMQ connection: %w", closeErr)
			}
		}
	}
	return err
}
