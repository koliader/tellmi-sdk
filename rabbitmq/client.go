package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

var UserUpdatedQueue = "updateUser"
var UserCreatedQueue = "userCreated"

const (
	dlqSuffix   = ".dlq"
	dlxSuffix   = ".dlx"
	retrySuffix = ".retry"

	retryCountHeader = "x-retry-count"
)

// ErrReject tells Consume to drop the message without requeueing it. With
// dead-lettering configured the broker routes rejected messages to the DLQ.
// Return it from a consumer handler for permanent failures (e.g. bad payload).
var ErrReject = errors.New("reject rabbitmq message")

const (
	defaultHeartbeat        = 10 * time.Second
	reconnectBackoffInitial = 500 * time.Millisecond
	reconnectBackoffMax     = 10 * time.Second
)

// defaultRetryDelays is the default tiered retry schedule. Each transiently
// failing message first travels to userCreated.retry.5s, then .30s, then .2m,
// then .10m before it is dead-lettered to the DLQ.
var defaultRetryDelays = []time.Duration{
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

// queueSpec remembers how a queue was declared so reconnects can rebuild the
// exact same topology (same arguments, otherwise the broker rejects with 406).
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

// connect dials, creates a channel, re-declares known queues and enables
// publisher confirms. Caller must NOT hold c.mu.
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

// declareQueueWithDlq declares the queue with an x-dead-letter-exchange plus a
// fanout dead-letter exchange and a dead-letter queue bound to it. Dead-lettered
// messages are republished to the exchange with the original routing key.
// Caller must hold c.mu.
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

// declareRetryQueue declares a delay queue: messages sit there for ttl and are
// then dead-lettered to the default exchange, which routes them back to the
// main queue by routing key. Retry state therefore lives in the broker, not in
// the consumer process.
// Caller must hold c.mu.
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
		case <-closed:
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

// SendMessage publishes to queueName with publisher confirms and waits for
// the broker confirmation before returning. Delivery is persistent.
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

// publishConfirmed publishes msg to queueName and waits for the publisher
// confirmation before returning.
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

// Consume subscribes to queueName, calling handler for each message.
// It acks on success, rejects (dead-letters) on ErrReject, and on any other
// handler error routes the message through the tiered retry queues before it is
// finally dead-lettered. It survives connection loss by re-subscribing
// automatically. Blocking; returns when ctx is cancelled.
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

		ch, err := c.readyChannel(ctx) // waiting for live channel
		if err != nil {
			return err
		}

		if err := ch.Qos(1, 0, false); err != nil { // 1 message on handle
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
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
				return ctx.Err()
			case delivery, ok := <-msgs:
				if !ok {
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

// readyChannel returns a live channel, waiting through reconnects until one
// is available or ctx is cancelled.
func (c *Client) readyChannel(ctx context.Context) (*amqp091.Channel, error) {
	for {
		c.mu.Lock()
		ch := c.ch
		c.mu.Unlock()
		if ch != nil {
			return ch, nil
		}
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()

			case <-ticker.C:
			}
		}
	}
}

func (c *Client) handleDelivery(ctx context.Context, delivery amqp091.Delivery, handler func([]byte) error) {
	err := handler(delivery.Body)
	switch {
	case err == nil:
		if ackErr := delivery.Ack(false); ackErr != nil {
			log.Printf("rabbitmq: failed to ack message %s on %s: %v", delivery.MessageId, delivery.RoutingKey, ackErr)
		}
	case errors.Is(err, ErrReject):
		// permanent failure: reject without requeue, broker routes it to the DLQ
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			log.Printf("rabbitmq: failed to reject message %s on %s: %v", delivery.MessageId, delivery.RoutingKey, nackErr)
		}
	default:
		c.retryOrDeadLetter(ctx, delivery)
	}
}

// retryOrDeadLetter handles a transient handler failure. The message is
// published to the next retry queue (publisher confirm) and only then the
// original is acked, so a retry publish failure never loses the message.
// Once every retry tier is exhausted the message is dead-lettered to the DLQ.
func (c *Client) retryOrDeadLetter(ctx context.Context, delivery amqp091.Delivery) {
	attempt := retryCount(delivery.Headers)
	if attempt >= len(c.retryDelays) {
		log.Printf("rabbitmq: message %s on %s exceeded %d retry tiers, dead-lettering", delivery.MessageId, delivery.RoutingKey, len(c.retryDelays))
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			log.Printf("rabbitmq: failed to dead-letter message %s on %s: %v", delivery.MessageId, delivery.RoutingKey, nackErr)
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
		log.Printf("rabbitmq: failed to publish message %s to retry queue %s: %v", delivery.MessageId, retryQueue, err)
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			log.Printf("rabbitmq: failed to requeue message %s on %s: %v", delivery.MessageId, delivery.RoutingKey, nackErr)
		}
		return
	}

	// retry copy is safely in the retry queue: only now ack the original
	if ackErr := delivery.Ack(false); ackErr != nil {
		log.Printf("rabbitmq: failed to ack message %s on %s after retry: %v", delivery.MessageId, delivery.RoutingKey, ackErr)
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

// retryQueueName names the delay queue for a tier, e.g. "userCreated.retry.30s".
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
