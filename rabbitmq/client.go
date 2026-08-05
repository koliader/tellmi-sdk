package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

var UserUpdatedQueue = "updateUser"
var UserCreatedQueue = "userCreated"

// ErrReject tells Consume to drop the message without requeueing it.
// Return it from a consumer handler for permanent failures (e.g. bad payload).
var ErrReject = errors.New("reject rabbitmq message")

const (
	defaultHeartbeat        = 10 * time.Second
	reconnectBackoffInitial = 500 * time.Millisecond
	reconnectBackoffMax     = 10 * time.Second
)

type Client struct {
	mu       sync.Mutex
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	confirms <-chan amqp091.Confirmation
	sendMu   sync.Mutex
	url      string
	config   amqp091.Config

	declared map[string]bool

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

func NewClient(url string, opts ...Option) (*Client, error) {
	c := &Client{
		url:      url,
		config:   amqp091.Config{Heartbeat: defaultHeartbeat},
		declared: make(map[string]bool),
		done:     make(chan struct{}),
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

	for queue := range c.declared {
		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			conn.Close()
			return fmt.Errorf("error re-declaring queue %s: %w", queue, err)
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

	if c.ch == nil {
		return fmt.Errorf("rabbitmq channel is nil")
	}
	if _, err := c.ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("error declaring queue on RabbitMQ: %w", err)
	}
	c.declared[queueName] = true
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

	err := ch.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			MessageId:    uuid.New().String(),
			Timestamp:    time.Now(),
			Body:         message,
		},
	)
	if err != nil {
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
// It acks on success, rejects (no requeue) on ErrReject, and requeues on any
// other handler error. It survives connection loss by re-subscribing
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

		ch, err := c.readyChannel(ctx)
		if err != nil {
			return err
		}

		if err := ch.Qos(1, 0, false); err != nil {
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
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
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
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			log.Printf("rabbitmq: failed to reject message %s on %s: %v", delivery.MessageId, delivery.RoutingKey, nackErr)
		}
	default:
		// transient failure: requeue for retry, pause briefly to avoid hot-loops
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			log.Printf("rabbitmq: failed to requeue message %s on %s: %v", delivery.MessageId, delivery.RoutingKey, nackErr)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
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
