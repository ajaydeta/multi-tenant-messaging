package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"multi-tenant-messaging/internal/service"
	"sync"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type TenantConsumer struct {
	tenantID   uuid.UUID
	channel    *amqp.Channel
	msgService service.MessageService
	workers    int
}

func NewTenantConsumer(tenantID uuid.UUID, channel *amqp.Channel, msgService service.MessageService, workers int) *TenantConsumer {
	return &TenantConsumer{
		tenantID:   tenantID,
		channel:    channel,
		msgService: msgService,
		workers:    workers,
	}
}

func (c *TenantConsumer) Start(shutdown chan struct{}) error {
	queueName := fmt.Sprintf("tenant_%s_queue", c.tenantID)
	consumerTag := fmt.Sprintf("consumer_%s", c.tenantID)

	msgs, err := c.channel.Consume(
		queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	var wg sync.WaitGroup
	workerJobs := make(chan amqp.Delivery)

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go c.worker(&wg, workerJobs)
	}

	log.Printf("[%s] Waiting for messages. To exit press CTRL+C", c.tenantID)

	for {
		select {
		case <-shutdown:
			log.Printf("[%s] Shutdown signal received. Closing consumer.", c.tenantID)

			close(workerJobs)

			wg.Wait()

			if err := c.channel.Cancel(consumerTag, false); err != nil {
				log.Printf("[%s] Error canceling consumer: %v", c.tenantID, err)
			}

			c.channel.Close()
			return nil
		case d, ok := <-msgs:
			if !ok {
				log.Printf("[%s] Message channel closed by RabbitMQ. Exiting.", c.tenantID)
				close(workerJobs)
				wg.Wait()
				return nil
			}
			workerJobs <- d
		}
	}
}

func (c *TenantConsumer) worker(wg *sync.WaitGroup, jobs <-chan amqp.Delivery) {
	defer wg.Done()
	for d := range jobs {
		log.Printf("[%s] Worker received a message: %s", c.tenantID, d.Body)
		err := c.msgService.ProcessMessage(context.Background(), c.tenantID, d.Body)
		if err != nil {
			log.Printf("[%s] Error processing message: %v. Sending to dead-letter.", c.tenantID, err)

			d.Nack(false, false)
		} else {
			d.Ack(false)
		}
	}
}
