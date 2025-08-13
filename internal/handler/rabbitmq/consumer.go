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
	log.Printf("[Consumer %s] Start", c.tenantID)

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

	log.Printf("[Consumer %s] Waiting for messages", c.tenantID)

	for {
		select {
		case <-shutdown:
			log.Printf("[Consumer %s] Shutdown signal received", c.tenantID)

			close(workerJobs)

			wg.Wait()

			if err := c.channel.Cancel(consumerTag, false); err != nil {
				log.Printf("[Consumer %s] Error canceling consumer: %v", c.tenantID, err)
			}

			c.channel.Close()
			log.Printf("[Consumer %s] Stopped gracefully", c.tenantID)
			return nil
		case d, ok := <-msgs:
			if !ok {
				log.Printf("[Consumer %s] Message channel closed by RabbitMQ", c.tenantID)
				close(workerJobs)
				wg.Wait()
				log.Printf("[Consumer %s] Stopped gracefully", c.tenantID)
				return nil
			}
			select {
			case workerJobs <- d:
			case <-shutdown:
				log.Printf("[Consumer %s] Shutdown signal received while sending to worker", c.tenantID)
				close(workerJobs)
				wg.Wait()
				if err := c.channel.Cancel(consumerTag, false); err != nil {
					log.Printf("[Consumer %s] Error canceling consumer: %v", c.tenantID, err)
				}
				c.channel.Close()
				log.Printf("[Consumer %s] Stopped gracefully", c.tenantID)
				return nil
			}
		}
	}
}

func (c *TenantConsumer) worker(wg *sync.WaitGroup, jobs <-chan amqp.Delivery) {
	defer func() {
		log.Printf("[Consumer %s] Worker stopped", c.tenantID)
		wg.Done()
	}()
	for d := range jobs {
		log.Printf("[Consumer %s] Worker received message: %s", c.tenantID, d.Body)
		err := c.msgService.ProcessMessage(context.Background(), c.tenantID, d.Body)
		if err != nil {
			log.Printf("[Consumer %s] Error processing message: %v. Sending to dead-letter.", c.tenantID, err)

			d.Nack(false, false)
		} else {
			d.Ack(false)
		}
	}
}
