package service

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/streadway/amqp"

	"multi-tenant-messaging/internal/manager"
	"multi-tenant-messaging/internal/repository"
)

type Consumer interface {
	Start(shutdown chan struct{}) error
}

type ConsumerFactory func(tenantID uuid.UUID, concurrency int) (Consumer, error)

type TenantService interface {
	CreateTenant(ctx context.Context, name string) (*repository.Tenant, error)
	DeleteTenant(ctx context.Context, tenantID uuid.UUID) error
	UpdateConcurrency(ctx context.Context, tenantID uuid.UUID, concurrency int) error
	InitializeExistingTenants(ctx context.Context) error
}

type tenantService struct {
	repo            repository.TenantRepository
	rabbitConn      *amqp.Connection
	tenantManager   *manager.TenantManager
	defaultWorkers  int
	consumerFactory ConsumerFactory
}

func NewTenantService(
	repo repository.TenantRepository,
	rabbitConn *amqp.Connection,
	tm *manager.TenantManager,
	defaultWorkers int,
	factory ConsumerFactory,
) TenantService {
	return &tenantService{
		repo:            repo,
		rabbitConn:      rabbitConn,
		tenantManager:   tm,
		defaultWorkers:  defaultWorkers,
		consumerFactory: factory,
	}
}

func (s *tenantService) CreateTenant(ctx context.Context, name string) (*repository.Tenant, error) {
	tenant := &repository.Tenant{
		ID:          uuid.New(),
		Name:        name,
		Concurrency: s.defaultWorkers,
	}

	err := s.repo.StartTransaction(ctx, func(ctx context.Context, db *sqlx.Tx) error {

		if err := s.repo.CreateTenant(ctx, tenant); err != nil {
			return err
		}

		if err := s.repo.CreateTenantMessagePartition(ctx, tenant.ID); err != nil {
			log.Printf("Could not create partition for tenant %s: %v", tenant.ID, err)
		}

		if err := s.startConsumerForTenant(tenant); err != nil {
			return fmt.Errorf("failed to start consumer: %w", err)
		}

		return nil
	})

	return tenant, err
}

func (s *tenantService) startConsumerForTenant(tenant *repository.Tenant) error {

	consumer, err := s.consumerFactory(tenant.ID, tenant.Concurrency)
	if err != nil {
		return fmt.Errorf("could not create consumer via factory: %w", err)
	}

	control := manager.ConsumerControl{
		Shutdown: make(chan struct{}),
		Wg:       new(sync.WaitGroup),
	}
	s.tenantManager.AddConsumer(tenant.ID, control)

	control.Wg.Add(1)
	go func() {
		defer func() {
			control.Wg.Done()
			fmt.Printf("Consumer for tenant %s has stopped.\n", tenant.ID)
		}()

		defer s.tenantManager.RemoveConsumer(tenant.ID)
		log.Printf("Starting consumer for tenant %s with %d workers", tenant.ID, tenant.Concurrency)

		if err := consumer.Start(control.Shutdown); err != nil {
			log.Printf("Consumer for tenant %s stopped with error: %v", tenant.ID, err)
		} else {
			log.Printf("Consumer for tenant %s stopped gracefully.", tenant.ID)
		}
	}()

	return nil
}

func (s *tenantService) DeleteTenant(ctx context.Context, tenantID uuid.UUID) error {
	control, ok := s.tenantManager.GetConsumerControl(tenantID)
	if !ok {
		log.Printf("No active consumer found for tenant %s to delete", tenantID)
	} else {
		close(control.Shutdown)
		control.Wg.Wait()
		s.tenantManager.RemoveConsumer(tenantID)
	}

	ch, err := s.rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()

	queueName := fmt.Sprintf("tenant_%s_queue", tenantID)
	_, err = ch.QueueDelete(queueName, false, false, false)
	if err != nil {
		log.Printf("Failed to delete queue %s: %v", queueName, err)
	}

	return s.repo.StartTransaction(ctx, func(ctx context.Context, db *sqlx.Tx) error {

		if err := s.repo.DeleteTenant(ctx, tenantID); err != nil {
			return err
		}

		if err := s.repo.DropTenantMessagePartition(ctx, tenantID); err != nil {
			log.Printf("Could not drop partition for tenant %s: %v", tenantID, err)
		}

		return nil
	})
}

func (s *tenantService) UpdateConcurrency(ctx context.Context, tenantID uuid.UUID, concurrency int) error {
	log.Printf("Updating concurrency for tenant %s to %d. Restart consumer is required.", tenantID, concurrency)
	return s.repo.UpdateConcurrency(ctx, tenantID, concurrency)
}

func (s *tenantService) InitializeExistingTenants(ctx context.Context) error {
	tenants, err := s.repo.FindAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to find all tenants: %w", err)
	}

	for _, tenant := range tenants {
		log.Printf("Initializing consumer for existing tenant: %s (%s)", tenant.Name, tenant.ID)
		if err := s.startConsumerForTenant(&tenant); err != nil {
			log.Printf("Failed to start consumer for tenant %s: %v", tenant.ID, err)
		}
	}
	return nil
}
