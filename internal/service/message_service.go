package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"multi-tenant-messaging/internal/repository"

	"github.com/google/uuid"
)

type MessageService interface {
	ProcessMessage(ctx context.Context, tenantID uuid.UUID, body []byte) error
	GetMessages(ctx context.Context, cursor string, limit int) ([]repository.Message, string, error)
}

type messageService struct {
	repo repository.MessageRepository
}

func NewMessageService(repo repository.MessageRepository) MessageService {
	return &messageService{repo: repo}
}

func (s *messageService) ProcessMessage(ctx context.Context, tenantID uuid.UUID, body []byte) error {
	log.Printf("Processing message for tenant %s", tenantID)

	if !json.Valid(body) {
		return fmt.Errorf("invalid JSON payload")
	}

	msg := &repository.Message{
		ID:       uuid.New(),
		TenantID: tenantID,
		Payload:  json.RawMessage(body),
	}

	if err := s.repo.SaveMessage(ctx, msg); err != nil {
		return fmt.Errorf("failed to save message: %w", err)
	}

	log.Printf("Successfully saved message %s for tenant %s", msg.ID, tenantID)
	return nil
}

func (s *messageService) GetMessages(ctx context.Context, cursor string, limit int) ([]repository.Message, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 2

	}
	return s.repo.GetMessages(ctx, cursor, limit)
}
