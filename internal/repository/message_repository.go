package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Message struct {
	ID        uuid.UUID       `db:"id"`
	TenantID  uuid.UUID       `db:"tenant_id"`
	Payload   json.RawMessage `db:"payload"`
	CreatedAt time.Time       `db:"created_at"`
}

type MessageRepository interface {
	SaveMessage(ctx context.Context, msg *Message) error
	GetMessages(ctx context.Context, cursor string, limit int) ([]Message, string, error)
}

type messageRepository struct {
	db *sqlx.DB
}

func NewMessageRepository(db *sqlx.DB) MessageRepository {
	return &messageRepository{db: db}
}

func (r *messageRepository) SaveMessage(ctx context.Context, msg *Message) error {
	query := `INSERT INTO messages (id, tenant_id, payload) VALUES ($1, $2, $3)`
	_, err := r.db.ExecContext(ctx, query, msg.ID, msg.TenantID, msg.Payload)
	return err
}

func (r *messageRepository) GetMessages(ctx context.Context, cursor string, limit int) ([]Message, string, error) {
	var messages []Message
	var query string
	var args []interface{}

	if cursor == "" {
		query = `SELECT id, tenant_id, payload, created_at FROM messages ORDER BY created_at DESC, id DESC LIMIT $1`
		args = append(args, limit+1)
	} else {

		cursorTime, err := time.Parse(time.RFC3339Nano, cursor)
		if err != nil {
			return nil, "", err
		}
		query = `SELECT id, tenant_id, payload, created_at FROM messages WHERE created_at <= $1 ORDER BY created_at DESC, id DESC LIMIT $2`
		args = append(args, cursorTime, limit+1)
	}

	err := r.db.SelectContext(ctx, &messages, query, args...)
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(messages) > limit {
		nextCursor = messages[limit-1].CreatedAt.Format(time.RFC3339Nano)
		messages = messages[:limit]
	}

	return messages, nextCursor, nil
}
