package repository

import (
	"context"
	"fmt"
	"multi-tenant-messaging/helper"
	"strings"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Tenant struct {
	ID          uuid.UUID `db:"id"`
	Name        string    `db:"name"`
	Concurrency int       `db:"concurrency"`
}

type TenantRepository interface {
	CreateTenant(ctx context.Context, tenant *Tenant) error
	CreateTenantMessagePartition(ctx context.Context, tenantID uuid.UUID) error
	DeleteTenant(ctx context.Context, tenantID uuid.UUID) error
	DropTenantMessagePartition(ctx context.Context, tenantID uuid.UUID) error
	UpdateConcurrency(ctx context.Context, tenantID uuid.UUID, concurrency int) error
	FindAll(ctx context.Context) ([]Tenant, error)
	StartTransaction(ctx context.Context, fn func(ctx context.Context, db *sqlx.Tx) error) error
}

type tenantRepository struct {
	db *sqlx.DB
}

func NewTenantRepository(db *sqlx.DB) TenantRepository {
	return &tenantRepository{db: db}
}

func generatePartitionName(tenantID uuid.UUID) string {

	sanitizedUUID := strings.ReplaceAll(tenantID.String(), "-", "_")
	return fmt.Sprintf("messages_tenant_%s", sanitizedUUID)
}

func (s *tenantRepository) StartTransaction(ctx context.Context, fn func(ctx context.Context, db *sqlx.Tx) error) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	ctx = context.WithValue(ctx, "db", tx)

	err = fn(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

func (r *tenantRepository) CreateTenant(ctx context.Context, tenant *Tenant) error {
	db := helper.GetDbFromContext(ctx, r.db)

	query := `INSERT INTO tenants (id, name, concurrency) VALUES ($1, $2, $3)`
	_, err := db.ExecContext(ctx, query, tenant.ID, tenant.Name, tenant.Concurrency)
	return err
}

func (r *tenantRepository) CreateTenantMessagePartition(ctx context.Context, tenantID uuid.UUID) error {
	db := helper.GetDbFromContext(ctx, r.db)

	partitionName := generatePartitionName(tenantID)

	query := fmt.Sprintf("CREATE TABLE %s PARTITION OF messages FOR VALUES IN ('%s')", partitionName, tenantID)
	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *tenantRepository) DeleteTenant(ctx context.Context, tenantID uuid.UUID) error {
	db := helper.GetDbFromContext(ctx, r.db)

	query := `DELETE FROM tenants WHERE id = $1`
	_, err := db.ExecContext(ctx, query, tenantID)
	return err
}

func (r *tenantRepository) DropTenantMessagePartition(ctx context.Context, tenantID uuid.UUID) error {
	db := helper.GetDbFromContext(ctx, r.db)

	partitionName := generatePartitionName(tenantID)
	query := fmt.Sprintf("DROP TABLE %s", partitionName)
	_, err := db.ExecContext(ctx, query)
	return err
}

func (r *tenantRepository) UpdateConcurrency(ctx context.Context, tenantID uuid.UUID, concurrency int) error {
	query := `UPDATE tenants SET concurrency = $1 WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, concurrency, tenantID)
	return err
}

func (r *tenantRepository) FindAll(ctx context.Context) ([]Tenant, error) {
	var tenants []Tenant
	query := `SELECT id, name, concurrency FROM tenants`
	err := r.db.SelectContext(ctx, &tenants, query)
	return tenants, err
}
