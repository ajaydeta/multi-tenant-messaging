package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	http_handler "multi-tenant-messaging/internal/handler/http"
	"multi-tenant-messaging/internal/handler/rabbitmq"
	"multi-tenant-messaging/internal/manager"
	"multi-tenant-messaging/internal/repository"
	"multi-tenant-messaging/internal/service"
)

type IntegrationTestSuite struct {
	suite.Suite
	db            *sqlx.DB
	rabbitConn    *amqp.Connection
	pgContainer   testcontainers.Container
	rbmqContainer testcontainers.Container
	echo          *echo.Echo
	tenantMgr     *manager.TenantManager
	tenantSvc     service.TenantService
}

func TestIntegration(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) SetupSuite() {
	ctx := context.Background()
	var err error

	pgReq := testcontainers.ContainerRequest{
		Image:        "postgres:14-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "user_test",
			"POSTGRES_PASSWORD": "secret",
			"POSTGRES_DB":       "test_db",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5 * time.Minute),
	}
	s.pgContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	s.Require().NoError(err)

	pgPort, err := s.pgContainer.MappedPort(ctx, "5432")
	s.Require().NoError(err)
	pgHost, err := s.pgContainer.Host(ctx)
	s.Require().NoError(err)
	dbURL := fmt.Sprintf("postgres://user_test:secret@%s:%s/test_db?sslmode=disable", pgHost, pgPort.Port())

	s.db, err = sqlx.Connect("postgres", dbURL)
	s.Require().NoError(err)

	rbmqReq := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.12-management-alpine",
		ExposedPorts: []string{"5672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
		WaitingFor: wait.ForLog("Server startup complete").WithStartupTimeout(5 * time.Minute),
	}
	s.rbmqContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: rbmqReq,
		Started:          true,
	})
	s.Require().NoError(err)

	rbmqPort, err := s.rbmqContainer.MappedPort(ctx, "5672")
	s.Require().NoError(err)
	rbmqHost, err := s.rbmqContainer.Host(ctx)
	s.Require().NoError(err)
	rabbitURL := fmt.Sprintf("amqp://guest:guest@%s:%s/", rbmqHost, rbmqPort.Port())

	s.rabbitConn, err = amqp.Dial(rabbitURL)
	s.Require().NoError(err)

	viper.Set("database.url", dbURL)
	viper.Set("rabbitmq.url", rabbitURL)
	viper.Set("app.default_worker_concurrency", 1)

	s.setupDatabase()
	s.setupApp()
}

func (s *IntegrationTestSuite) TearDownSuite() {
	ctx := context.Background()
	s.tenantMgr.ShutdownAll(ctx)
	s.rabbitConn.Close()
	s.db.Close()

	s.Require().NoError(s.pgContainer.Terminate(ctx))
	s.Require().NoError(s.rbmqContainer.Terminate(ctx))
}

func (s *IntegrationTestSuite) setupDatabase() {
	_, err := s.db.Exec(`
        CREATE TABLE tenants (
            id UUID PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            concurrency INT NOT NULL
        )
    `)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
        CREATE TABLE messages (
			id UUID NOT NULL,
			tenant_id UUID NOT NULL,
			payload JSONB,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (tenant_id, id)
		)
		PARTITION BY LIST (tenant_id);
    `)
	s.Require().NoError(err)
}

func (s *IntegrationTestSuite) setupApp() {
	tenantRepo := repository.NewTenantRepository(s.db)
	messageRepo := repository.NewMessageRepository(s.db)
	s.tenantMgr = manager.NewTenantManager()
	messageService := service.NewMessageService(messageRepo)

	consumerFactory := func(tenantID uuid.UUID, concurrency int) (service.Consumer, error) {
		ch, err := s.rabbitConn.Channel()
		if err != nil {
			return nil, fmt.Errorf("failed to open a channel for consumer: %w", err)
		}
		queueName := fmt.Sprintf("tenant_%s_queue", tenantID)
		_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil {
			ch.Close()
			return nil, fmt.Errorf("failed to declare a queue for consumer: %w", err)
		}
		return rabbitmq.NewTenantConsumer(tenantID, ch, messageService, concurrency), nil
	}

	s.tenantSvc = service.NewTenantService(
		tenantRepo,
		s.rabbitConn,
		s.tenantMgr,
		viper.GetInt("app.default_worker_concurrency"),
		consumerFactory,
	)

	s.echo = echo.New()
	tenantHandler := http_handler.NewTenantHandler(s.tenantSvc)
	messageHandler := http_handler.NewMessageHandler(messageService)

	s.echo.POST("/tenants", tenantHandler.CreateTenant)
	s.echo.DELETE("/tenants/:id", tenantHandler.DeleteTenant)
	s.echo.PUT("/tenants/:id/config/concurrency", tenantHandler.UpdateConcurrency)
	s.echo.GET("/messages", messageHandler.GetMessages)
}

func (s *IntegrationTestSuite) TestTenantLifecycle() {

	reqBody := `{"name": "test-tenant-1"}`
	req := httptest.NewRequest(http.MethodPost, "/tenants", bytes.NewBufferString(reqBody))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Require().Equal(http.StatusCreated, rec.Code)

	var tenant repository.Tenant
	err := json.Unmarshal(rec.Body.Bytes(), &tenant)
	s.Require().NoError(err)
	s.Require().Equal("test-tenant-1", tenant.Name)
	s.Require().NotEqual(uuid.Nil, tenant.ID)

	time.Sleep(1 * time.Second)
	_, ok := s.tenantMgr.GetConsumerControl(tenant.ID)
	s.Require().True(ok, "Consumer should be active in TenantManager")

	ch, err := s.rabbitConn.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	queueName := fmt.Sprintf("tenant_%s_queue", tenant.ID)
	msgBody := `{"key": "value"}`
	err = ch.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(msgBody),
		},
	)
	s.Require().NoError(err)

	time.Sleep(2 * time.Second)
	var msgCount int

	err = s.db.Get(&msgCount, "SELECT COUNT(*) FROM messages WHERE tenant_id = $1", tenant.ID)
	s.Require().NoError(err)
	s.Require().Equal(1, msgCount, "Message should be saved to the database")

	req = httptest.NewRequest(http.MethodGet, "/messages?limit=5", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Require().Equal(http.StatusOK, rec.Code)
	var msgResponse map[string]interface{}
	err = json.Unmarshal(rec.Body.Bytes(), &msgResponse)
	s.Require().NoError(err)
	data, _ := msgResponse["data"].([]interface{})
	s.Require().Len(data, 1)

	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/tenants/%s", tenant.ID), nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Require().Equal(http.StatusNoContent, rec.Code)

	time.Sleep(1 * time.Second)
	_, ok = s.tenantMgr.GetConsumerControl(tenant.ID)
	s.Require().False(ok, "Consumer should be removed from TenantManager")

	var tenantCount int
	err = s.db.Get(&tenantCount, "SELECT COUNT(*) FROM tenants WHERE id = $1", tenant.ID)
	s.Require().NoError(err)
	s.Require().Equal(0, tenantCount)
}
