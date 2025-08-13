package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	http_handler "multi-tenant-messaging/internal/handler/http"
	"multi-tenant-messaging/internal/handler/rabbitmq"
	"multi-tenant-messaging/internal/manager"
	"multi-tenant-messaging/internal/repository"
	"multi-tenant-messaging/internal/service"
	"multi-tenant-messaging/pkg/database"
	"multi-tenant-messaging/pkg/queue"
)

var rootCmd = &cobra.Command{
	Use:   "messaging-app",
	Short: "A multi-tenant messaging application",
	Run:   run,
}

func init() {
	cobra.OnInitialize(initConfig)
}

func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}
}

func Execute() error {
	return rootCmd.Execute()
}

func run(cmd *cobra.Command, args []string) {
	// --- Inisialisasi Database ---
	db, err := database.NewPostgresDB(viper.GetString("database.url"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// --- Inisialisasi RabbitMQ ---
	rabbitConn, err := queue.NewRabbitMQConn(viper.GetString("rabbitmq.url"))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	// --- Inisialisasi Layer ---
	tenantRepo := repository.NewTenantRepository(db)
	messageRepo := repository.NewMessageRepository(db)

	tenantManager := manager.NewTenantManager()
	messageService := service.NewMessageService(messageRepo)

	// --- Implementasi ConsumerFactory ---
	consumerFactory := func(tenantID uuid.UUID, concurrency int) (service.Consumer, error) {
		ch, err := rabbitConn.Channel()
		if err != nil {
			return nil, fmt.Errorf("failed to open a channel for consumer: %w", err)
		}

		queueName := fmt.Sprintf("tenant_%s_queue", tenantID)
		_, err = ch.QueueDeclare(
			queueName, true, false, false, false, nil,
		)
		if err != nil {
			ch.Close()
			return nil, fmt.Errorf("failed to declare a queue for consumer: %w", err)
		}

		return rabbitmq.NewTenantConsumer(tenantID, ch, messageService, concurrency), nil
	}

	// --- Inisialisasi TenantService dengan Factory ---
	tenantService := service.NewTenantService(
		tenantRepo,
		rabbitConn,
		tenantManager,
		viper.GetInt("app.default_worker_concurrency"),
		consumerFactory,
	)

	if err := tenantService.InitializeExistingTenants(context.Background()); err != nil {
		log.Printf("Warning: could not initialize existing tenants: %v", err)
	}

	// --- Inisialisasi Echo Server ---
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// --- Setup Handlers ---
	tenantHandler := http_handler.NewTenantHandler(tenantService)
	messageHandler := http_handler.NewMessageHandler(messageService)

	// --- Setup Routes ---
	e.POST("/tenants", tenantHandler.CreateTenant)
	e.DELETE("/tenants/:id", tenantHandler.DeleteTenant)
	e.PUT("/tenants/:id/config/concurrency", tenantHandler.UpdateConcurrency)
	e.GET("/messages", messageHandler.GetMessages)

	// --- Start Server & Graceful Shutdown ---
	go func() {
		port := viper.GetString("server.port")
		if err := e.Start(fmt.Sprintf(":%s", port)); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tenantManager.ShutdownAll(ctx)

	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}

	log.Println("Server gracefully stopped")
}
