run:
	@go run main.go messaging-app

mock:
	@echo "Mocking..."
	@mockgen -source=internal/repository/tenant_repository.go -destination=internal/repository/mocks/mock_tenant_repository.go -package=mocks
	@mockgen -source=internal/repository/message_repository.go -destination=internal/repository/mocks/mock_message_repository.go -package=mocks

