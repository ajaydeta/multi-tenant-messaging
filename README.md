# Multi-Tenant Messaging App Usage Guide
This document explains how to set up, run, and use the application.

## 1. Prerequisites
Ensure the following software is installed on your system:
- Go: Version 1.21 or higher.
- Docker and Docker Compose: To run the database and message broker.

## 2. Configuration
The application uses the `configs/config.yaml` file for configuration. The default settings are already prepared to run with `docker-compose`. 

``` yaml
server:
  port: "8080"

database:
  url: "postgres://user:password@localhost:5000/multi_tenant_db?sslmode=disable"

rabbitmq:
  url: "amqp://guest:guest@localhost:5672/"

app:
  default_worker_concurrency: 3

```
You can change these values if needed, for example, if you are running Postgres or RabbitMQ on different ports.

## 3. Running the Application

### Step 1: Run Dependencies
Use docker-compose to start the PostgreSQL and RabbitMQ containers in the background.

``` bash
docker-compose up -d
```

This command will download the necessary images and run the containers.

### Step 2: Run the Go Application
After the database and RabbitMQ are running, you can run the main application.

``` bash
make run
```

If successful, you will see logs indicating a successful connection to the database and RabbitMQ, followed by the HTTP server running on port 8080.

## 4. Using the API
You can interact with the API using curl or a similar tool.

### a. Create a New Tenant
This will create a new tenant, a database partition for it, and a RabbitMQ consumer.

``` bash
curl -X POST http://localhost:8080/tenants \
-H "Content-Type: application/json" \
-d '{"name": "tenant_alpha"}'
```

Response (Example):
``` json
{"ID":"a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6","Name":"tenant_alpha","Concurrency":3}
```

Note the tenant `ID` for use in the next steps.

### b. Send a Message (Simulation)
Since there is no endpoint to send messages, you can use the RabbitMQ management dashboard at `http://localhost:15672` (login: `guest/guest`) to manually send a message to the `tenant_{id}_queue`.
- Queue Name: `tenant_a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6`
- Payload: `{"message": "Hello Tenant Alpha!"}`

After the message is sent, you will see logs in the application console indicating the message has been received and processed.

### c. Retrieve Messages
Fetch all messages that have been stored in the database.

``` bash
curl -X GET "http://localhost:8080/messages?limit=10"
```


Response (Example):
``` json
{
  "data": [
    {
      "ID": "...",
      "TenantID": "a1b2c3d4-e5f6-g7h8-i9j0-k1l2m3n4o5p6",
      "Payload": {
        "message": "Hello Tenant Alpha!"
      },
      "CreatedAt": "..."
    }
  ],
  "next_cursor": ""
}
```

### d. Update Worker Concurrency

Change the number of workers for a tenant's consumer. This change will be applied when the application is restarted.

```bash
# Replace {TENANT_ID} with your tenant ID
curl -X PUT http://localhost:8080/tenants/{TENANT_ID}/config/concurrency \
-H "Content-Type: application/json" \
-d '{"workers": 5}'
```

### e. Delete a Tenant
This will stop the consumer, delete the RabbitMQ queue, and remove the tenant's data from the database.

``` bash
# Replace {TENANT_ID} with your tenant ID
curl -X DELETE http://localhost:8080/tenants/{TENANT_ID}
```

## 5. Running Tests
To run all tests (unit and integration), ensure Docker is running and execute the following command from the project's root directory:

``` bash
make test
```