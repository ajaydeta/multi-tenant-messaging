package main

import (
	"multi-tenant-messaging/cmd"
)

// @title Multi-Tenant Messaging API
// @version 1.0
// @description This is a server for a multi-tenant messaging system.
// @host localhost:8080
// @BasePath /
func main() {
	cmd.Execute()
}
