package helper

func GetQueueName(tenantID string) string {
	return "tenant_" + tenantID + "_queue"
	
}