package manager

import (
	"context"
	"log"
	"sync"

	"github.com/google/uuid"
)

type ConsumerControl struct {
	Shutdown chan struct{}
	Wg       *sync.WaitGroup
}

type TenantManager struct {
	mu        sync.RWMutex
	consumers map[uuid.UUID]ConsumerControl
}

func NewTenantManager() *TenantManager {
	return &TenantManager{
		consumers: make(map[uuid.UUID]ConsumerControl),
	}
}

func (tm *TenantManager) AddConsumer(tenantID uuid.UUID, control ConsumerControl) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.consumers[tenantID] = control
	log.Printf("Consumer for tenant %s added to manager", tenantID)
}

func (tm *TenantManager) RemoveConsumer(tenantID uuid.UUID) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.consumers, tenantID)
	log.Printf("Consumer for tenant %s removed from manager", tenantID)
}

func (tm *TenantManager) GetConsumerControl(tenantID uuid.UUID) (ConsumerControl, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	control, ok := tm.consumers[tenantID]
	return control, ok
}

func (tm *TenantManager) ShutdownAll(ctx context.Context) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	log.Printf("Shutting down all %d consumers...", len(tm.consumers))
	for id, control := range tm.consumers {
		log.Printf("Sending shutdown signal to consumer for tenant %s", id)
		close(control.Shutdown)
		control.Wg.Wait()
	}
	log.Println("All consumers have been shut down.")
}
