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
	log.Printf("[TenantManager] Added consumer for tenant %s", tenantID)
}

func (tm *TenantManager) RemoveConsumer(tenantID uuid.UUID) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.consumers, tenantID)
	log.Printf("[TenantManager] Removed consumer for tenant %s", tenantID)
}

func (tm *TenantManager) GetConsumerControl(tenantID uuid.UUID) (ConsumerControl, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	control, ok := tm.consumers[tenantID]
	return control, ok
}

func safeClose(ch chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TenantManager] Warning: tried to close an already closed shutdown channel")
		}
	}()
	close(ch)
}

func (tm *TenantManager) ShutdownAll(ctx context.Context) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	log.Printf("[TenantManager] Shutting down %d consumers: %v", len(tm.consumers), tm.tenantIDs())
	for id, control := range tm.consumers {
		log.Printf("[TenantManager] Sending shutdown signal to tenant %s", id)
		safeClose(control.Shutdown)
		log.Printf("[TenantManager] Waiting for tenant %s to stop...", id)
		done := make(chan struct{})
		go func() {
			control.Wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			log.Printf("[TenantManager] Tenant %s stopped.", id)
		case <-ctx.Done():
			log.Printf("[TenantManager] WARNING: Tenant %s did not stop before deadline!", id)
		}
	}
	log.Println("[TenantManager] All consumers shut down.")
}

func (tm *TenantManager) tenantIDs() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(tm.consumers))
	for id := range tm.consumers {
		ids = append(ids, id)
	}
	return ids
}
