package storage

import (
	"context"
	"fmt"
	"sync"
)

type MemoryStorage struct {
	mu            sync.RWMutex
	locks         map[string]*Lock
	sessions      map[string]*Session
	fenceCounters map[string]uint64
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		locks:         make(map[string]*Lock),
		sessions:      make(map[string]*Session),
		fenceCounters: make(map[string]uint64),
	}
}

func (m *MemoryStorage) SaveLock(ctx context.Context, lock *Lock) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locks[lock.Name] = lock
	return nil
}
func (m *MemoryStorage) GetLock(ctx context.Context, name string) (*Lock, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lock, exists := m.locks[name]
	if !exists {
		return nil, fmt.Errorf("lock not found")
	}
	return lock, nil
}

func (m *MemoryStorage) DeleteLock(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, name)
	return nil
}

func (m *MemoryStorage) ListLocks(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lockNames := make([]string, 0, len(m.locks))
	for name := range m.locks {
		lockNames = append(lockNames, name)
	}
	return lockNames, nil
}

func (m *MemoryStorage) SaveSession(ctx context.Context, session *Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[session.ID.String()] = session
	return nil
}

func (m *MemoryStorage) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, exists := m.sessions[sessionID]
	if !exists {
		return nil, fmt.Errorf("session not found")
	}
	return session, nil
}

func (m *MemoryStorage) DeleteSession(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
	return nil
}

func (m *MemoryStorage) ListSessions(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessionIDs := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		sessionIDs = append(sessionIDs, id)
	}
	return sessionIDs, nil
}

func (m *MemoryStorage) IncrementFenceCounter(ctx context.Context, lockName string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenceCounters[lockName]++
	return m.fenceCounters[lockName], nil
}

func (m *MemoryStorage) Ping(ctx context.Context) error {
	return nil
}
