package coordinator

import (
	"context"
	"fmt"
	"go-rpc/internal/coordinator/storage"
	"go-rpc/internal/util"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.bryk.io/pkg/ulid"
)

type Coordinator struct {
	mu sync.RWMutex
	/*
		locks          map[string]*Lock
		sessions       map[ulid.ULID]*Session
		metrics        metrics.Metrics
	*/
	sessionAfw     time.Duration
	stopChan       chan struct{}
	sessionTimeout time.Duration
	storage        storage.Storage
	logger         util.Logger
	lockCache      map[string]*LockState
	sessionCache   map[ulid.ULID]*SessionState
}

type LockState struct {
	Lock  *storage.Lock
	Timer *time.Timer
}

type SessionState struct {
	Session  *storage.Session
	LastSeen time.Time
}

func NewCoordinator(store storage.Storage, sessionTimeout time.Duration) *Coordinator {
	c := &Coordinator{
		storage:        store,
		sessionTimeout: sessionTimeout,
		stopChan:       make(chan struct{}),
		lockCache:      make(map[string]*LockState),
		sessionCache:   make(map[ulid.ULID]*SessionState),
	}
	go c.cleanupAfws()
	return c
}

func (c *Coordinator) RegisterSession(ctx context.Context, clientID string) (sessionID ulid.ULID, expiresAt time.Time, err error) {
	if err := ctx.Err(); err != nil {
		return ulid.ULID{}, time.Time{}, fmt.Errorf("request cancelled or timed out: %v", err)
	}

	if clientID == "" {
		return ulid.ULID{}, time.Time{}, fmt.Errorf("client ID cannot be empty")
	}

	sessionID, err = ulid.New()
	if err != nil {
		return ulid.ULID{}, time.Time{}, fmt.Errorf("failed to generate session ID: %v", err)
	}
	expiresAt = time.Now().Add(c.sessionTimeout)
	session := &storage.Session{
		ID:        sessionID,
		ClientID:  clientID,
		LastSeen:  time.Now(),
		HeldLocks: make([]string, 0),
		CreatedAt: time.Now(),
	}

	if err := c.storage.SaveSession(ctx, session); err != nil {
		return ulid.ULID{}, time.Time{}, fmt.Errorf("failed to save session to storage: %v", err)
	}

	c.mu.Lock()
	c.sessionCache[sessionID] = &SessionState{
		Session:  session,
		LastSeen: time.Now(),
	}
	c.mu.Unlock()

	util.Log().Info("Session registered: %v (client: %s)", sessionID, clientID)

	return sessionID, expiresAt, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, sessionID ulid.ULID) (bool, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return false, time.Time{}, fmt.Errorf("request cancelled or timed out: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	state, exists := c.sessionCache[sessionID]
	if !exists {
		session, err := c.storage.GetSession(ctx, sessionID.String())
		if err != nil {
			return false, time.Time{}, fmt.Errorf("session not found: %v", err)
		}
		state = &SessionState{
			Session:  session,
			LastSeen: time.Now(),
		}
		c.sessionCache[sessionID] = state
	}

	state.Session.LastSeen = time.Now()
	expiresAt := state.Session.LastSeen.Add(c.sessionTimeout)
	if err := c.storage.SaveSession(ctx, state.Session); err != nil {
		return false, time.Time{}, fmt.Errorf("failed to update session in storage: %v", err)
	}

	return true, expiresAt, nil
}

func (c *Coordinator) AcquireLock(ctx context.Context, req AcquireLockRequest) (AcquireLockResponse, error) {
	if err := ctx.Err(); err != nil {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: fmt.Sprintf("Request cancelled or timed out: %v", err),
			},
		}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	sessionState, exists := c.sessionCache[req.SessionID]
	if !exists {
		session, err := c.storage.GetSession(ctx, req.SessionID.String())
		if err != nil {
			return AcquireLockResponse{
				Resp: GenericResponse{
					Success: false,
					Message: "Session not found",
				},
			}, nil
		}
		sessionState = &SessionState{
			Session:  session,
			LastSeen: time.Now(),
		}
		c.sessionCache[req.SessionID] = sessionState
	}
	lockState, lExists := c.lockCache[req.LockName]
	if !lExists {
		fenceToken, err := c.storage.IncrementFenceCounter(ctx, req.LockName)
		if err != nil {
			return AcquireLockResponse{
				Resp: GenericResponse{
					Success: false,
					Message: fmt.Sprintf("Failed to increment fence counter: %v", err),
				},
			}, nil
		}
		lock, err := c.storage.GetLock(ctx, req.LockName)
		lock.FenceToken = fenceToken
		if err == nil {
			lockState = &LockState{
				Lock: lock,
				Timer: time.AfterFunc(time.Until(lock.ExpiresAt), func() {
					c.expireLock(req.LockName, lock.Token)
				}),
			}
			c.lockCache[req.LockName] = lockState
		}
	}
	if lExists && lockState.Lock.Owner != req.SessionID {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "Lock is already held by another session",
			},
		}, nil
	}
	if lExists && lockState.Lock.Owner == req.SessionID {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: true,
				Message: "Lock already held by the session",
			},
			Token:      lockState.Lock.Token,
			FenceToken: lockState.Lock.FenceToken,
			ExpiresAt:  lockState.Lock.ExpiresAt,
		}, nil
	}

	fenceToken, err := c.storage.IncrementFenceCounter(ctx, req.LockName)
	if err != nil {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to increment fence counter: %v", err),
			},
		}, nil
	}
	token, err := uuid.NewRandom()
	if err != nil {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to generate lock token: %v", err),
			},
		}, nil
	}
	expiresAt := time.Now().Add(req.Timeout)
	createdAt := time.Now()
	lock := &storage.Lock{
		Name:       req.LockName,
		Owner:      req.SessionID,
		Token:      token.String(),
		FenceToken: fenceToken,
		ExpiresAt:  expiresAt,
		CreatedAt:  createdAt,
	}
	if err := c.storage.SaveLock(ctx, lock); err != nil {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to save lock to storage: %v", err),
			},
		}, nil
	}
	timer := time.AfterFunc(req.Timeout, func() {
		c.expireLock(req.LockName, lock.Token)
	})
	lockState = &LockState{
		Lock:  lock,
		Timer: timer,
	}

	c.lockCache[req.LockName] = lockState
	sessionState.Session.HeldLocks = append(sessionState.Session.HeldLocks, req.LockName)

	if err := c.storage.SaveSession(ctx, sessionState.Session); err != nil {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to update session in storage: %v", err),
			},
		}, nil
	}
	util.Log().Info("Lock %s acquired by session %v", req.LockName, req.SessionID)
	return AcquireLockResponse{
		Resp: GenericResponse{
			Success: true,
			Message: "Lock acquired successfully",
		},
		Token:      lock.Token,
		FenceToken: lock.FenceToken,
		ExpiresAt:  lock.ExpiresAt,
	}, nil
}

func (c *Coordinator) ReleaseLock(ctx context.Context, req ReleaseLockRequest) (ReleaseLockResponse, error) {
	if err := ctx.Err(); err != nil {
		return ReleaseLockResponse{}, fmt.Errorf("request cancelled or timed out: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	lock, exists := c.lockCache[req.LockName]
	if !exists {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "Lock does not exists",
			},
		}, nil
	}

	if lock.Lock.Owner != req.SessionID {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "The client does not hold the lock",
			},
		}, nil
	}

	if lock.Lock.Token != req.Token {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "Invalid token",
			},
		}, nil
	}

	lock.Timer.Stop()
	if err := c.storage.DeleteLock(ctx, req.LockName); err != nil {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to delete lock from storage: %v", err),
			},
		}, nil
	}
	delete(c.lockCache, req.LockName)

	util.Log().Debug("Deleted the lock %s at %T", lock.Lock.Name, time.Now())

	if sessionState, Sexists := c.sessionCache[req.SessionID]; Sexists {
		sessionState.Session.HeldLocks = util.RemoveStr(sessionState.Session.HeldLocks, lock.Lock.Name)
		c.storage.SaveSession(ctx, sessionState.Session)
	}

	util.Log().Debug("Lock %s released by session %v", req.LockName, req.SessionID)

	return ReleaseLockResponse{
		Resp: GenericResponse{
			Success: true,
			Message: "Lock released succesfully",
		},
	}, nil
}

func (c *Coordinator) expireLock(lockName, token string) {
	ctx := context.Background()
	if err := ctx.Err(); err != nil {
		util.Log().Debug("Context error while expiring lock %s: %v", lockName, err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	lockState, lExists := c.lockCache[lockName]
	if !lExists {
		util.Log().Debug("Lock %s was already released", lockName)
		return
	}
	if lockState.Lock.Token != token {
		util.Log().Debug("Provided token %s was different from lock's %s", token, lockState.Lock.Token)
		return
	}

	util.Log().Info("Lock expired: %s was held by session %v", lockName, lockState.Lock.Owner)

	c.storage.DeleteLock(ctx, lockName)

	delete(c.lockCache, lockName)

	if session, Sexists := c.sessionCache[lockState.Lock.Owner]; Sexists {
		session.Session.HeldLocks = util.RemoveStr(session.Session.HeldLocks, lockName)
		c.storage.SaveSession(ctx, session.Session)
	}
}

func (c *Coordinator) cleanupAfws() {
	ticker := time.NewTicker(c.sessionAfw)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			util.Log().Debug("Starting afws session cleanup")
			c.sessionCleanupProc()
			util.Log().Debug("Completed afws session cleanup")
		case <-c.stopChan:
			// application graceful shutdown
			return
		}
	}
}

func (c *Coordinator) sessionCleanupProc() {
	ctx := context.Background()
	if err := ctx.Err(); err != nil {
		util.Log().Debug("Context error during afw session cleanup: %v", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	staleSessionIds := make([]ulid.ULID, 0)

	for sessionID, state := range c.sessionCache {
		if now.Sub(state.Session.LastSeen) > c.sessionTimeout {
			staleSessionIds = append(staleSessionIds, sessionID)
		}
	}

	for _, sessionID := range staleSessionIds {
		state := c.sessionCache[sessionID]

		util.Log().Info("Cleaning up afw session %v", sessionID)

		for _, lockName := range state.Session.HeldLocks {
			if lockState, lExists := c.lockCache[lockName]; lExists {
				lockState.Timer.Stop() // as to avoid side effects on later phases
				delete(c.lockCache, lockName)
				util.Log().Info("Auto released lock %s from session %v (afw)", lockName, sessionID)
			}
		}

		c.storage.DeleteLock(ctx, sessionID.String())
		delete(c.sessionCache, sessionID)
	}
}

func (c *Coordinator) clearSessions() {
	if len(c.sessionCache) > 0 {
		for _, state := range c.sessionCache {
			c.mu.Lock()
			delete(c.sessionCache, state.Session.ID)
			c.storage.DeleteSession(context.Background(), state.Session.ID.String())
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) clearLocks() {
	if len(c.lockCache) > 0 {
		for _, lockState := range c.lockCache {
			c.mu.Lock()
			lockState.Timer.Stop()
			c.storage.DeleteLock(context.Background(), lockState.Lock.Name)
			delete(c.lockCache, lockState.Lock.Name)
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) Stop() {
	close(c.stopChan)

	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.lockCache) > 0 || len(c.sessionCache) > 0 {
		c.clearLocks()
		c.clearSessions()
	}
}
