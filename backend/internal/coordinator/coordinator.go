// there should be a context for the requests - the code flows like fasthttp-ish, so it would make sense to have the code similarly wrote

package coordinator

import (
	"fmt"
	"go-rpc/internal/util"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.bryk.io/pkg/ulid"
)

type Session struct {
	ID         ulid.ULID
	ClientID   string
	ClientInfo rpc.Client
	LastSeen   time.Time
	HeldLocks  []string
	CreatedAt  time.Time
}

type Lock struct {
	Name      string
	Owner     ulid.ULID
	Token     string
	ExpiresAt time.Time
	Timer     *time.Timer
}

type Coordinator struct {
	mu             sync.RWMutex
	locks          map[string]*Lock
	sessions       map[ulid.ULID]*Session
	sessionTimeout time.Duration
	sessionAfw     time.Duration
	stopChan       chan struct{}
}

func NewCoordinator(sessionTimeout time.Duration) *Coordinator {
	c := &Coordinator{
		locks:          make(map[string]*Lock),
		sessions:       make(map[ulid.ULID]*Session),
		sessionTimeout: sessionTimeout,
		stopChan:       make(chan struct{}),
	}
	go c.cleanupAfws()
	return c
}

func (c *Coordinator) RegisterSession(clientID string) (sessionID ulid.ULID, expiresAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sessionID, err := ulid.New()
	if err != nil {
		return ulid.ULID{}, time.Time{}
	}
	expiresAt = time.Now().Add(c.sessionTimeout)

	c.sessions[sessionID] = &Session{
		ID:        sessionID,
		ClientID:  clientID,
		LastSeen:  time.Now(),
		HeldLocks: make([]string, 0),
		CreatedAt: time.Now(),
	}

	util.Log().Info("Session registered: %v (client: %s)", sessionID, clientID)

	return sessionID, expiresAt
}

func (c *Coordinator) Heartbeat(sessionID ulid.ULID) (bool, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	session, exists := c.sessions[sessionID]
	if !exists {
		return false, time.Time{}
	}

	session.LastSeen = time.Now()
	expiresAt := session.LastSeen.Add(c.sessionTimeout)

	return true, expiresAt
}

func (c *Coordinator) AcquireLock(req AcquireLockRequest) AcquireLockResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	session, exists := c.sessions[req.SessionID]
	if !exists {
		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "This session wasn't found",
			},
		}
	}

	lock, lockExists := c.locks[req.LockName]
	if lockExists {
		if lock.Owner == req.SessionID {
			return AcquireLockResponse{
				Resp: GenericResponse{
					Success: true,
					Message: "Lock already held by the client",
				},
				Token:     lock.Token,
				ExpiresAt: lock.ExpiresAt, // shouldnt the expires at be redefined here with a time add approach starting from now
			}
		}
		if req.WaitTime > 0 { // time for the lock to be released by its owner before getting accessed
			// should do a Futex.wake for this call for the 'sleeping' service to access this right after the lock is released by the first session
			return AcquireLockResponse{
				Resp: GenericResponse{
					Success: false,
					Message: fmt.Sprintf("Lock currently held by another client. Wait %T before trying again", req.WaitTime),
				},
			}
		}

		return AcquireLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "Lock being currently held",
			},
		}

	}

	token := uuid.New().String()
	expiresAt := time.Now().Add(req.Timeout) // do the request parse with criteria - ofc timeout <= max allowed

	timer := time.AfterFunc(req.Timeout, func() {
		c.expireLock(req.LockName, token)
	})

	c.locks[req.LockName] = &Lock{
		Name:      req.LockName,
		Owner:     req.SessionID,
		Token:     token,
		ExpiresAt: expiresAt,
		Timer:     timer,
	}

	session.HeldLocks = append(session.HeldLocks, req.LockName)

	util.Log().Info("Lock acquired: %s by session %v, expiring at %T")

	return AcquireLockResponse{
		Resp: GenericResponse{
			Success: true,
			Message: "Lock acquired successfully",
		},
		Token:     token,
		ExpiresAt: expiresAt,
	}
}

func (c *Coordinator) ReleaseLock(req ReleaseLockRequest) ReleaseLockResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	lock, exists := c.locks[req.LockName]
	if !exists {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "Lock does not exists",
			},
		}
	}

	if lock.Owner != req.SessionID {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "The client does not hold the lock",
			},
		}
	}

	if lock.Token != req.Token {
		return ReleaseLockResponse{
			Resp: GenericResponse{
				Success: false,
				Message: "Invalid token",
			},
		}
	}

	lock.Timer.Stop()
	delete(c.locks, req.LockName)
	util.Log().Debug("Deleted the lock %s at %T", lock.Name, time.Now())

	if session, exists := c.sessions[req.SessionID]; exists {
		session.HeldLocks = util.RemoveStr(session.HeldLocks, lock.Name)
	}
	util.Log().Debug("Lock %s released by session %v", req.LockName, req.SessionID)

	return ReleaseLockResponse{
		Resp: GenericResponse{
			Success: true,
			Message: "Lock released succesfully",
		},
	}
}

func (c *Coordinator) expireLock(lockName, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lock, exists := c.locks[lockName]
	if !exists {
		util.Log().Debug("Lock %s was already released", lockName)
		return
	}
	if lock.Token != token {
		util.Log().Debug("Provided token %s was different from lock's %s", token, lock.Token)
		return
	}

	util.Log().Info("Lock expired: %s was held by session %v", lockName, lock.Owner)

	if session, exists := c.sessions[lock.Owner]; exists {
		session.HeldLocks = util.RemoveStr(session.HeldLocks, lockName)
	}

	delete(c.locks, lockName)
}

func (c *Coordinator) cleanupAfws() {
	ticker := time.NewTicker(c.sessionAfw)
	defer ticker.Stop()

	c.mu.Lock()
	defer c.mu.Unlock()
	// looks like a gc stop the world approach running endlessly

	for {
		select {
		case <-ticker.C:
			c.sessionCleanupProc()
		case <-c.stopChan:
			// application graceful shutdown
			return
		}
	}
}

func (c *Coordinator) sessionCleanupProc() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	staleSessionIds := make([]ulid.ULID, 0)

	for sessionID, session := range c.sessions {
		if now.Sub(session.LastSeen) > c.sessionTimeout {
			staleSessionIds = append(staleSessionIds, sessionID)
		}
	}

	for _, sessionID := range staleSessionIds {
		session := c.sessions[sessionID]

		util.Log().Info("Cleaning up afw session %v", sessionID)

		for _, lockName := range session.HeldLocks {
			if lock, exists := c.locks[lockName]; exists {
				lock.Timer.Stop() // as to avoid side effects on later phases
				delete(c.locks, lockName)
				util.Log().Info("Auto released lock %s from session %v (afw)", lockName, sessionID)
			}
		}

		delete(c.sessions, sessionID)
	}
}

func (c *Coordinator) clearSessions() {
	if len(c.sessions) > 0 {
		for _, session := range c.sessions {
			delete(c.sessions, session.ID)
		}
	}
}

func (c *Coordinator) clearLocks() {
	if len(c.locks) > 0 {
		for _, lock := range c.locks {
			delete(c.locks, lock.Name)
		}
	}
}

func (c *Coordinator) Stop() {
	close(c.stopChan)

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, lock := range c.locks {
		// nested, can be improved
		lock.Timer.Stop()
		// also avoids side effects
		c.clearLocks()
		c.clearSessions()
	}
}

func (c *Coordinator) GetStats() StatsServer {
	c.mu.Lock()
	defer c.mu.Unlock()

	return StatsServer{
		ActiveSessions: len(c.sessions),
		ActiveLocks:    len(c.locks),
	}
}
