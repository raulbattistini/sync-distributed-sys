// there should be a context for the requests - the code flows like fasthttp-ish, so it would make sense to have the code similarly wrote

package coordinator

import (
	"go-rpc/internal/util"
	"net/rpc"
	"sync"
	"time"

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
	Owner     string
	Token     string
	ExpiresAt time.Time
	Timer     *time.Timer
}

type Coordinator struct {
	mu             sync.RWMutex
	locks          map[string]*Lock
	sessions       map[ulid.ULID]*Session
	sessionTimeout time.Duration
	stopChan       chan struct{}
}

func NewCoordinator(sessionTimeout time.Duration) *Coordinator {
	c := &Coordinator{
		locks:          make(map[string]*Lock),
		sessions:       make(map[ulid.ULID]*Session),
		sessionTimeout: sessionTimeout,
		stopChan:       make(chan struct{}),
	}
	// go c.cleanupAfws()
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

func (c *Coordinator) AcquireLock(req AcquireLockRequest, resp AcquireLockResponse)
