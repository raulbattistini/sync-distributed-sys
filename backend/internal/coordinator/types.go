package coordinator

import (
	"time"

	"github.com/google/uuid"
	"go.bryk.io/pkg/ulid"
)

// next phase: return granted/denied/grant-expired in the response instead of a bool

type GenericResponse struct {
	Success bool
	Message string
}

type AcquireLockRequest struct {
	LockName  string
	SessionID ulid.ULID // later on it will be useful to order the sessions time-wise
	Timeout   time.Duration
	WaitTime  time.Duration
}

type AcquireLockResponse struct {
	Resp      GenericResponse
	Token     string // meant to work like an api key
	ExpiresAt time.Time
}

type ReleaseLockRequest struct {
	LockName  string // despite using uuids in this field, keeping some flexibility as for this phase
	Token     string
	SessionID ulid.ULID
}

type ReleaseLockResponse struct {
	Resp GenericResponse
}

type HeartbeatRequest struct {
	SessionID ulid.ULID
	Token     uuid.UUID
}

type HeartbeatResponse struct {
	Resp      GenericResponse
	ExpiresAt time.Time
}

type RegisterSessionRequest struct {
	ClientID string
}

type RegisterSessionResponse struct {
	SessionID ulid.ULID
	ExpiresAt time.Time
}

type StatsServer struct {
	ActiveSessions int
	ActiveLocks    int
}
