package coordinator

import (
	"time"

	"github.com/google/uuid"
)

type GenericResponse struct {
	Success bool
	Message string
}

type AcquireLockRequest struct {
	LockName  string
	SessionID string
	Timeout   time.Duration
	WaitTime  time.Duration
}

type AcquireLockResponse struct {
	Resp      GenericResponse
	Token     uuid.UUID
	ExpiresAt time.Time
}

type ReleaseLockRequest struct {
	LockName  string
	Token     uuid.UUID
	SessionID string
}

type ReleaseLockResponse struct {
	Resp GenericResponse
}

type HeartbeatRequest struct {
	SessionID string
	Token     uuid.UUID
}

type HeartbeatResponse struct {
	Resp GenericResponse
}

type RegisterSessionRequest struct {
	ClientID string
}

type RegisterSessionResponse struct {
	SessionID string
	ExpiresAt time.Time
}
