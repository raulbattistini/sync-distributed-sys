package storage

import (
	"context"
	"net/rpc"
	"time"

	"go.bryk.io/pkg/ulid"
)

type Storage interface {
	SaveLock(ctx context.Context, lock *Lock) error
	GetLock(ctx context.Context, name string) (*Lock, error)
	DeleteLock(ctx context.Context, name string) error
	ListLocks(ctx context.Context) ([]string, error)

	SaveSession(ctx context.Context, session *Session) error
	GetSession(ctx context.Context, sessionID string) (*Session, error)
	DeleteSession(ctx context.Context, sessionID string) error
	ListSessions(ctx context.Context) ([]string, error)

	IncrementFenceCounter(ctx context.Context, lockName string) (uint64, error)
	Ping(ctx context.Context) error
}

type Lock struct {
	Name       string
	Owner      ulid.ULID
	Token      string
	FenceToken uint64
	ExpiresAt  time.Time
	CreatedAt  time.Time
}

type Session struct {
	ID         ulid.ULID
	ClientID   string
	ClientInfo rpc.Client
	LastSeen   time.Time
	HeldLocks  []string
	CreatedAt  time.Time
}
