package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStorage struct {
	client *redis.Client
}

func NewRedisStorage(addr string, password string, db int) *RedisStorage {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     10,
		DialTimeout:  5 * 1e9, // 5 seconds
		ReadTimeout:  3 * 1e9, // 3 seconds
		WriteTimeout: 3 * 1e9, // 3 seconds
	})
	return &RedisStorage{
		client: rdb,
	}
}

func (r *RedisStorage) SaveLock(ctx context.Context, lock *Lock) error {
	data, err := json.Marshal(lock)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("lock:%s", lock.Name)
	ttl := time.Until(lock.ExpiresAt)
	if ttl <= 0 {
		ttl = 0
	}

	if err := r.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return err
	}

	if err := r.client.SAdd(ctx, "locks:index", lock.Name).Err(); err != nil {
		return err
	}

	return nil
}

func (r *RedisStorage) GetLock(ctx context.Context, name string) (*Lock, error) {
	key := fmt.Sprintf("lock:%s", name)
	data, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("lock not found")
	}
	if err != nil {
		return nil, err
	}
	var lock Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, err
	}

	return &lock, nil
}

func (r *RedisStorage) DeleteLock(ctx context.Context, name string) error {
	key := fmt.Sprintf("lock:%s", name)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	if err := r.client.SRem(ctx, "locks:index", name).Err(); err != nil {
		return err
	}

	return nil
}

func (r *RedisStorage) ListLocks(ctx context.Context) ([]string, error) {
	lockNames, err := r.client.SMembers(ctx, "locks:index").Result()
	if err != nil {
		return nil, err
	}
	return lockNames, nil
}

func (r *RedisStorage) SaveSession(ctx context.Context, session *Session) error {
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("session:%s", session.ID.String())
	ttl := time.Until(session.LastSeen.Add(60 * time.Second))
	if ttl <= 0 {
		ttl = 0
	}
	if err := r.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return err
	}
	if err := r.client.SAdd(ctx, "sessions:index", session.ID.String()).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisStorage) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	key := fmt.Sprintf("session:%s", sessionID)
	data, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("session not found")
	}
	if err != nil {
		return nil, err
	}
	var session Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, err
	}
	return &session, nil
}

func (r *RedisStorage) DeleteSession(ctx context.Context, sessionID string) error {
	key := fmt.Sprintf("session:%s", sessionID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	if err := r.client.SRem(ctx, "sessions:index", sessionID).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisStorage) ListSessions(ctx context.Context) ([]string, error) {
	sessionIDs, err := r.client.SMembers(ctx, "sessions:index").Result()
	if err != nil {
		return nil, err
	}
	return sessionIDs, nil
}

func (r *RedisStorage) IncrementFenceCounter(ctx context.Context, lockName string) (uint64, error) {
	key := fmt.Sprintf("lock:%s:fence", lockName)
	newValue, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return uint64(newValue), nil
}

func (r *RedisStorage) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RedisStorage) Close() error {
	return r.client.Close()
}
