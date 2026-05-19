package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/redis/go-redis/v9"
)

// RedisStore persists each job as a JSON string at infraforge:job:<id>.
//
// A JSON blob (not a Redis hash) is deliberate: jobs are read and written
// whole, never field-by-field, so one Set/Get per job is the simplest
// correct thing. A TTL keeps a local learning box from accumulating
// dead job keys forever.
type RedisStore struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewRedis(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb, ttl: 24 * time.Hour}
}

func jobKey(id string) string { return "infraforge:job:" + id }

func (s *RedisStore) Put(ctx context.Context, j *model.Job) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, jobKey(j.ID), b, s.ttl).Err()
}

func (s *RedisStore) Get(ctx context.Context, id string) (*model.Job, error) {
	b, err := s.rdb.Get(ctx, jobKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var j model.Job
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, err
	}
	return &j, nil
}
