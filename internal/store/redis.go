package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/redis/go-redis/v9"
)

// jobKeyPrefix namespaces every job record. List scans on it.
const jobKeyPrefix = "infraforge:job:"

func jobKey(id string) string { return jobKeyPrefix + id }

// RedisStore persists each job as a JSON string at infraforge:job:<id>.
//
// A JSON blob (not a Redis hash) is deliberate: jobs are read and written
// whole, never field-by-field, so one Set/Get per job is the simplest
// correct thing.
//
// Records carry no TTL. A job record maps to a real, long-lived container;
// expiring the record would orphan the container from the control plane's
// inventory. Records are removed only by an explicit deprovision (which
// leaves the job in the terminal "deleted" state rather than deleting the
// key, so a later GET still reports the outcome).
type RedisStore struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

func (s *RedisStore) Put(ctx context.Context, j *model.Job) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, jobKey(j.ID), b, 0).Err()
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

// List enumerates every job record. It uses SCAN, not KEYS: SCAN is
// cursor-based and does not block Redis while it walks the keyspace. For
// a learning-scale system the per-key GET loop is fine; a larger system
// would maintain a secondary index set instead of scanning.
func (s *RedisStore) List(ctx context.Context) ([]*model.Job, error) {
	var (
		jobs   []*model.Job
		cursor uint64
	)
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, jobKeyPrefix+"*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		for _, k := range keys {
			b, err := s.rdb.Get(ctx, k).Bytes()
			if errors.Is(err, redis.Nil) {
				continue // raced with a concurrent change; skip
			}
			if err != nil {
				return nil, fmt.Errorf("get %s: %w", k, err)
			}
			var j model.Job
			if err := json.Unmarshal(b, &j); err != nil {
				return nil, fmt.Errorf("decode %s: %w", k, err)
			}
			jobs = append(jobs, &j)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return jobs, nil
}
