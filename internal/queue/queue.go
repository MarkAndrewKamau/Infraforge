// Package queue is the async hand-off between the broker (producer) and
// the worker (consumer, Phase 3).
//
// It is built on a Redis Stream + consumer group rather than a plain list
// (LPUSH/BRPOP). The reason is delivery guarantees:
//
//	LPUSH/BRPOP : the moment BRPOP returns, the item is gone from Redis.
//	              If the worker crashes before finishing, the job is lost.
//
//	Stream+group: XREADGROUP hands the entry to a consumer but keeps it in
//	              the group's Pending Entries List (PEL) until XACK. A
//	              crashed worker's in-flight job can be reclaimed (XCLAIM,
//	              Phase 7) instead of vanishing. That is at-least-once
//	              delivery, which is what provisioning needs.
package queue

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// StreamKey is the Redis Stream all provisioning jobs flow through.
	StreamKey = "infraforge:jobs"
	// Group is the consumer group the worker(s) read under.
	Group = "provisioners"
)

// Message is one job reference pulled off the stream. ID is the Redis
// stream entry ID and is required to XACK it.
type Message struct {
	ID    string
	JobID string
}

type Queue interface {
	// Enqueue appends a job reference to the stream.
	Enqueue(ctx context.Context, jobID string) error
	// Dequeue blocks up to block for the next unread message for this
	// consumer. Returns (nil, nil) if the block elapsed with nothing.
	Dequeue(ctx context.Context, consumer string, block time.Duration) (*Message, error)
	// Ack removes a message from the group's pending list.
	Ack(ctx context.Context, m *Message) error
}

type RedisQueue struct{ rdb *redis.Client }

func NewRedis(rdb *redis.Client) *RedisQueue { return &RedisQueue{rdb: rdb} }

// EnsureGroup creates the stream + consumer group if absent. Idempotent:
// a re-run just hits BUSYGROUP, which we treat as success.
func (q *RedisQueue) EnsureGroup(ctx context.Context) error {
	err := q.rdb.XGroupCreateMkStream(ctx, StreamKey, Group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey,
		Values: map[string]any{"job_id": jobID},
	}).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context, consumer string, block time.Duration) (*Message, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    Group,
		Consumer: consumer,
		Streams:  []string{StreamKey, ">"}, // ">" = entries never delivered to this group
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // block elapsed, nothing to do
	}
	if err != nil {
		return nil, err
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return nil, nil
	}
	m := res[0].Messages[0]
	jobID, _ := m.Values["job_id"].(string)
	return &Message{ID: m.ID, JobID: jobID}, nil
}

func (q *RedisQueue) Ack(ctx context.Context, m *Message) error {
	return q.rdb.XAck(ctx, StreamKey, Group, m.ID).Err()
}
