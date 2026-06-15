// Package queue is the async hand-off between the broker (producer) and
// the worker (consumer).
//
// It is built on a Redis Stream + consumer group rather than a plain list
// (LPUSH/BRPOP). The reason is delivery guarantees:
//
//	LPUSH/BRPOP : the moment BRPOP returns, the item is gone from Redis.
//	              If the worker crashes before finishing, the job is lost.
//
//	Stream+group: XREADGROUP hands the entry to a consumer but keeps it in
//	              the group's Pending Entries List (PEL) until XACK. A
//	              crashed worker's in-flight job stays in the PEL and can
//	              be reclaimed (XAUTOCLAIM) by a live worker. That is
//	              at-least-once delivery, which is what provisioning needs.
//
// Each message also carries an Action, so the same stream can carry both
// "provision this job" and "tear this job down" work items.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// StreamKey is the Redis Stream all job work items flow through.
	StreamKey = "infraforge:jobs"
	// Group is the consumer group the worker(s) read under.
	Group = "provisioners"
	// DeadStreamKey is the parallel stream where abandoned jobs land for
	// post-mortem inspection. It is intentionally not consumed by anyone;
	// it is an audit log.
	DeadStreamKey = "infraforge:jobs:dead"
	// deadStreamMaxLen caps the dead-letter stream so a long-running
	// learning system does not accumulate entries forever. Approximate
	// trimming so the actual length may slightly exceed this.
	deadStreamMaxLen = 1000
)

// Action is what the worker should do with a job.
type Action string

const (
	ActionProvision   Action = "provision"
	ActionDeprovision Action = "deprovision"
)

// Message is one work item pulled off the stream. ID is the Redis stream
// entry ID and is required to XACK it.
type Message struct {
	ID     string
	JobID  string
	Action Action
}

type Queue interface {
	// Enqueue appends a work item to the stream.
	Enqueue(ctx context.Context, jobID string, action Action) error
	// Dequeue blocks up to block for the next unread message for this
	// consumer. Returns (nil, nil) if the block elapsed with nothing.
	Dequeue(ctx context.Context, consumer string, block time.Duration) (*Message, error)
	// Reclaim takes ownership of messages that have been pending
	// (delivered but unacked) longer than minIdle, reassigning them to
	// consumer. This is the recovery path for a worker that died holding
	// a message. Returns up to count messages.
	Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int) ([]*Message, error)
	// Ack removes a message from the group's pending list.
	Ack(ctx context.Context, m *Message) error
	// DeadLetter writes an entry to the dead-letter stream describing an
	// abandoned job. The caller is responsible for the corresponding Ack
	// on the main stream — DeadLetter only writes the audit record.
	DeadLetter(ctx context.Context, m *Message, detail string, attempts int) error
	// ListDead returns up to limit recent dead-letter entries, oldest
	// first. limit <= 0 means a sensible default.
	ListDead(ctx context.Context, limit int) ([]*DeadEntry, error)
}

// DeadEntry is one record in the dead-letter stream.
type DeadEntry struct {
	StreamID  string    `json:"stream_id"`
	JobID     string    `json:"job_id"`
	Action    Action    `json:"action"`
	Detail    string    `json:"detail"`
	Attempts  int       `json:"attempts"`
	Timestamp time.Time `json:"timestamp"`
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

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string, action Action) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey,
		Values: map[string]any{"job_id": jobID, "action": string(action)},
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
	return messageFrom(res[0].Messages[0]), nil
}

func (q *RedisQueue) Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int) ([]*Message, error) {
	// Start "0-0" rescans the PEL from the beginning each call. Acked
	// messages have already left the PEL, so this converges; we do not
	// need to thread the returned cursor across calls.
	entries, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   StreamKey,
		Group:    Group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(count),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*Message, 0, len(entries))
	for _, e := range entries {
		out = append(out, messageFrom(e))
	}
	return out, nil
}

func (q *RedisQueue) Ack(ctx context.Context, m *Message) error {
	return q.rdb.XAck(ctx, StreamKey, Group, m.ID).Err()
}

func (q *RedisQueue) DeadLetter(ctx context.Context, m *Message, detail string, attempts int) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DeadStreamKey,
		MaxLen: deadStreamMaxLen,
		Approx: true, // Approximate trimming is faster and good enough for an audit log.
		Values: map[string]any{
			"job_id":    m.JobID,
			"action":    string(m.Action),
			"detail":    detail,
			"attempts":  attempts,
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		},
	}).Err()
}

func (q *RedisQueue) ListDead(ctx context.Context, limit int) ([]*DeadEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := q.rdb.XRangeN(ctx, DeadStreamKey, "-", "+", int64(limit)).Result()
	if err != nil {
		// A missing stream (no XADDs ever made it) is just an empty list.
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]*DeadEntry, 0, len(res))
	for _, m := range res {
		e := &DeadEntry{StreamID: m.ID}
		if v, ok := m.Values["job_id"].(string); ok {
			e.JobID = v
		}
		if v, ok := m.Values["action"].(string); ok {
			e.Action = Action(v)
		}
		if v, ok := m.Values["detail"].(string); ok {
			e.Detail = v
		}
		if v, ok := m.Values["attempts"].(string); ok {
			_, _ = fmt.Sscanf(v, "%d", &e.Attempts)
		}
		if v, ok := m.Values["timestamp"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				e.Timestamp = t
			}
		}
		out = append(out, e)
	}
	return out, nil
}

// messageFrom decodes a raw stream entry. A missing action field is
// treated as provision, so entries written before actions existed (and
// any hand-crafted XADDs) still work.
func messageFrom(m redis.XMessage) *Message {
	jobID, _ := m.Values["job_id"].(string)
	action, _ := m.Values["action"].(string)
	if action == "" {
		action = string(ActionProvision)
	}
	return &Message{ID: m.ID, JobID: jobID, Action: Action(action)}
}
