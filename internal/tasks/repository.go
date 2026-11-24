package tasks

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"taskflow/internal/redisresp"
)

const (
	Stream = "taskflow:stream"
	Group  = "taskflow:workers"
)

var ErrNotFound = errors.New("task not found")

type Task struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Attempts    int    `json:"attempts"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	Error       string `json:"error,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
	CreatedMS   int64  `json:"created_ms"`
	StartedMS   int64  `json:"started_ms,omitempty"`
	FinishedMS  int64  `json:"finished_ms,omitempty"`
	InputFile   string `json:"-"`
	ResultFile  string `json:"-"`
}

type Repository struct{ Redis *redisresp.Client }

func New(redis *redisresp.Client) *Repository { return &Repository{Redis: redis} }
func Key(id string) string                    { return "taskflow:task:" + id }

func ValidID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func (r *Repository) Get(ctx context.Context, id string) (Task, error) {
	v, err := r.Redis.Do(ctx, "HGETALL", Key(id))
	if err != nil {
		return Task{}, err
	}
	if v.Kind != '*' || len(v.Items) == 0 {
		return Task{}, ErrNotFound
	}
	if len(v.Items)%2 != 0 {
		return Task{}, errors.New("invalid Redis HGETALL reply")
	}
	m := make(map[string]string, len(v.Items)/2)
	for i := 0; i < len(v.Items); i += 2 {
		m[v.Items[i].String()] = v.Items[i+1].String()
	}
	attempts, _ := strconv.Atoi(m["attempts"])
	t := Task{ID: m["id"], Status: m["status"], Attempts: attempts, CreatedAt: m["created_at"], UpdatedAt: m["updated_at"], Error: m["error"], InputFile: m["input_file"], ResultFile: m["result_file"]}
	t.CreatedMS, _ = strconv.ParseInt(m["created_ms"], 10, 64)
	t.StartedMS, _ = strconv.ParseInt(m["started_ms"], 10, 64)
	t.FinishedMS, _ = strconv.ParseInt(m["finished_ms"], 10, 64)
	if t.Status == "succeeded" && t.ResultFile != "" {
		t.DownloadURL = "/tasks/" + t.ID + "/download"
	}
	return t, nil
}

func (r *Repository) Ready(ctx context.Context) error {
	v, err := r.Redis.Do(ctx, "PING")
	if err != nil {
		return err
	}
	if v.String() != "PONG" {
		return fmt.Errorf("unexpected redis ping: %s", v.String())
	}
	return nil
}

func (r *Repository) EnsureGroup(ctx context.Context) error {
	_, err := r.Redis.Do(ctx, "XGROUP", "CREATE", Stream, Group, "0", "MKSTREAM")
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (r *Repository) Ack(ctx context.Context, msgID string) error {
	_, err := r.Redis.Do(ctx, "XACK", Stream, Group, msgID)
	return err
}

type Message struct {
	StreamID string
	TaskID   string
}

// Parse the nested Redis reply from XREADGROUP or XAUTOCLAIM.
func parseMessages(records redisresp.Value) ([]Message, error) {
	if records.Kind != '*' || records.Null {
		return nil, nil
	}
	messages := make([]Message, 0, len(records.Items))
	for _, record := range records.Items {
		if len(record.Items) != 2 {
			return nil, errors.New("invalid stream message")
		}
		fields := record.Items[1].Items
		if len(fields)%2 != 0 {
			return nil, errors.New("invalid stream fields")
		}
		var taskID string
		for i := 0; i < len(fields); i += 2 {
			if fields[i].String() == "task_id" {
				taskID = fields[i+1].String()
			}
		}
		messages = append(messages, Message{StreamID: record.Items[0].String(), TaskID: taskID})
	}
	return messages, nil
}
func (r *Repository) Read(ctx context.Context, consumer string, block bool) ([]Message, error) {
	args := []string{"XREADGROUP", "GROUP", Group, consumer, "COUNT", "1"}
	if block {
		args = append(args, "BLOCK", "1000")
	}
	args = append(args, "STREAMS", Stream, ">")
	v, err := r.Redis.Do(ctx, args...)
	if err != nil {
		return nil, err
	}
	if v.Null {
		return nil, nil
	}
	if len(v.Items) == 0 {
		return nil, nil
	}
	if len(v.Items[0].Items) != 2 {
		return nil, errors.New("invalid XREADGROUP response")
	}
	return parseMessages(v.Items[0].Items[1])
}
func (r *Repository) Reclaim(ctx context.Context, consumer string, idle time.Duration, cursor string) (string, []Message, error) {
	v, err := r.Redis.Do(ctx, "XAUTOCLAIM", Stream, Group, consumer, strconv.FormatInt(idle.Milliseconds(), 10), cursor, "COUNT", "1")
	if err != nil {
		return cursor, nil, err
	}
	if len(v.Items) < 2 {
		return cursor, nil, errors.New("invalid XAUTOCLAIM response")
	}
	messages, err := parseMessages(v.Items[1])
	return v.Items[0].String(), messages, err
}
