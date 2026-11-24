package tasks

import (
	"context"
	"fmt"
	"strconv"
	"taskflow/internal/redisresp"
)

const metricKey = "taskflow:metrics"
const buckets = "0.1,0.5,1,5,30,120"

const observeTerminal = `
local function observe(outcome)
  redis.pcall('HINCRBY', KEYS[3], outcome, 1)
  local created=tonumber(redis.call('HGET',KEYS[1],'created_ms'))
  if not created then return end
  local elapsed=math.max(0,now-created)
  redis.pcall('HINCRBY',KEYS[3],'latency_count',1)
  redis.pcall('HINCRBY',KEYS[3],'latency_ms_sum',elapsed)
  for bound in string.gmatch('` + buckets + `','[^,]+') do
    if elapsed<=tonumber(bound)*1000 then
      redis.pcall('HINCRBY',KEYS[3],'le_'..bound,1)
    end
  end
end
`

func (r *Repository) Metrics(ctx context.Context) (string, error) {
	counts, err := r.Redis.Do(ctx, "HGETALL", metricKey)
	if err != nil {
		return "", err
	}
	queue, err := r.Redis.Do(ctx, "XINFO", "GROUPS", Stream)
	if err != nil {
		return "", err
	}
	fields := pairs(counts)
	var out string
	out += "# TYPE taskflow_tasks_finished_total counter\n"
	for _, state := range []string{"succeeded", "failed"} {
		out += fmt.Sprintf("taskflow_tasks_finished_total{state=%q} %d\n", state, fields[state].Number)
	}
	out += "# TYPE taskflow_task_latency_seconds histogram\n"
	for _, bound := range []string{"0.1", "0.5", "1", "5", "30", "120"} {
		out += fmt.Sprintf("taskflow_task_latency_seconds_bucket{le=%q} %d\n", bound, fields["le_"+bound].Number)
	}
	count := fields["latency_count"].Number
	out += fmt.Sprintf("taskflow_task_latency_seconds_bucket{le=\"+Inf\"} %d\n", count)
	out += fmt.Sprintf("taskflow_task_latency_seconds_count %d\n", count)
	out += fmt.Sprintf("taskflow_task_latency_seconds_sum %.3f\n", float64(fields["latency_ms_sum"].Number)/1000)
	for _, entry := range queue.Items {
		info := pairs(entry)
		if info["name"].String() != Group {
			continue
		}
		out += fmt.Sprintf("# TYPE taskflow_queue_pending gauge\ntaskflow_queue_pending %d\n", info["pending"].Number)
		if lag := info["lag"]; lag.Kind == ':' {
			out += fmt.Sprintf("# TYPE taskflow_queue_lag gauge\ntaskflow_queue_lag %d\n", lag.Number)
		}
		break
	}
	return out, nil
}

func pairs(reply redisresp.Value) map[string]redisresp.Value {
	values := make(map[string]redisresp.Value, len(reply.Items)/2)
	for i := 0; i+1 < len(reply.Items); i += 2 {
		value := reply.Items[i+1]
		if number, err := strconv.ParseInt(value.Text, 10, 64); err == nil {
			value.Number = number
		}
		values[reply.Items[i].String()] = value
	}
	return values
}
