package tasks

import (
	"context"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"
)

const redisClock = `
local t=redis.call('TIME')
local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
`

const createLua = redisClock + `
if redis.call('EXISTS',KEYS[1]) == 1 then return 0 end
redis.call('XADD',KEYS[2], '*', 'task_id',ARGV[1])
redis.call('HSET',KEYS[1], 'id',ARGV[1], 'status','queued', 'attempts','0',
  'input_file',ARGV[2], 'created_at',ARGV[3], 'updated_at',ARGV[3], 'created_ms',now)
return 1
`

func (r *Repository) Create(ctx context.Context, id, input string) error {
	reply, err := r.Redis.Do(ctx, "EVAL", createLua, "2", Key(id), Stream, id, input, timestamp())
	if err == nil && reply.Number != 1 {
		return errors.New("task already exists")
	}
	return err
}

type ClaimState string

const (
	Acquired  ClaimState = "acquired"
	Busy      ClaimState = "busy"
	Done      ClaimState = "done"
	Missing   ClaimState = "missing"
	Exhausted ClaimState = "exhausted"
	Invalid   ClaimState = "invalid"
)

const claimLua = redisClock + observeTerminal + `
local status=redis.call('HGET',KEYS[1],'status')
if not status and redis.call('EXISTS',KEYS[1]) == 0 then return 'missing' end
if status=='succeeded' or status=='failed' then return 'done' end
if status~='queued' and status~='running' then return 'invalid' end
if status=='running' and tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')>now then
  return 'busy'
end
local attempts=tonumber(redis.call('HGET',KEYS[1],'attempts') or '')
if not attempts or attempts < 0 or attempts ~= math.floor(attempts) then return 'invalid' end
if attempts>=tonumber(ARGV[3]) then
  redis.call('XACK',KEYS[2],ARGV[5],ARGV[6])
  redis.call('HSET',KEYS[1], 'status','failed','owner','','lease_until','0',
    'error','attempt budget exhausted before completion','updated_at',ARGV[4],'finished_ms',now)
  observe('failed')
  return 'exhausted'
end
redis.call('HINCRBY',KEYS[1],'attempts',1)
redis.call('HSETNX',KEYS[1],'started_ms',now)
redis.call('HSET',KEYS[1], 'status','running','owner',ARGV[1],
  'lease_until',now+tonumber(ARGV[2]),'updated_at',ARGV[4],'error','')
return 'acquired'
`

func (r *Repository) Claim(ctx context.Context, m Message, owner string, lease time.Duration, maxAttempts int) (ClaimState, error) {
	v, err := r.Redis.Do(ctx, "EVAL", claimLua, "3", Key(m.TaskID), Stream, metricKey, owner,
		strconv.FormatInt(lease.Milliseconds(), 10), strconv.Itoa(maxAttempts), timestamp(), Group, m.StreamID)
	return ClaimState(v.String()), err
}

const requireLease = redisClock + `
if redis.call('HGET',KEYS[1],'status')~='running' or
   redis.call('HGET',KEYS[1],'owner')~=ARGV[1] or
   tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=now then return 'stale' end
`

const succeedLua = requireLease + observeTerminal + `
redis.call('XACK',KEYS[2],ARGV[4],ARGV[5])
redis.call('HSET',KEYS[1], 'status','succeeded','result_file',ARGV[2], 'error','',
  'owner','','lease_until','0','updated_at',ARGV[3],'finished_ms',now)
observe('succeeded')
return 'succeeded'
`

func (r *Repository) Succeed(ctx context.Context, m Message, owner, file string) (bool, error) {
	v, err := r.Redis.Do(ctx, "EVAL", succeedLua, "3", Key(m.TaskID), Stream, metricKey, owner,
		file, timestamp(), Group, m.StreamID)
	return v.String() == "succeeded", err
}

const failLua = requireLease + observeTerminal + `
local attempts=tonumber(redis.call('HGET',KEYS[1],'attempts'))
if attempts>=tonumber(ARGV[2]) then
  redis.call('XACK',KEYS[2],ARGV[5],ARGV[6])
  redis.call('HSET',KEYS[1], 'status','failed','error',ARGV[3],'owner','','lease_until','0',
    'updated_at',ARGV[4],'finished_ms',now)
  observe('failed')
  return 'failed'
end
-- Validate the group before publishing. A Lua runtime error does not roll back earlier writes.
redis.call('XPENDING',KEYS[2],ARGV[5])
redis.call('XADD',KEYS[2],'*','task_id',ARGV[7])
redis.call('XACK',KEYS[2],ARGV[5],ARGV[6])
redis.call('HSET',KEYS[1], 'status','queued','error',ARGV[3],'owner','','lease_until','0','updated_at',ARGV[4])
return 'retry'
`

func (r *Repository) Fail(ctx context.Context, m Message, owner, reason string, maxAttempts int) (string, error) {
	reason = string([]rune(reason))
	for len(reason) > 350 {
		_, width := utf8.DecodeLastRuneInString(reason)
		reason = reason[:len(reason)-width]
	}
	v, err := r.Redis.Do(ctx, "EVAL", failLua, "3", Key(m.TaskID), Stream, metricKey, owner,
		strconv.Itoa(maxAttempts), reason, timestamp(), Group, m.StreamID, m.TaskID)
	return v.String(), err
}

func timestamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }
