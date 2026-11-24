// Package redisresp implements the small subset of the Redis RESP2 protocol
// needed for this educational project. For production, prefer go-redis.
package redisresp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Value struct {
	Kind   byte
	Text   string
	Number int64
	Items  []Value
	Null   bool
}

func (v Value) String() string {
	if v.Kind == ':' {
		return strconv.FormatInt(v.Number, 10)
	}
	return v.Text
}

func (v Value) Item(i int) (Value, error) {
	if i < 0 || i >= len(v.Items) {
		return Value{}, fmt.Errorf("redis reply index %d out of range", i)
	}
	return v.Items[i], nil
}

type Client struct {
	Addr     string
	Password string
}

func New(addr string) *Client { return &Client{Addr: addr} }

func FromEnv() *Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return &Client{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")}
}

func (c *Client) Do(ctx context.Context, args ...string) (Value, error) {
	if len(args) == 0 {
		return Value{}, errors.New("empty redis command")
	}
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return Value{}, fmt.Errorf("redis dial: %w", err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return Value{}, err
	}
	rd := bufio.NewReader(conn)
	if c.Password != "" {
		if err := send(conn, []string{"AUTH", c.Password}); err != nil {
			return Value{}, err
		}
		if _, err := parse(rd); err != nil {
			return Value{}, err
		}
	}
	if err := send(conn, args); err != nil {
		return Value{}, err
	}
	reply, err := parse(rd)
	if ctx.Err() != nil {
		return Value{}, ctx.Err()
	}
	return reply, err
}

func send(w io.Writer, args []string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func parse(rd *bufio.Reader) (Value, error) {
	marker, err := rd.ReadByte()
	if err != nil {
		return Value{}, err
	}
	line, err := rd.ReadString('\n')
	if err != nil {
		return Value{}, err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return Value{}, errors.New("invalid RESP line ending")
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch marker {
	case '+':
		return Value{Kind: '+', Text: line}, nil
	case '-':
		return Value{}, fmt.Errorf("redis: %s", line)
	case ':':
		n, err := strconv.ParseInt(line, 10, 64)
		return Value{Kind: ':', Number: n}, err
	case '$':
		n, err := strconv.Atoi(line)
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Kind: '$', Null: true}, nil
		}
		if n < 0 || n > 32<<20 {
			return Value{}, errors.New("invalid or oversized RESP bulk string")
		}
		data := make([]byte, n+2)
		if _, err := io.ReadFull(rd, data); err != nil {
			return Value{}, err
		}
		if data[n] != '\r' || data[n+1] != '\n' {
			return Value{}, errors.New("invalid RESP bulk ending")
		}
		return Value{Kind: '$', Text: string(data[:n])}, nil
	case '*':
		n, err := strconv.Atoi(line)
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Kind: '*', Null: true}, nil
		}
		if n < 0 || n > 65536 {
			return Value{}, errors.New("invalid or oversized RESP array")
		}
		result := Value{Kind: '*', Items: make([]Value, 0, n)}
		for i := 0; i < n; i++ {
			item, err := parse(rd)
			if err != nil {
				return Value{}, err
			}
			result.Items = append(result.Items, item)
		}
		return result, nil
	default:
		return Value{}, fmt.Errorf("unknown RESP marker %q", marker)
	}
}
