package redisresp

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestParseRESP(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*3\r\n$3\r\nfoo\r\n:42\r\n$-1\r\n"))
	v, err := parse(r)
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != '*' || len(v.Items) != 3 || v.Items[0].Text != "foo" || v.Items[1].Number != 42 || !v.Items[2].Null {
		t.Fatalf("unexpected parsed reply: %+v", v)
	}
}
func TestErrorReply(t *testing.T) {
	_, err := parse(bufio.NewReader(strings.NewReader("-ERR bad command\r\n")))
	if err == nil || !strings.Contains(err.Error(), "bad command") {
		t.Fatalf("expected redis error, got %v", err)
	}
}
func TestClientRoundTrip(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		v, err := parse(r)
		if err != nil || len(v.Items) != 1 || v.Items[0].Text != "PING" {
			return
		}
		_, _ = io.WriteString(conn, "+PONG\r\n")
	}()
	client := New(listener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := client.Do(ctx, "PING")
	if err != nil || v.Text != "PONG" {
		t.Fatalf("got %v %v", v, err)
	}
	<-done
}
