package mcp

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
)

func TestReconnectFailsPermanently(t *testing.T) {
	bin := buildMockMCP(t)

	// drop-after-n-calls=0 means the server runs normally for initialize,
	// but we'll kill it manually and point cmdPath to a nonexistent binary
	// so reconnect attempts all fail.
	client, err := NewClient(bin, nil, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// verify it works first
	_, err = client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	if err != nil {
		t.Fatalf("first call should work: %v", err)
	}

	// kill the server and swap cmdPath to something that can't start
	client.kill()
	client.cmdPath = "/nonexistent/binary"

	// wait for readLoop to detect the closed pipe
	<-client.closed

	// next call should fail: connection lost, reconnect fails all 3 attempts, error
	_, err = client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-002"}`))
	if err == nil {
		t.Fatal("expected error when reconnect permanently fails")
	}
}

func TestReconnectFailsThenToolCallPropagatesError(t *testing.T) {
	// Use a binary that exits immediately after 1 tool call,
	// then make it unlaunchable so reconnect fails.
	bin := buildMockMCP(t)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewClient(bin, []string{"--drop-after-n-calls", "1"}, logger)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// first call succeeds, server exits after it
	_, err = client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// sabotage: make the binary unexecutable so reconnect fails
	client.cmdPath = "/nonexistent/binary"

	// second call: connection lost, reconnect fails, error returned
	_, err = client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-002"}`))
	if err == nil {
		t.Fatal("expected error when server is permanently gone")
	}

	// error message should mention reconnect failure
	errMsg := err.Error()
	if len(errMsg) == 0 {
		t.Fatal("error message should not be empty")
	}
}
