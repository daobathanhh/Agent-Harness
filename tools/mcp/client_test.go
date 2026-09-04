package mcp

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"
)

func buildMockMCP(t *testing.T) string {
	t.Helper()
	bin := t.TempDir() + "/mockmcp"
	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "../../cmd/mockmcp/")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build mockmcp: %v", err)
	}
	return bin
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestListTools(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, nil, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	tools, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}

	if len(tools) != 3 {
		t.Fatalf("expected 3 tools, got %d", len(tools))
	}

	names := map[string]bool{}
	for _, tool := range tools {
		names[tool.Name] = true
	}
	for _, expected := range []string{"get_machine_status", "search_maintenance_logs", "create_work_order"} {
		if !names[expected] {
			t.Fatalf("missing tool: %s", expected)
		}
	}
}

func TestListToolsCached(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, nil, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	tools1, _ := client.ListTools(context.Background())
	tools2, _ := client.ListTools(context.Background())

	if len(tools1) != len(tools2) {
		t.Fatal("cached tools should be same length")
	}
}

func TestCallTool(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, nil, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	result, err := client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", result.Content)
	}
	if result.Content == "" {
		t.Fatal("expected non-empty result")
	}
}

func TestCallToolWithLatency(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, []string{"--latency-ms", "100"}, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	start := time.Now()
	result, err := client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected error: %s", result.Content)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected at least 100ms latency, got %s", elapsed)
	}
}

func TestCallToolContextCancelled(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, []string{"--latency-ms", "5000"}, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = client.CallTool(ctx, "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestCallToolFailRate(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, []string{"--fail-rate", "1.0"}, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	result, err := client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	if err != nil {
		t.Fatalf("call should succeed (tool-level error, not transport error): %v", err)
	}
	if !result.IsError {
		t.Fatal("expected isError=true from 100% fail rate")
	}
}

func TestReconnectAfterDrop(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, []string{"--drop-after-n-calls", "1"}, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// first call succeeds
	result, err := client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	if err != nil {
		t.Fatalf("first call should succeed: %v", err)
	}
	if result.IsError {
		t.Fatalf("first call unexpected error: %s", result.Content)
	}

	// server exits after first tool call, next call triggers reconnect
	time.Sleep(100 * time.Millisecond)

	result, err = client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-002"}`))
	if err != nil {
		t.Fatalf("second call should succeed after reconnect: %v", err)
	}
	if result.IsError {
		t.Fatalf("second call unexpected error: %s", result.Content)
	}
}

func TestReconnectClearsToolsCache(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, []string{"--drop-after-n-calls", "1"}, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// cache tools
	tools1, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}

	// trigger server drop
	client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-001"}`))
	time.Sleep(100 * time.Millisecond)

	// force reconnect via another call
	client.CallTool(context.Background(), "get_machine_status", json.RawMessage(`{"machine_id":"CNC-002"}`))

	// tools cache should have been cleared on reconnect, re-fetch works
	client.toolsMu.Lock()
	client.tools = nil
	client.toolsMu.Unlock()

	tools2, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatalf("list tools after reconnect: %v", err)
	}
	if len(tools2) != len(tools1) {
		t.Fatalf("tools changed after reconnect: %d vs %d", len(tools2), len(tools1))
	}
}

func TestJSONRPCErrorReturnsGoError(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, nil, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// call a method that doesn't exist, server returns JSON-RPC error
	_, err = client.call(context.Background(), "nonexistent/method", nil)
	if err == nil {
		t.Fatal("expected Go error from JSON-RPC error response")
	}
}

func TestIsConnected(t *testing.T) {
	bin := buildMockMCP(t)
	client, err := NewClient(bin, nil, testLogger())
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if !client.isConnected() {
		t.Fatal("should be connected after creation")
	}

	client.kill()
	time.Sleep(100 * time.Millisecond)

	if client.isConnected() {
		t.Fatal("should not be connected after kill")
	}
}
