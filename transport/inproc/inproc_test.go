package inproc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/daobathanh/celesnity/agent"
	"github.com/daobathanh/celesnity/core"
	"github.com/daobathanh/celesnity/store/sqlite"
	"github.com/daobathanh/celesnity/transport/inproc"
)

type stubProvider struct {
	responses []*core.LLMResponse
	calls     int
	latency   time.Duration
}

func (p *stubProvider) Complete(ctx context.Context, _ string, _ []core.Message, _ []core.ToolSpec) (*core.LLMResponse, error) {
	if p.latency > 0 {
		select {
		case <-time.After(p.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	idx := p.calls
	p.calls++
	if idx >= len(p.responses) {
		return &core.LLMResponse{Content: "done", StopReason: "end_turn"}, nil
	}
	return p.responses[idx], nil
}

type stubTools struct {
	tools   []core.ToolSpec
	results map[string]*core.ToolResult
	errors  map[string]error
}

func (t *stubTools) ListTools(_ context.Context) ([]core.ToolSpec, error) {
	return t.tools, nil
}

func (t *stubTools) CallTool(_ context.Context, name string, _ json.RawMessage) (*core.ToolResult, error) {
	if err, ok := t.errors[name]; ok {
		return nil, err
	}
	if r, ok := t.results[name]; ok {
		return r, nil
	}
	return &core.ToolResult{Content: "{}", CallID: "call"}, nil
}

func setupInproc(t *testing.T, provider core.LLMProvider, tools core.ToolRegistry) *inproc.Client {
	t.Helper()
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	ag := agent.New(provider, tools, store, logger)
	return inproc.New(ag)
}

func waitDone(t *testing.T, client *inproc.Client, runID string, timeout time.Duration) *core.Run {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatalf("run %s did not finish within %s", runID, timeout)
		case <-time.After(50 * time.Millisecond):
			run, err := client.GetRun(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if run.Status != core.RunRunning {
				return run
			}
		}
	}
}

func TestInprocHappyPath(t *testing.T) {
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "check", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
			},
			{Content: "Machine is running fine.", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "check"}},
		results: map[string]*core.ToolResult{"check": {CallID: "c1", Content: `{"status":"ok"}`}},
	}

	client := setupInproc(t, provider, tools)

	sess, err := client.CreateSession(context.Background(), core.SessionOpts{
		Model:        "test",
		SystemPrompt: "You are a factory assistant.",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if sess.Status != core.SessionActive {
		t.Fatalf("expected active, got %s", sess.Status)
	}

	runID, err := client.SendMessage(context.Background(), sess.ID, "Check machine CNC-001")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	run := waitDone(t, client, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}
	if run.StepCount != 2 {
		t.Fatalf("expected 2 steps, got %d", run.StepCount)
	}
}

func TestInprocStreamEvents(t *testing.T) {
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	client := setupInproc(t, provider, tools)

	sess, _ := client.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := client.SendMessage(context.Background(), sess.ID, "hello")
	waitDone(t, client, runID, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := client.StreamEvents(ctx, sess.ID, 0)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	var events []core.Event
	for e := range ch {
		events = append(events, e)
	}

	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	if events[0].Type != core.EventRunStarted {
		t.Fatalf("first event should be run_started, got %s", events[0].Type)
	}
}

func TestInprocCancel(t *testing.T) {
	provider := &stubProvider{
		latency: 3 * time.Second,
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	client := setupInproc(t, provider, tools)

	sess, _ := client.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := client.SendMessage(context.Background(), sess.ID, "slow task")

	time.Sleep(200 * time.Millisecond)

	if err := client.CancelRun(context.Background(), runID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	run := waitDone(t, client, runID, 5*time.Second)
	if run.Status != core.RunCancelled {
		t.Fatalf("expected cancelled, got %s", run.Status)
	}
}

func TestInprocToolFailure(t *testing.T) {
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "broken", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
			},
		},
	}
	tools := &stubTools{
		tools:  []core.ToolSpec{{Name: "broken"}},
		errors: map[string]error{"broken": fmt.Errorf("connection refused")},
	}

	client := setupInproc(t, provider, tools)

	sess, _ := client.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := client.SendMessage(context.Background(), sess.ID, "use broken tool")

	run := waitDone(t, client, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded (model recovers), got %s", run.Status)
	}
}

func TestInprocCloseSession(t *testing.T) {
	provider := &stubProvider{}
	tools := &stubTools{tools: []core.ToolSpec{}}

	client := setupInproc(t, provider, tools)

	sess, _ := client.CreateSession(context.Background(), core.SessionOpts{Model: "test"})

	if err := client.CloseSession(context.Background(), sess.ID); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := client.SendMessage(context.Background(), sess.ID, "should fail")
	if err == nil {
		t.Fatal("expected error sending to closed session")
	}
}

func TestInprocConcurrentRunRejected(t *testing.T) {
	provider := &stubProvider{
		latency: 2 * time.Second,
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	client := setupInproc(t, provider, tools)

	sess, _ := client.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := client.SendMessage(context.Background(), sess.ID, "first")

	time.Sleep(100 * time.Millisecond)

	_, err := client.SendMessage(context.Background(), sess.ID, "second")
	if err == nil {
		t.Fatal("expected error for concurrent run")
	}

	client.CancelRun(context.Background(), runID)
	waitDone(t, client, runID, 5*time.Second)
}
