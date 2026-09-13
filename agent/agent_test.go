package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daobathanh/celesnity/core"
)

// --- test helpers ---

type stubProvider struct {
	mu        sync.Mutex
	responses []*core.LLMResponse
	calls     int
	latency   time.Duration
}

func (p *stubProvider) Complete(ctx context.Context, _ string, msgs []core.Message, tools []core.ToolSpec) (*core.LLMResponse, error) {
	if p.latency > 0 {
		select {
		case <-time.After(p.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p.mu.Lock()
	idx := p.calls
	p.calls++
	p.mu.Unlock()
	if idx >= len(p.responses) {
		return &core.LLMResponse{Content: "done", StopReason: "end_turn"}, nil
	}
	return p.responses[idx], nil
}

type stubTools struct {
	tools   []core.ToolSpec
	results map[string]*core.ToolResult
	errors  map[string]error
	latency time.Duration
}

func (t *stubTools) ListTools(ctx context.Context) ([]core.ToolSpec, error) {
	return t.tools, nil
}

func (t *stubTools) CallTool(ctx context.Context, name string, args json.RawMessage) (*core.ToolResult, error) {
	if t.latency > 0 {
		select {
		case <-time.After(t.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err, ok := t.errors[name]; ok {
		return nil, err
	}
	if r, ok := t.results[name]; ok {
		return r, nil
	}
	return &core.ToolResult{Content: "{}", CallID: "call"}, nil
}

type memStore struct {
	mu       sync.Mutex
	sessions map[string]*core.Session
	runs     map[string]*core.Run
	events   map[string][]core.Event
	subs     map[string][]chan core.Event
}

func newMemStore() *memStore {
	return &memStore{
		sessions: make(map[string]*core.Session),
		runs:     make(map[string]*core.Run),
		events:   make(map[string][]core.Event),
		subs:     make(map[string][]chan core.Event),
	}
}

func (s *memStore) SaveSession(_ context.Context, sess *core.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sess.ID] = sess
	return nil
}

func (s *memStore) GetSession(_ context.Context, id string) (*core.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil, fmt.Errorf("session %q not found", id)
	}
	return sess, nil
}

func (s *memStore) SaveRun(_ context.Context, run *core.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *run
	s.runs[run.ID] = &cp
	return nil
}

func (s *memStore) GetRun(_ context.Context, id string) (*core.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return nil, fmt.Errorf("run not found")
	}
	cp := *run
	return &cp, nil
}

func (s *memStore) GetActiveRun(_ context.Context, sessionID string) (*core.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.runs {
		if r.SessionID == sessionID && r.Status == core.RunRunning {
			cp := *r
			return &cp, nil
		}
	}
	return nil, nil
}

func (s *memStore) Append(_ context.Context, event core.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	evts := s.events[event.SessionID]
	if len(evts) == 0 {
		event.Seq = 1
	} else {
		event.Seq = evts[len(evts)-1].Seq + 1
	}
	s.events[event.SessionID] = append(s.events[event.SessionID], event)
	for _, ch := range s.subs[event.SessionID] {
		select {
		case ch <- event:
		default:
		}
	}
	return nil
}

func (s *memStore) AppendAndUpdateRun(_ context.Context, event core.Event, run *core.Run) error {
	s.mu.Lock()
	evts := s.events[event.SessionID]
	if len(evts) == 0 {
		event.Seq = 1
	} else {
		event.Seq = evts[len(evts)-1].Seq + 1
	}
	s.events[event.SessionID] = append(s.events[event.SessionID], event)
	for _, ch := range s.subs[event.SessionID] {
		select {
		case ch <- event:
		default:
		}
	}
	cp := *run
	s.runs[run.ID] = &cp
	s.mu.Unlock()
	return nil
}

func (s *memStore) LoadFrom(_ context.Context, sessionID string, fromSeq int64) ([]core.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Event
	for _, e := range s.events[sessionID] {
		if e.Seq >= fromSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *memStore) NextSeq(_ context.Context, sessionID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evts := s.events[sessionID]
	if len(evts) == 0 {
		return 1, nil
	}
	return evts[len(evts)-1].Seq + 1, nil
}

func (s *memStore) Subscribe(sessionID string) (<-chan core.Event, func()) {
	ch := make(chan core.Event, 128)
	s.mu.Lock()
	s.subs[sessionID] = append(s.subs[sessionID], ch)
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, c := range s.subs[sessionID] {
			if c == ch {
				s.subs[sessionID] = append(s.subs[sessionID][:i], s.subs[sessionID][i+1:]...)
				break
			}
		}
		close(ch)
	}
}

func (s *memStore) MarkInterruptedRuns(_ context.Context) (int64, error) { return 0, nil }

func (s *memStore) getEvents(sessionID string) []core.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]core.Event(nil), s.events[sessionID]...)
}

func newTestAgent(provider core.LLMProvider, tools core.ToolRegistry, store *memStore) *Agent {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(provider, tools, store, logger)
}

func waitRunDone(t *testing.T, _ *Agent, store *memStore, runID string, timeout time.Duration) *core.Run {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatalf("run %s did not finish within %s", runID, timeout)
		case <-time.After(50 * time.Millisecond):
			run, err := store.GetRun(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if run.Status != core.RunRunning {
				return run
			}
		}
	}
}

func hasEventType(events []core.Event, typ core.EventType) bool {
	for _, e := range events {
		if e.Type == typ {
			return true
		}
	}
	return false
}

func findEventPayload[T any](events []core.Event, typ core.EventType) (T, bool) {
	var zero T
	for _, e := range events {
		if e.Type == typ {
			var p T
			json.Unmarshal(e.Payload, &p)
			return p, true
		}
	}
	return zero, false
}

// --- tests ---

func TestHappyPath(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "get_machine_status", Args: json.RawMessage(`{"machine_id":"CNC-001"}`)}},
				StopReason: "tool_use",
			},
			{Content: "Machine is running fine.", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "get_machine_status"}},
		results: map[string]*core.ToolResult{"get_machine_status": {CallID: "c1", Content: `{"status":"ok"}`}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, err := ag.SendMessage(context.Background(), sess.ID, "check machine")
	if err != nil {
		t.Fatal(err)
	}

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}
	if run.StepCount != 2 {
		t.Fatalf("expected 2 steps, got %d", run.StepCount)
	}

	events := store.getEvents(sess.ID)
	if !hasEventType(events, core.EventRunSucceeded) {
		t.Fatal("missing run_succeeded event")
	}
	if !hasEventType(events, core.EventToolCallSucceeded) {
		t.Fatal("missing tool_call_succeeded event")
	}
}

func TestNoProgressDetection(t *testing.T) {
	store := newMemStore()
	sameCall := &core.LLMResponse{
		ToolCalls:  []core.ToolCall{{ID: "c1", Name: "get_machine_status", Args: json.RawMessage(`{"machine_id":"CNC-001"}`)}},
		StopReason: "tool_use",
	}
	provider := &stubProvider{
		responses: []*core.LLMResponse{sameCall, sameCall, sameCall, sameCall, sameCall},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "get_machine_status"}},
		results: map[string]*core.ToolResult{"get_machine_status": {CallID: "c1", Content: `{"status":"ok"}`}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "check machine")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunFailed {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.Error == nil || run.Error.Code != "no_progress" {
		t.Fatalf("expected no_progress error, got %+v", run.Error)
	}
}

func TestPerToolTimeout(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{{
			ToolCalls:  []core.ToolCall{{ID: "c1", Name: "slow_tool", Args: json.RawMessage(`{}`)}},
			StopReason: "tool_use",
		}},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "slow_tool"}},
		latency: 10 * time.Second,
	}

	ag := newTestAgent(provider, tools, store)
	ag.toolTimeout = 500 * time.Millisecond // override for test
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "run slow tool")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)

	events := store.getEvents(sess.ID)
	if !hasEventType(events, core.EventToolCallFailed) {
		t.Fatal("expected tool_call_failed event for timeout")
	}
	_ = run
}

func TestCancelDuringLLMCall(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{{
			ToolCalls:  []core.ToolCall{{ID: "c1", Name: "tool1", Args: json.RawMessage(`{}`)}},
			StopReason: "tool_use",
		}},
		latency: 5 * time.Second,
	}
	tools := &stubTools{tools: []core.ToolSpec{{Name: "tool1"}}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "do something")

	time.Sleep(200 * time.Millisecond)
	ag.CancelRun(context.Background(), runID)

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunCancelled {
		t.Fatalf("expected cancelled, got %s", run.Status)
	}
}

func TestCancelDuringToolCall(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{{
			ToolCalls:  []core.ToolCall{{ID: "c1", Name: "slow_tool", Args: json.RawMessage(`{}`)}},
			StopReason: "tool_use",
		}},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "slow_tool"}},
		latency: 5 * time.Second,
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "run slow tool")

	time.Sleep(200 * time.Millisecond)
	ag.CancelRun(context.Background(), runID)

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunCancelled {
		t.Fatalf("expected cancelled, got %s", run.Status)
	}
	events := store.getEvents(sess.ID)
	if !hasEventType(events, core.EventToolCallCancelled) {
		t.Fatal("expected tool_call_cancelled event")
	}
}

func TestCancelIdempotent(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{{Content: "done", StopReason: "end_turn"}},
	}
	tools := &stubTools{}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "hello")

	waitRunDone(t, ag, store, runID, 5*time.Second)

	// cancel after done, should be idempotent
	if err := ag.CancelRun(context.Background(), runID); err != nil {
		t.Fatalf("cancel finished run should not error: %v", err)
	}
	if err := ag.CancelRun(context.Background(), runID); err != nil {
		t.Fatalf("double cancel should not error: %v", err)
	}
}

func TestBudgetMaxSteps(t *testing.T) {
	store := newMemStore()
	// always returns tool call with varying args, will hit max steps (not no-progress)
	provider := &stubProvider{}
	for i := 0; i < 30; i++ {
		provider.responses = append(provider.responses, &core.LLMResponse{
			ToolCalls:  []core.ToolCall{{ID: fmt.Sprintf("c%d", i), Name: "tool1", Args: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))}},
			StopReason: "tool_use",
		})
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "tool1"}},
		results: map[string]*core.ToolResult{"tool1": {CallID: "c", Content: "ok"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "loop forever")

	run := waitRunDone(t, ag, store, runID, 10*time.Second)
	if run.Status != core.RunFailed {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.Error == nil || run.Error.Code != "budget_exceeded" {
		t.Fatalf("expected budget_exceeded, got %+v", run.Error)
	}
	events := store.getEvents(sess.ID)
	if !hasEventType(events, core.EventBudgetExceeded) {
		t.Fatal("missing budget_exceeded event")
	}
}

func TestConcurrentRunRejected(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{{Content: "done", StopReason: "end_turn"}},
		latency:   2 * time.Second,
	}
	tools := &stubTools{}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})

	_, err := ag.SendMessage(context.Background(), sess.ID, "first")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	_, err = ag.SendMessage(context.Background(), sess.ID, "second")
	if err == nil {
		t.Fatal("expected error for concurrent run")
	}
}

func TestMultiTurnHistory(t *testing.T) {
	store := newMemStore()
	callNum := 0
	provider := &stubProvider{}
	// run 1: tool call then done
	provider.responses = append(provider.responses,
		&core.LLMResponse{
			ToolCalls:  []core.ToolCall{{ID: "c1", Name: "tool1", Args: json.RawMessage(`{}`)}},
			StopReason: "tool_use",
		},
		&core.LLMResponse{Content: "result 1", StopReason: "end_turn"},
	)
	// run 2: just done, but model should see history
	provider.responses = append(provider.responses,
		&core.LLMResponse{Content: "result 2 with context", StopReason: "end_turn"},
	)
	_ = callNum

	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "tool1"}},
		results: map[string]*core.ToolResult{"tool1": {CallID: "c1", Content: `{"data":"x"}`}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})

	// Run 1
	runID1, _ := ag.SendMessage(context.Background(), sess.ID, "first message")
	run1 := waitRunDone(t, ag, store, runID1, 5*time.Second)
	if run1.Status != core.RunSucceeded {
		t.Fatalf("run1: expected succeeded, got %s", run1.Status)
	}

	// Run 2: should include history from run 1
	runID2, _ := ag.SendMessage(context.Background(), sess.ID, "second message")
	run2 := waitRunDone(t, ag, store, runID2, 5*time.Second)
	if run2.Status != core.RunSucceeded {
		t.Fatalf("run2: expected succeeded, got %s", run2.Status)
	}

	// Verify run 2's model_request includes history
	events := store.getEvents(sess.ID)
	for _, e := range events {
		if e.Type == core.EventModelRequest && e.RunID == runID2 {
			var p core.ModelRequestPayload
			json.Unmarshal(e.Payload, &p)
			if len(p.Messages) < 4 {
				t.Fatalf("run2 should have history + new message, got %d messages", len(p.Messages))
			}
			if p.Messages[0].Content != "first message" {
				t.Fatalf("first message in history should be 'first message', got %q", p.Messages[0].Content)
			}
			return
		}
	}
	t.Fatal("no model_request event found for run 2")
}

func TestRebuildMessages(t *testing.T) {
	events := []core.Event{
		{Type: core.EventRunStarted, Payload: core.MustMarshalPayload(core.RunStartedPayload{UserMessage: "hello"})},
		{Type: core.EventModelResponse, Payload: core.MustMarshalPayload(core.ModelResponsePayload{
			Content:   "checking",
			ToolCalls: []core.ToolCall{{ID: "c1", Name: "tool1", Args: json.RawMessage(`{"x":1}`)}},
		})},
		{Type: core.EventToolCallSucceeded, Payload: core.MustMarshalPayload(core.ToolCallResultPayload{CallID: "c1", Name: "tool1", Content: "result1"})},
		{Type: core.EventModelResponse, Payload: core.MustMarshalPayload(core.ModelResponsePayload{Content: "done"})},
		{Type: core.EventRunSucceeded, Payload: core.MustMarshalPayload(core.RunTerminalPayload{})},
	}

	msgs := rebuildMessages(events)
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(msgs))
	}
	if msgs[0].Role != core.RoleUser || msgs[0].Content != "hello" {
		t.Fatalf("msg[0]: expected user 'hello', got %s %q", msgs[0].Role, msgs[0].Content)
	}
	if msgs[1].Role != core.RoleAssistant || len(msgs[1].ToolCalls) != 1 {
		t.Fatalf("msg[1]: expected assistant with 1 tool call, got %s tc=%d", msgs[1].Role, len(msgs[1].ToolCalls))
	}
	if msgs[2].Role != core.RoleToolResult || msgs[2].ToolCallID != "c1" {
		t.Fatalf("msg[2]: expected tool_result c1, got %s %q", msgs[2].Role, msgs[2].ToolCallID)
	}
	if msgs[3].Role != core.RoleAssistant || msgs[3].Content != "done" {
		t.Fatalf("msg[3]: expected assistant 'done', got %s %q", msgs[3].Role, msgs[3].Content)
	}
}

func TestRebuildInterruptedRun(t *testing.T) {
	// server crashed after model response with tool calls but before tool executed
	events := []core.Event{
		{Type: core.EventRunStarted, Payload: core.MustMarshalPayload(core.RunStartedPayload{UserMessage: "go"})},
		{Type: core.EventModelResponse, Payload: core.MustMarshalPayload(core.ModelResponsePayload{
			Content:   "",
			ToolCalls: []core.ToolCall{{ID: "c1", Name: "t1", Args: json.RawMessage(`{}`)}, {ID: "c2", Name: "t2", Args: json.RawMessage(`{}`)}},
		})},
		{Type: core.EventToolCallSucceeded, Payload: core.MustMarshalPayload(core.ToolCallResultPayload{CallID: "c1", Name: "t1", Content: "ok"})},
		// c2 never executed, server crashed
	}

	msgs := rebuildMessages(events)
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages (user, assistant, tool_result c1, synthetic c2), got %d", len(msgs))
	}
	// last message should be synthetic tool_result for c2
	last := msgs[3]
	if last.Role != core.RoleToolResult || last.ToolCallID != "c2" {
		t.Fatalf("expected synthetic tool_result for c2, got %s %q", last.Role, last.ToolCallID)
	}
	if last.Content != "Tool execution was interrupted before completion." {
		t.Fatalf("unexpected synthetic content: %q", last.Content)
	}
}

func TestToolError(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "failing_tool", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
			},
			{Content: "handled the error", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{
		tools:  []core.ToolSpec{{Name: "failing_tool"}},
		errors: map[string]error{"failing_tool": fmt.Errorf("connection refused")},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "use tool")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded (model handles error), got %s", run.Status)
	}

	events := store.getEvents(sess.ID)
	if !hasEventType(events, core.EventToolCallFailed) {
		t.Fatal("expected tool_call_failed event")
	}
}

func TestUnknownTool(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "nonexistent_tool", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
			},
			{Content: "sorry, let me try a valid tool", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{
		tools: []core.ToolSpec{{Name: "real_tool"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "use a tool")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded (model recovers), got %s", run.Status)
	}

	events := store.getEvents(sess.ID)
	if !hasEventType(events, core.EventToolCallFailed) {
		t.Fatal("expected tool_call_failed for unknown tool")
	}

	// verify the error message contains available tools
	for _, e := range events {
		if e.Type == core.EventToolCallFailed {
			var p core.ToolCallResultPayload
			json.Unmarshal(e.Payload, &p)
			if p.Content == "" {
				t.Fatal("tool_call_failed should have error content")
			}
			return
		}
	}
}

func TestInvalidToolCallJSON(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls: []core.ToolCall{
					{ID: "c1", Name: "tool1", Args: json.RawMessage(`{not valid json}`)},
				},
				StopReason: "tool_use",
			},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "tool1"}},
		results: map[string]*core.ToolResult{"tool1": {CallID: "c1", Content: "ok"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "test invalid json")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded (model recovers), got %s", run.Status)
	}
	events, _ := store.LoadFrom(context.Background(), sess.ID, 0)
	if !hasEventType(events, core.EventToolCallFailed) {
		t.Fatal("expected tool_call_failed event for invalid JSON")
	}
}

func TestCloseSession(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})

	if err := ag.CloseSession(context.Background(), sess.ID); err != nil {
		t.Fatalf("close session: %v", err)
	}

	// closing again is idempotent
	if err := ag.CloseSession(context.Background(), sess.ID); err != nil {
		t.Fatalf("close again: %v", err)
	}

	// sending to closed session fails
	_, err := ag.SendMessage(context.Background(), sess.ID, "should fail")
	if err == nil {
		t.Fatal("expected error sending to closed session")
	}
}

func TestEmptyMessageRejected(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})

	_, err := ag.SendMessage(context.Background(), sess.ID, "")
	if err == nil {
		t.Fatal("expected error for empty message")
	}
}

func TestWallClockBudget(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		latency: 2 * time.Second,
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "tool1", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
			},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "tool1"}},
		results: map[string]*core.ToolResult{"tool1": {CallID: "c1", Content: "ok"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "slow run")

	// default wall clock is 5 min, too long for test
	// cancel manually after short delay to simulate budget
	time.Sleep(500 * time.Millisecond)
	ag.CancelRun(context.Background(), runID)

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunCancelled {
		t.Fatalf("expected cancelled, got %s", run.Status)
	}
}

func TestCancelFinishedRunIsNoop(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "hello")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}

	err := ag.CancelRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("cancel finished run should not error: %v", err)
	}

	run, _ = store.GetRun(context.Background(), runID)
	if run.Status != core.RunSucceeded {
		t.Fatalf("status should remain succeeded, got %s", run.Status)
	}
}

func TestSendToNonExistentSession(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)

	_, err := ag.SendMessage(context.Background(), "non-existent-id", "hello")
	if err == nil {
		t.Fatal("expected error for non-existent session")
	}
}

func TestGetNonExistentRun(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)

	_, err := ag.GetRun(context.Background(), "non-existent-run")
	if err == nil {
		t.Fatal("expected error for non-existent run")
	}
}

func TestMultipleToolCallsPartialFailure(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls: []core.ToolCall{
					{ID: "c1", Name: "good_tool", Args: json.RawMessage(`{}`)},
					{ID: "c2", Name: "bad_tool", Args: json.RawMessage(`{}`)},
				},
				StopReason: "tool_use",
			},
		},
	}
	tools := &stubTools{
		tools: []core.ToolSpec{
			{Name: "good_tool"},
			{Name: "bad_tool"},
		},
		results: map[string]*core.ToolResult{
			"good_tool": {CallID: "c1", Content: "success"},
		},
		errors: map[string]error{
			"bad_tool": fmt.Errorf("connection refused"),
		},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "use both tools")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded (model recovers from partial failure), got %s", run.Status)
	}

	events, _ := store.LoadFrom(context.Background(), sess.ID, 0)
	if !hasEventType(events, core.EventToolCallSucceeded) {
		t.Fatal("expected tool_call_succeeded for good_tool")
	}
	if !hasEventType(events, core.EventToolCallFailed) {
		t.Fatal("expected tool_call_failed for bad_tool")
	}
}

func TestToolReturnsIsError(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls: []core.ToolCall{
					{ID: "c1", Name: "flaky", Args: json.RawMessage(`{}`)},
				},
				StopReason: "tool_use",
			},
		},
	}
	tools := &stubTools{
		tools: []core.ToolSpec{{Name: "flaky"}},
		results: map[string]*core.ToolResult{
			"flaky": {CallID: "c1", Content: "permission denied", IsError: true},
		},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "use flaky tool")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded (model sees tool error and decides to stop), got %s", run.Status)
	}

	events, _ := store.LoadFrom(context.Background(), sess.ID, 0)
	if !hasEventType(events, core.EventToolCallSucceeded) {
		t.Fatal("tool returning isError=true should still emit tool_call_succeeded (it's a tool-level error, not a system error)")
	}
}

func TestLargeToolResultTruncated(t *testing.T) {
	store := newMemStore()
	largeContent := make([]byte, 50000)
	for i := range largeContent {
		largeContent[i] = 'x'
	}
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls: []core.ToolCall{
					{ID: "c1", Name: "big", Args: json.RawMessage(`{}`)},
				},
				StopReason: "tool_use",
			},
		},
	}
	tools := &stubTools{
		tools: []core.ToolSpec{{Name: "big"}},
		results: map[string]*core.ToolResult{
			"big": {CallID: "c1", Content: string(largeContent)},
		},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "get big data")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}

	events, _ := store.LoadFrom(context.Background(), sess.ID, 0)
	p, ok := findEventPayload[core.ToolCallResultPayload](events, core.EventToolCallSucceeded)
	if !ok {
		t.Fatal("expected tool_call_succeeded event")
	}
	if len(p.Content) != 50000 {
		t.Fatalf("event log should store full content (%d), got %d", 50000, len(p.Content))
	}
}

func TestStreamEventsReplay(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "hello")

	waitRunDone(t, ag, store, runID, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := ag.StreamEvents(ctx, sess.ID, 0)
	if err != nil {
		t.Fatalf("stream events: %v", err)
	}

	var events []core.Event
	for e := range ch {
		events = append(events, e)
	}

	if len(events) == 0 {
		t.Fatal("expected replayed events")
	}
	if events[0].Type != core.EventRunStarted {
		t.Fatalf("first event should be run_started, got %s", events[0].Type)
	}

	found := false
	for _, e := range events {
		if e.Type == core.EventRunSucceeded {
			found = true
		}
	}
	if !found {
		t.Fatal("expected run_succeeded in replayed events")
	}
}

func TestStreamEventsFromMidpoint(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "hello")

	waitRunDone(t, ag, store, runID, 5*time.Second)

	allEvents, _ := store.LoadFrom(context.Background(), sess.ID, 0)
	if len(allEvents) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(allEvents))
	}

	midSeq := allEvents[2].Seq
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := ag.StreamEvents(ctx, sess.ID, midSeq)
	if err != nil {
		t.Fatalf("stream from midpoint: %v", err)
	}

	var replayed []core.Event
	for e := range ch {
		replayed = append(replayed, e)
	}

	if len(replayed) >= len(allEvents) {
		t.Fatalf("from midpoint should return fewer events: got %d, all %d", len(replayed), len(allEvents))
	}
	if replayed[0].Seq != midSeq {
		t.Fatalf("first replayed event should have seq %d, got %d", midSeq, replayed[0].Seq)
	}
}

func TestStreamEventsNonExistentSession(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)

	_, err := ag.StreamEvents(context.Background(), "non-existent", 0)
	if err == nil {
		t.Fatal("expected error for non-existent session")
	}
}

func TestCloseSessionWithActiveRun(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		latency: 3 * time.Second,
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "long running")

	time.Sleep(100 * time.Millisecond)

	err := ag.CloseSession(context.Background(), sess.ID)
	if err == nil {
		t.Fatal("expected error closing session with active run")
	}

	ag.CancelRun(context.Background(), runID)
	waitRunDone(t, ag, store, runID, 5*time.Second)
}

func TestTokenUsageAccumulated(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "t1", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
				Usage:      core.TokenUsage{InputTokens: 100, OutputTokens: 50},
			},
			{
				Content:    "done",
				StopReason: "end_turn",
				Usage:      core.TokenUsage{InputTokens: 200, OutputTokens: 80},
			},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "t1"}},
		results: map[string]*core.ToolResult{"t1": {CallID: "c1", Content: "ok"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "count tokens")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	expectedTokens := 100 + 50 + 200 + 80
	if run.TokensUsed != expectedTokens {
		t.Fatalf("expected %d tokens used, got %d", expectedTokens, run.TokensUsed)
	}
	if run.StepCount != 2 {
		t.Fatalf("expected 2 steps, got %d", run.StepCount)
	}
}

func TestShutdownCancelsActiveRuns(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		latency: 5 * time.Second,
		responses: []*core.LLMResponse{
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{tools: []core.ToolSpec{}}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "long task")

	time.Sleep(100 * time.Millisecond)
	ag.Shutdown()

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunCancelled {
		t.Fatalf("expected cancelled after shutdown, got %s", run.Status)
	}
}

func TestEventSequenceOrdering(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "t1", Args: json.RawMessage(`{}`)}},
				StopReason: "tool_use",
			},
			{Content: "done", StopReason: "end_turn"},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "t1"}},
		results: map[string]*core.ToolResult{"t1": {CallID: "c1", Content: "ok"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "check ordering")

	waitRunDone(t, ag, store, runID, 5*time.Second)

	events, _ := store.LoadFrom(context.Background(), sess.ID, 0)

	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("event seq not monotonically increasing: %d <= %d", events[i].Seq, events[i-1].Seq)
		}
	}

	expectedOrder := []core.EventType{
		core.EventRunStarted,
		core.EventModelRequest,
		core.EventModelResponse,
		core.EventToolCallStarted,
		core.EventToolCallSucceeded,
		core.EventModelRequest,
		core.EventModelResponse,
		core.EventRunSucceeded,
	}

	if len(events) != len(expectedOrder) {
		t.Fatalf("expected %d events, got %d", len(expectedOrder), len(events))
	}
	for i, expected := range expectedOrder {
		if events[i].Type != expected {
			t.Fatalf("event[%d]: expected %s, got %s", i, expected, events[i].Type)
		}
	}
}

func TestBudgetMaxTokens(t *testing.T) {
	store := newMemStore()
	provider := &stubProvider{
		responses: []*core.LLMResponse{
			{
				ToolCalls:  []core.ToolCall{{ID: "c1", Name: "t1", Args: json.RawMessage(`{"i":1}`)}},
				StopReason: "tool_use",
				Usage:      core.TokenUsage{InputTokens: 60000, OutputTokens: 20000},
			},
			{
				ToolCalls:  []core.ToolCall{{ID: "c2", Name: "t1", Args: json.RawMessage(`{"i":2}`)}},
				StopReason: "tool_use",
				Usage:      core.TokenUsage{InputTokens: 60000, OutputTokens: 20000},
			},
		},
	}
	tools := &stubTools{
		tools:   []core.ToolSpec{{Name: "t1"}},
		results: map[string]*core.ToolResult{"t1": {CallID: "c1", Content: "ok"}},
	}

	ag := newTestAgent(provider, tools, store)
	sess, _ := ag.CreateSession(context.Background(), core.SessionOpts{Model: "test"})
	runID, _ := ag.SendMessage(context.Background(), sess.ID, "burn tokens")

	run := waitRunDone(t, ag, store, runID, 5*time.Second)
	if run.Status != core.RunFailed {
		t.Fatalf("expected failed from token budget, got %s", run.Status)
	}
	if run.Error == nil || run.Error.Code != "budget_exceeded" {
		t.Fatalf("expected budget_exceeded error, got %+v", run.Error)
	}

	events, _ := store.LoadFrom(context.Background(), sess.ID, 0)
	if !hasEventType(events, core.EventBudgetExceeded) {
		t.Fatal("expected budget_exceeded event")
	}
}
