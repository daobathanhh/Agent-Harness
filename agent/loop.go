package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/daobathanh/celesnity/core"
	"github.com/google/uuid"
)

type Agent struct {
	provider core.LLMProvider
	tools    core.ToolRegistry
	store    core.EventStore
	logger   *slog.Logger

	mu          sync.Mutex
	cancels     map[string]context.CancelFunc
	toolTimeout time.Duration
}

func New(provider core.LLMProvider, tools core.ToolRegistry, store core.EventStore, logger *slog.Logger) *Agent {
	return &Agent{
		provider:    provider,
		tools:       tools,
		store:       store,
		logger:      logger,
		cancels:     make(map[string]context.CancelFunc),
		toolTimeout: 30 * time.Second,
	}
}

func (a *Agent) CreateSession(ctx context.Context, opts core.SessionOpts) (*core.Session, error) {
	sess := &core.Session{
		ID:           uuid.NewString(),
		Model:        opts.Model,
		SystemPrompt: opts.SystemPrompt,
		Status:       core.SessionActive,
		CreatedAt:    time.Now(),
	}
	if err := a.store.SaveSession(ctx, sess); err != nil {
		return nil, fmt.Errorf("agent: save session: %w", err)
	}
	return sess, nil
}

func (a *Agent) CloseSession(ctx context.Context, sessionID string) error {
	sess, err := a.store.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	if sess.Status == core.SessionClosed {
		return nil
	}

	active, err := a.store.GetActiveRun(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("agent: check active run: %w", err)
	}
	if active != nil {
		return fmt.Errorf("agent: session %q has active run %q, cancel it first", sessionID, active.ID)
	}

	sess.Status = core.SessionClosed
	if err := a.store.SaveSession(ctx, sess); err != nil {
		return fmt.Errorf("agent: save session: %w", err)
	}
	return nil
}

func (a *Agent) SendMessage(ctx context.Context, sessionID, msg string) (string, error) {
	if msg == "" {
		return "", fmt.Errorf("agent: message must not be empty")
	}

	sess, err := a.store.GetSession(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("agent: %w", err)
	}
	if sess.Status != core.SessionActive {
		return "", fmt.Errorf("agent: session %q is %s", sessionID, sess.Status)
	}

	active, err := a.store.GetActiveRun(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("agent: check active run: %w", err)
	}
	if active != nil {
		return "", fmt.Errorf("agent: session %q already has a running run %q", sessionID, active.ID)
	}

	history, err := a.loadSessionHistory(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("agent: load history: %w", err)
	}

	run := &core.Run{
		ID:        uuid.NewString(),
		SessionID: sessionID,
		Status:    core.RunRunning,
		StartedAt: time.Now(),
	}
	if err := a.store.SaveRun(ctx, run); err != nil {
		if errors.Is(err, core.ErrActiveRunExists) {
			return "", fmt.Errorf("agent: session %q already has a running run", sessionID)
		}
		return "", fmt.Errorf("agent: save run: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	a.mu.Lock()
	a.cancels[run.ID] = cancel
	a.mu.Unlock()

	go a.runLoop(runCtx, sess, run, msg, history)

	return run.ID, nil
}

func (a *Agent) GetRun(ctx context.Context, runID string) (*core.Run, error) {
	return a.store.GetRun(ctx, runID)
}

func (a *Agent) StreamEvents(ctx context.Context, sessionID string, fromSeq int64) (<-chan core.Event, error) {
	_, err := a.store.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	out := make(chan core.Event, 128)

	liveCh, unsub := a.store.Subscribe(sessionID)

	go func() {
		defer close(out)
		defer unsub()

		history, err := a.store.LoadFrom(ctx, sessionID, fromSeq)
		if err != nil {
			a.logger.Error("stream: load history", "err", err)
			return
		}

		nextSeq := fromSeq
		for _, e := range history {
			select {
			case out <- e:
				nextSeq = e.Seq + 1
			case <-ctx.Done():
				return
			}
		}

		for {
			select {
			case e, ok := <-liveCh:
				if !ok {
					return
				}
				if e.Seq < nextSeq {
					continue
				}
				select {
				case out <- e:
					nextSeq = e.Seq + 1
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (a *Agent) CancelRun(ctx context.Context, runID string) error {
	run, err := a.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("agent: %w", err)
	}

	if run.Status != core.RunRunning {
		return nil
	}

	a.mu.Lock()
	cancel, ok := a.cancels[runID]
	a.mu.Unlock()

	if !ok {
		run, err = a.store.GetRun(ctx, runID)
		if err != nil {
			return fmt.Errorf("agent: %w", err)
		}
		if run.Status != core.RunRunning {
			return nil
		}
		return fmt.Errorf("agent: run %q is marked running but not owned by this process", runID)
	}
	cancel()
	return nil
}

func (a *Agent) Shutdown() {
	a.mu.Lock()
	cancels := make(map[string]context.CancelFunc, len(a.cancels))
	for k, v := range a.cancels {
		cancels[k] = v
	}
	a.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (a *Agent) loadSessionHistory(ctx context.Context, sessionID string) ([]core.Message, error) {
	events, err := a.store.LoadFrom(ctx, sessionID, 0)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	return rebuildMessages(events), nil
}

func (a *Agent) runLoop(ctx context.Context, sess *core.Session, run *core.Run, userMsg string, history []core.Message) {
	defer func() {
		a.mu.Lock()
		delete(a.cancels, run.ID)
		a.mu.Unlock()
	}()

	budget := core.DefaultBudget()

	budgetCtx, budgetCancel := context.WithTimeout(ctx, budget.MaxWallClock)
	defer budgetCancel()

	a.emitEvent(run, core.EventRunStarted, core.RunStartedPayload{UserMessage: userMsg})

	toolSpecs, err := a.tools.ListTools(budgetCtx)
	if err != nil {
		a.finishRun(run, core.RunFailed, &core.RunError{Code: "tool_list_error", Message: err.Error()})
		return
	}

	msgs := append(history, core.Message{Role: core.RoleUser, Content: userMsg})

	validTools := make(map[string]bool, len(toolSpecs))
	for _, ts := range toolSpecs {
		validTools[ts.Name] = true
	}

	const noProgressThreshold = 3
	type callSig struct{ name, args string }
	callHistory := make(map[callSig]int)

	for step := 0; step < budget.MaxSteps; step++ {
		if err := budgetCtx.Err(); err != nil {
			if ctx.Err() != nil {
				a.finishRun(run, core.RunCancelled, nil)
			} else {
				a.finishRun(run, core.RunFailed, &core.RunError{Code: "budget_exceeded", Message: "wall clock limit exceeded"})
			}
			return
		}

		if run.TokensUsed >= budget.MaxTokens {
			a.emitEvent(run, core.EventBudgetExceeded, core.BudgetExceededPayload{
				Reason: "token limit exceeded",
				Limit:  fmt.Sprintf("%d tokens", budget.MaxTokens),
			})
			a.finishRun(run, core.RunFailed, &core.RunError{Code: "budget_exceeded", Message: "token limit exceeded"})
			return
		}

		msgs = truncateMessages(msgs)

		a.emitEvent(run, core.EventModelRequest, core.ModelRequestPayload{
			Messages:  msgs,
			ToolSpecs: toolSpecs,
		})

		resp, err := a.provider.Complete(budgetCtx, sess.SystemPrompt, msgs, toolSpecs)
		if err != nil {
			if budgetCtx.Err() != nil && ctx.Err() != nil {
				a.finishRun(run, core.RunCancelled, nil)
			} else {
				a.finishRun(run, core.RunFailed, &core.RunError{Code: "provider_error", Message: err.Error()})
			}
			return
		}

		run.StepCount++
		run.TokensUsed += resp.Usage.InputTokens + resp.Usage.OutputTokens

		for i := range resp.ToolCalls {
			if !json.Valid(resp.ToolCalls[i].Args) {
				resp.ToolCalls[i].Args, _ = json.Marshal(string(resp.ToolCalls[i].Args))
			}
		}
		a.emitEvent(run, core.EventModelResponse, core.ModelResponsePayload{
			Content:    resp.Content,
			ToolCalls:  resp.ToolCalls,
			Usage:      resp.Usage,
			StopReason: resp.StopReason,
		})

		if len(resp.ToolCalls) == 0 {
			msgs = append(msgs, core.Message{Role: core.RoleAssistant, Content: resp.Content})
			a.finishRun(run, core.RunSucceeded, nil)
			return
		}

		msgs = append(msgs, core.Message{Role: core.RoleAssistant, Content: resp.Content, ToolCalls: resp.ToolCalls})

		// no-progress detection: same tool+args repeated too many times
		for _, tc := range resp.ToolCalls {
			sig := callSig{name: tc.Name, args: string(tc.Args)}
			callHistory[sig]++
			if callHistory[sig] >= noProgressThreshold {
				a.finishRun(run, core.RunFailed, &core.RunError{
					Code:    "no_progress",
					Message: fmt.Sprintf("tool %q called with identical args %d times", tc.Name, callHistory[sig]),
				})
				return
			}
		}

		for _, tc := range resp.ToolCalls {
			if err := budgetCtx.Err(); err != nil {
				a.emitEvent(run, core.EventToolCallCancelled, core.ToolCallResultPayload{CallID: tc.ID, Name: tc.Name})
				if ctx.Err() != nil {
					a.finishRun(run, core.RunCancelled, nil)
				} else {
					a.finishRun(run, core.RunFailed, &core.RunError{Code: "budget_exceeded", Message: "wall clock limit during tool execution"})
				}
				return
			}

			if !validTools[tc.Name] {
				var names []string
				for _, ts := range toolSpecs {
					names = append(names, ts.Name)
				}
				errMsg := fmt.Sprintf("tool %q does not exist. Available tools: %v", tc.Name, names)
				a.emitEvent(run, core.EventToolCallFailed, core.ToolCallResultPayload{
					CallID: tc.ID, Name: tc.Name, Content: errMsg, IsError: true,
				})
				msgs = append(msgs, core.Message{
					Role: core.RoleToolResult, ToolCallID: tc.ID,
					Content: fmt.Sprintf("Error: %s", errMsg),
					IsError: true,
				})
				continue
			}

			var argsObj map[string]any
			if err := json.Unmarshal(tc.Args, &argsObj); err != nil {
				errMsg := fmt.Sprintf("invalid JSON arguments for tool %q: %s", tc.Name, err.Error())
				a.emitEvent(run, core.EventToolCallFailed, core.ToolCallResultPayload{
					CallID: tc.ID, Name: tc.Name, Content: errMsg, IsError: true,
				})
				msgs = append(msgs, core.Message{
					Role: core.RoleToolResult, ToolCallID: tc.ID,
					Content: fmt.Sprintf("Error: %s. Please provide a valid JSON object as arguments.", errMsg),
					IsError: true,
				})
				continue
			}

			a.emitEvent(run, core.EventToolCallStarted, core.ToolCallRequestedPayload{
				CallID: tc.ID,
				Name:   tc.Name,
				Args:   tc.Args,
			})

			// per-tool timeout
			toolCtx, toolCancel := context.WithTimeout(budgetCtx, a.toolTimeout)
			result, err := a.tools.CallTool(toolCtx, tc.Name, tc.Args)
			toolCancel()

			if err != nil {
				if budgetCtx.Err() != nil {
					a.emitEvent(run, core.EventToolCallCancelled, core.ToolCallResultPayload{CallID: tc.ID, Name: tc.Name})
					if ctx.Err() != nil {
						a.finishRun(run, core.RunCancelled, nil)
					} else {
						a.finishRun(run, core.RunFailed, &core.RunError{Code: "budget_exceeded", Message: "wall clock limit during tool execution"})
					}
					return
				}

				errMsg := err.Error()
				if toolCtx.Err() != nil && budgetCtx.Err() == nil {
					errMsg = fmt.Sprintf("tool %q timed out after %s", tc.Name, a.toolTimeout)
				}

				a.emitEvent(run, core.EventToolCallFailed, core.ToolCallResultPayload{
					CallID:  tc.ID,
					Name:    tc.Name,
					Content: errMsg,
					IsError: true,
				})

				msgs = append(msgs, core.Message{
					Role:       core.RoleToolResult,
					ToolCallID: tc.ID,
					Content:    fmt.Sprintf("Error: %s", errMsg),
					IsError:    true,
				})
				continue
			}

			// store full content in event log for traceability
			a.emitEvent(run, core.EventToolCallSucceeded, core.ToolCallResultPayload{
				CallID:  tc.ID,
				Name:    tc.Name,
				Content: result.Content,
				IsError: result.IsError,
			})

			// truncate for model context
			msgs = append(msgs, core.Message{
				Role:       core.RoleToolResult,
				ToolCallID: tc.ID,
				Content:    truncateToolResult(result.Content),
				IsError:    result.IsError,
			})
		}

		if err := a.store.SaveRun(context.Background(), run); err != nil {
			a.logger.Error("save run progress", "err", err)
		}
	}

	a.emitEvent(run, core.EventBudgetExceeded, core.BudgetExceededPayload{
		Reason: "max steps exceeded",
		Limit:  fmt.Sprintf("%d steps", budget.MaxSteps),
	})
	a.finishRun(run, core.RunFailed, &core.RunError{Code: "budget_exceeded", Message: "max steps exceeded"})
}

func (a *Agent) emitEvent(run *core.Run, eventType core.EventType, payload any) {
	event := core.Event{
		SessionID: run.SessionID,
		RunID:     run.ID,
		Type:      eventType,
		Payload:   core.MustMarshalPayload(payload),
		At:        time.Now(),
	}
	if err := a.store.Append(context.Background(), event); err != nil {
		a.logger.Error("emit event: append", "err", err, "type", eventType)
	}
}

func (a *Agent) finishRun(run *core.Run, status core.RunStatus, runErr *core.RunError) {
	now := time.Now()
	run.Status = status
	run.EndedAt = &now
	run.Error = runErr

	var eventType core.EventType
	var payload any
	switch status {
	case core.RunSucceeded:
		eventType = core.EventRunSucceeded
		payload = core.RunTerminalPayload{}
	case core.RunFailed:
		reason := ""
		if runErr != nil {
			reason = runErr.Message
		}
		eventType = core.EventRunFailed
		payload = core.RunTerminalPayload{Reason: reason}
	case core.RunCancelled:
		eventType = core.EventRunCancelled
		payload = core.RunTerminalPayload{}
	case core.RunInterrupted:
		eventType = core.EventRunInterrupted
		payload = core.RunTerminalPayload{}
	}

	event := core.Event{
		SessionID: run.SessionID,
		RunID:     run.ID,
		Type:      eventType,
		Payload:   core.MustMarshalPayload(payload),
		At:        now,
	}

	if err := a.store.AppendAndUpdateRun(context.Background(), event, run); err != nil {
		a.logger.Error("finish run: atomic save", "err", err)
	}
}
