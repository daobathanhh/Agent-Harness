package core

import (
	"encoding/json"
	"time"
)

type SessionStatus string

const (
	SessionActive SessionStatus = "active"
	SessionClosed SessionStatus = "closed"
)

type RunStatus string

const (
	RunRunning     RunStatus = "running"
	RunSucceeded   RunStatus = "succeeded"
	RunFailed      RunStatus = "failed"
	RunCancelled   RunStatus = "cancelled"
	RunInterrupted RunStatus = "interrupted"
)

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleToolResult Role = "tool_result"
)

type Session struct {
	ID           string        `json:"id"`
	Model        string        `json:"model"`
	SystemPrompt string        `json:"system_prompt"`
	Status       SessionStatus `json:"status"`
	CreatedAt    time.Time     `json:"created_at"`
}

type Run struct {
	ID         string     `json:"id"`
	SessionID  string     `json:"session_id"`
	Status     RunStatus  `json:"status"`
	StepCount  int        `json:"step_count"`
	TokensUsed int        `json:"tokens_used"`
	StartedAt  time.Time  `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
	Error      *RunError  `json:"error,omitempty"`
}

type RunError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Event struct {
	Seq       int64           `json:"seq"`
	SessionID string          `json:"session_id"`
	RunID     string          `json:"run_id"`
	Type      EventType       `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	At        time.Time       `json:"at"`
}

type Message struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type ToolCall struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type ToolResult struct {
	CallID  string `json:"call_id"`
	Content string `json:"content"`
	IsError bool   `json:"is_error"`
}

type TokenUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type LLMResponse struct {
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	Usage      TokenUsage `json:"usage"`
	StopReason string     `json:"stop_reason"`
}

type SessionOpts struct {
	Model        string `json:"model"`
	SystemPrompt string `json:"system_prompt"`
}

type Budget struct {
	MaxSteps     int           `json:"max_steps"`
	MaxTokens    int           `json:"max_tokens"`
	MaxWallClock time.Duration `json:"max_wall_clock"`
}

func DefaultBudget() Budget {
	return Budget{
		MaxSteps:     25,
		MaxTokens:    100000,
		MaxWallClock: 5 * time.Minute,
	}
}
