package core

import "encoding/json"

type EventType string

const (
	EventRunStarted        EventType = "run_started"
	EventModelRequest      EventType = "model_request"
	EventModelResponse     EventType = "model_response"
	EventToolCallRequested EventType = "tool_call_requested"
	EventToolCallStarted   EventType = "tool_call_started"
	EventToolCallSucceeded EventType = "tool_call_succeeded"
	EventToolCallFailed    EventType = "tool_call_failed"
	EventToolCallCancelled EventType = "tool_call_cancelled"
	EventBudgetExceeded    EventType = "budget_exceeded"
	EventRunSucceeded      EventType = "run_succeeded"
	EventRunFailed         EventType = "run_failed"
	EventRunCancelled      EventType = "run_cancelled"
	EventRunInterrupted    EventType = "run_interrupted"
)

type RunStartedPayload struct {
	UserMessage string `json:"user_message"`
}

type ModelRequestPayload struct {
	Messages  []Message  `json:"messages"`
	ToolSpecs []ToolSpec `json:"tool_specs"`
}

type ModelResponsePayload struct {
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	Usage      TokenUsage `json:"usage"`
	StopReason string     `json:"stop_reason"`
}

type ToolCallRequestedPayload struct {
	CallID string          `json:"call_id"`
	Name   string          `json:"name"`
	Args   json.RawMessage `json:"args"`
}

type ToolCallResultPayload struct {
	CallID  string `json:"call_id"`
	Name    string `json:"name"`
	Content string `json:"content"`
	IsError bool   `json:"is_error"`
}

type BudgetExceededPayload struct {
	Reason string `json:"reason"`
	Limit  string `json:"limit"`
}

type RunTerminalPayload struct {
	Reason string `json:"reason,omitempty"`
}

func MustMarshalPayload(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic("core: failed to marshal event payload: " + err.Error())
	}
	return data
}
