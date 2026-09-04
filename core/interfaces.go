package core

import (
	"context"
	"encoding/json"
)

type LLMProvider interface {
	Complete(ctx context.Context, systemPrompt string, msgs []Message, tools []ToolSpec) (*LLMResponse, error)
}

type ToolRegistry interface {
	ListTools(ctx context.Context) ([]ToolSpec, error)
	CallTool(ctx context.Context, name string, args json.RawMessage) (*ToolResult, error)
}

type EventStore interface {
	Append(ctx context.Context, event Event) error
	AppendAndUpdateRun(ctx context.Context, event Event, run *Run) error
	LoadFrom(ctx context.Context, sessionID string, fromSeq int64) ([]Event, error)
	NextSeq(ctx context.Context, sessionID string) (int64, error)

	Subscribe(sessionID string) (<-chan Event, func())

	SaveSession(ctx context.Context, session *Session) error
	GetSession(ctx context.Context, sessionID string) (*Session, error)

	SaveRun(ctx context.Context, run *Run) error
	GetRun(ctx context.Context, runID string) (*Run, error)
	GetActiveRun(ctx context.Context, sessionID string) (*Run, error)
}

type AgentCore interface {
	CreateSession(ctx context.Context, opts SessionOpts) (*Session, error)
	CloseSession(ctx context.Context, sessionID string) error
	SendMessage(ctx context.Context, sessionID, msg string) (string, error)
	GetRun(ctx context.Context, runID string) (*Run, error)
	StreamEvents(ctx context.Context, sessionID string, fromSeq int64) (<-chan Event, error)
	CancelRun(ctx context.Context, runID string) error
}
