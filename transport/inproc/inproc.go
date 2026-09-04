package inproc

import (
	"context"

	"github.com/daobathanh/celesnity/core"
)

type Client struct {
	agent core.AgentCore
}

func New(agent core.AgentCore) *Client {
	return &Client{agent: agent}
}

func (c *Client) CreateSession(ctx context.Context, opts core.SessionOpts) (*core.Session, error) {
	return c.agent.CreateSession(ctx, opts)
}

func (c *Client) CloseSession(ctx context.Context, sessionID string) error {
	return c.agent.CloseSession(ctx, sessionID)
}

func (c *Client) SendMessage(ctx context.Context, sessionID, msg string) (string, error) {
	return c.agent.SendMessage(ctx, sessionID, msg)
}

func (c *Client) GetRun(ctx context.Context, runID string) (*core.Run, error) {
	return c.agent.GetRun(ctx, runID)
}

func (c *Client) StreamEvents(ctx context.Context, sessionID string, fromSeq int64) (<-chan core.Event, error) {
	return c.agent.StreamEvents(ctx, sessionID, fromSeq)
}

func (c *Client) CancelRun(ctx context.Context, runID string) error {
	return c.agent.CancelRun(ctx, runID)
}
