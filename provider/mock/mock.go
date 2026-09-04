package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/daobathanh/celesnity/core"
)

type Provider struct {
	Latency time.Duration
}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Complete(ctx context.Context, _ string, msgs []core.Message, tools []core.ToolSpec) (*core.LLMResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if p.Latency > 0 {
		select {
		case <-time.After(p.Latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	toolResultCount := 0
	for _, m := range msgs {
		if m.Role == core.RoleToolResult {
			toolResultCount++
		}
	}

	lastMsg := msgs[len(msgs)-1]
	content := strings.ToLower(lastMsg.Content)

	if toolResultCount >= 2 {
		return &core.LLMResponse{
			Content:    buildSummary(msgs),
			StopReason: "end_turn",
			Usage:      core.TokenUsage{InputTokens: 200, OutputTokens: 80},
		}, nil
	}

	if toolResultCount == 1 {
		return &core.LLMResponse{
			ToolCalls: []core.ToolCall{{
				ID:   fmt.Sprintf("call_%d", toolResultCount+1),
				Name: "search_maintenance_logs",
				Args: json.RawMessage(`{"query": "recent failures CNC-001", "limit": 5}`),
			}},
			StopReason: "tool_use",
			Usage:      core.TokenUsage{InputTokens: 150, OutputTokens: 40},
		}, nil
	}

	if strings.Contains(content, "work order") || strings.Contains(content, "repair") || strings.Contains(content, "create") {
		return &core.LLMResponse{
			ToolCalls: []core.ToolCall{{
				ID:   "call_1",
				Name: "get_machine_status",
				Args: json.RawMessage(`{"machine_id": "CNC-001"}`),
			}},
			Content:    "Let me check the machine status first before creating a work order.",
			StopReason: "tool_use",
			Usage:      core.TokenUsage{InputTokens: 120, OutputTokens: 35},
		}, nil
	}

	return &core.LLMResponse{
		ToolCalls: []core.ToolCall{{
			ID:   "call_1",
			Name: "get_machine_status",
			Args: json.RawMessage(`{"machine_id": "CNC-001"}`),
		}},
		StopReason: "tool_use",
		Usage:      core.TokenUsage{InputTokens: 100, OutputTokens: 30},
	}, nil
}

func buildSummary(msgs []core.Message) string {
	var toolNames []string
	for _, m := range msgs {
		if m.Role == core.RoleAssistant && len(m.ToolCalls) > 0 {
			for _, tc := range m.ToolCalls {
				toolNames = append(toolNames, tc.Name)
			}
		}
	}
	return fmt.Sprintf("Based on my analysis using %s, machine CNC-001 is running but showing elevated vibration levels (1.2 mm/s). Recent maintenance logs show routine checks were performed. I recommend scheduling a detailed inspection to investigate the vibration trend.", strings.Join(toolNames, " and "))
}
