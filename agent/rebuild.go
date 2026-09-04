package agent

import (
	"encoding/json"

	"github.com/daobathanh/celesnity/core"
)

func rebuildMessages(events []core.Event) []core.Message {
	var msgs []core.Message
	var pendingToolCalls []core.ToolCall

	for _, e := range events {
		switch e.Type {
		case core.EventRunStarted:
			flushPending(&msgs, &pendingToolCalls)
			var p core.RunStartedPayload
			json.Unmarshal(e.Payload, &p)
			msgs = append(msgs, core.Message{Role: core.RoleUser, Content: p.UserMessage})

		case core.EventModelResponse:
			flushPending(&msgs, &pendingToolCalls)
			var p core.ModelResponsePayload
			json.Unmarshal(e.Payload, &p)
			msg := core.Message{Role: core.RoleAssistant, Content: p.Content, ToolCalls: p.ToolCalls}
			msgs = append(msgs, msg)
			if len(p.ToolCalls) > 0 {
				pendingToolCalls = append(pendingToolCalls[:0], p.ToolCalls...)
			}

		case core.EventToolCallSucceeded:
			var p core.ToolCallResultPayload
			json.Unmarshal(e.Payload, &p)
			msgs = append(msgs, core.Message{
				Role:       core.RoleToolResult,
				ToolCallID: p.CallID,
				Content:    p.Content,
			})
			removePending(&pendingToolCalls, p.CallID)

		case core.EventToolCallFailed:
			var p core.ToolCallResultPayload
			json.Unmarshal(e.Payload, &p)
			msgs = append(msgs, core.Message{
				Role:       core.RoleToolResult,
				ToolCallID: p.CallID,
				Content:    "Error: " + p.Content,
			})
			removePending(&pendingToolCalls, p.CallID)

		case core.EventToolCallCancelled:
			var p core.ToolCallResultPayload
			json.Unmarshal(e.Payload, &p)
			msgs = append(msgs, core.Message{
				Role:       core.RoleToolResult,
				ToolCallID: p.CallID,
				Content:    "Tool execution was interrupted before completion.",
			})
			removePending(&pendingToolCalls, p.CallID)
		}
	}

	flushPending(&msgs, &pendingToolCalls)
	return msgs
}

// flushPending adds synthetic tool_result messages for any tool calls
// that never got a result (server crashed before reaching them).
func flushPending(msgs *[]core.Message, pending *[]core.ToolCall) {
	for _, tc := range *pending {
		*msgs = append(*msgs, core.Message{
			Role:       core.RoleToolResult,
			ToolCallID: tc.ID,
			Content:    "Tool execution was interrupted before completion.",
		})
	}
	*pending = (*pending)[:0]
}

func removePending(pending *[]core.ToolCall, callID string) {
	for i, tc := range *pending {
		if tc.ID == callID {
			*pending = append((*pending)[:i], (*pending)[i+1:]...)
			return
		}
	}
}
