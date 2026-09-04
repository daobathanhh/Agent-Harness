package agent

import (
	"github.com/daobathanh/celesnity/core"
)

const (
	maxContextChars     = 400000 // ~100K tokens (rough 4 chars/token estimate)
	maxToolResultChars  = 20000  // truncate individual tool results beyond this
	truncationMarker    = "\n[... truncated]"
)

// truncateMessages drops oldest tool_result content when total size exceeds maxContextChars.
// Preserves: first user message, all assistant messages, most recent tool results.
func truncateMessages(msgs []core.Message) []core.Message {
	total := 0
	for _, m := range msgs {
		total += len(m.Content)
	}
	if total <= maxContextChars {
		return msgs
	}

	// truncate from oldest tool_result forward until under limit
	for i := range msgs {
		if total <= maxContextChars {
			break
		}
		if msgs[i].Role == core.RoleToolResult && len(msgs[i].Content) > 200 {
			saved := len(msgs[i].Content) - 200
			msgs[i].Content = msgs[i].Content[:200] + truncationMarker
			total -= saved
		}
	}
	return msgs
}

// truncateToolResult caps a single tool result to maxToolResultChars.
// Full content should be stored in the event log; only the truncated version goes to the model.
func truncateToolResult(content string) string {
	if len(content) <= maxToolResultChars {
		return content
	}
	return content[:maxToolResultChars] + truncationMarker
}
