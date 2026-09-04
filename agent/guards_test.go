package agent

import (
	"strings"
	"testing"

	"github.com/daobathanh/celesnity/core"
)

func TestTruncateToolResult(t *testing.T) {
	short := "short result"
	if got := truncateToolResult(short); got != short {
		t.Fatalf("short result should not be truncated")
	}

	long := strings.Repeat("x", maxToolResultChars+1000)
	got := truncateToolResult(long)
	if len(got) > maxToolResultChars+len(truncationMarker)+10 {
		t.Fatalf("truncated result too long: %d", len(got))
	}
	if !strings.HasSuffix(got, truncationMarker) {
		t.Fatal("truncated result missing marker")
	}
}

func TestTruncateMessages(t *testing.T) {
	msgs := []core.Message{
		{Role: core.RoleUser, Content: "hello"},
		{Role: core.RoleAssistant, Content: "let me check"},
		{Role: core.RoleToolResult, ToolCallID: "c1", Content: strings.Repeat("a", 300000)},
		{Role: core.RoleToolResult, ToolCallID: "c2", Content: strings.Repeat("b", 300000)},
	}

	result := truncateMessages(msgs)
	total := 0
	for _, m := range result {
		total += len(m.Content)
	}
	if total > maxContextChars+1000 {
		t.Fatalf("messages not truncated enough: %d chars", total)
	}

	// user and assistant messages should be untouched
	if result[0].Content != "hello" {
		t.Fatal("user message was modified")
	}
	if result[1].Content != "let me check" {
		t.Fatal("assistant message was modified")
	}
}

func TestTruncateMessagesNoOpWhenSmall(t *testing.T) {
	msgs := []core.Message{
		{Role: core.RoleUser, Content: "hello"},
		{Role: core.RoleToolResult, ToolCallID: "c1", Content: "small result"},
	}
	result := truncateMessages(msgs)
	if result[1].Content != "small result" {
		t.Fatal("small messages should not be truncated")
	}
}
