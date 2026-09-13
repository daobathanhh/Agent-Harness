package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/daobathanh/celesnity/core"
)

type Provider struct {
	client sdk.Client
	model  string
}

func New(apiKey, model string) *Provider {
	var opts []option.RequestOption
	if apiKey != "" {
		opts = append(opts, option.WithAPIKey(apiKey))
	}
	return &Provider{client: sdk.NewClient(opts...), model: model}
}

func (p *Provider) Complete(ctx context.Context, systemPrompt string, msgs []core.Message, tools []core.ToolSpec) (*core.LLMResponse, error) {
	apiMsgs := convertMessages(msgs)

	params := sdk.MessageNewParams{
		Model:     p.model,
		MaxTokens: 16000,
		Messages:  apiMsgs,
	}

	if systemPrompt != "" {
		params.System = []sdk.TextBlockParam{{Text: systemPrompt}}
	}

	apiTools, err := convertTools(tools)
	if err != nil {
		return nil, fmt.Errorf("convert tools: %w", err)
	}
	if len(apiTools) > 0 {
		params.Tools = apiTools
	}

	const maxRetries = 3
	backoff := time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		resp, err := p.client.Messages.New(ctx, params)
		if err == nil {
			return convertResponse(resp), nil
		}

		lastErr = err
		if !isRetryable(err, &backoff) {
			return nil, fmt.Errorf("anthropic api: %w", err)
		}
	}

	return nil, fmt.Errorf("anthropic api: %w (after %d retries)", lastErr, maxRetries)
}

func isRetryable(err error, backoff *time.Duration) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var apiErr *sdk.Error
	if !errors.As(err, &apiErr) {
		return false
	}

	switch apiErr.StatusCode {
	case 429:
		if apiErr.Response != nil {
			if ra := apiErr.Response.Header.Get("Retry-After"); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil {
					*backoff = time.Duration(secs) * time.Second
				}
			}
		}
		return true
	case 500, 502, 503, 529:
		return true
	default:
		return false
	}
}

func convertMessages(msgs []core.Message) []sdk.MessageParam {
	var result []sdk.MessageParam

	for i := 0; i < len(msgs); {
		msg := msgs[i]

		switch msg.Role {
		case core.RoleUser:
			result = append(result, sdk.NewUserMessage(sdk.NewTextBlock(msg.Content)))
			i++

		case core.RoleAssistant:
			var blocks []sdk.ContentBlockParamUnion
			if msg.Content != "" {
				blocks = append(blocks, sdk.NewTextBlock(msg.Content))
			}
			for _, tc := range msg.ToolCalls {
				blocks = append(blocks, sdk.NewToolUseBlock(tc.ID, json.RawMessage(tc.Args), tc.Name))
			}
			result = append(result, sdk.NewAssistantMessage(blocks...))
			i++

		case core.RoleToolResult:
			var toolResults []sdk.ContentBlockParamUnion
			for i < len(msgs) && msgs[i].Role == core.RoleToolResult {
				toolResults = append(toolResults, sdk.NewToolResultBlock(
					msgs[i].ToolCallID,
					msgs[i].Content,
					msgs[i].IsError,
				))
				i++
			}
			result = append(result, sdk.NewUserMessage(toolResults...))

		default:
			i++
		}
	}

	return result
}

func convertTools(tools []core.ToolSpec) ([]sdk.ToolUnionParam, error) {
	var result []sdk.ToolUnionParam
	for _, spec := range tools {
		var schema struct {
			Properties map[string]any `json:"properties"`
			Required   []string       `json:"required"`
		}
		if err := json.Unmarshal(spec.InputSchema, &schema); err != nil {
			return nil, fmt.Errorf("unmarshal schema for %s: %w", spec.Name, err)
		}

		t := sdk.ToolUnionParamOfTool(sdk.ToolInputSchemaParam{
			Properties: schema.Properties,
			Required:   schema.Required,
		}, spec.Name)
		t.OfTool.Description = sdk.String(spec.Description)
		result = append(result, t)
	}
	return result, nil
}

func convertResponse(resp *sdk.Message) *core.LLMResponse {
	result := &core.LLMResponse{
		StopReason: string(resp.StopReason),
		Usage: core.TokenUsage{
			InputTokens:  int(resp.Usage.InputTokens),
			OutputTokens: int(resp.Usage.OutputTokens),
		},
	}

	for _, block := range resp.Content {
		switch block.Type {
		case "text":
			tb := block.AsText()
			if result.Content != "" {
				result.Content += "\n"
			}
			result.Content += tb.Text
		case "tool_use":
			tb := block.AsToolUse()
			result.ToolCalls = append(result.ToolCalls, core.ToolCall{
				ID:   tb.ID,
				Name: tb.Name,
				Args: tb.Input,
			})
		}
	}

	return result
}
