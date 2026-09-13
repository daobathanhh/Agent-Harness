package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daobathanh/celesnity/core"
)

type Client struct {
	cmdPath string
	cmdArgs []string
	logger  *slog.Logger

	mu      sync.Mutex
	writeMu sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	pending map[int64]chan callResult
	nextID  atomic.Int64
	closed  chan struct{}

	toolsMu sync.RWMutex
	tools   []core.ToolSpec
}

type callResult struct {
	data json.RawMessage
	err  error
}

type jsonrpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewClient(command string, args []string, logger *slog.Logger) (*Client, error) {
	c := &Client{
		cmdPath: command,
		cmdArgs: args,
		logger:  logger,
	}

	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	cmd := exec.Command(c.cmdPath, c.cmdArgs...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("mcp: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("mcp: stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("mcp: start: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	closedCh := make(chan struct{})

	c.mu.Lock()
	c.cmd = cmd
	c.stdin = stdin
	c.pending = make(map[int64]chan callResult)
	c.closed = closedCh
	c.mu.Unlock()

	go c.readLoop(scanner, closedCh)

	if err := c.initialize(); err != nil {
		c.kill()
		return fmt.Errorf("mcp: initialize: %w", err)
	}

	c.toolsMu.Lock()
	c.tools = nil
	c.toolsMu.Unlock()

	return nil
}

func (c *Client) reconnect() error {
	c.kill()

	const maxAttempts = 3
	backoff := 500 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c.logger.Info("mcp: reconnecting", "attempt", attempt)
		time.Sleep(backoff)

		if err := c.connect(); err != nil {
			c.logger.Warn("mcp: reconnect failed", "attempt", attempt, "err", err)
			backoff *= 2
			continue
		}
		c.logger.Info("mcp: reconnected")
		return nil
	}
	return fmt.Errorf("mcp: reconnect failed after %d attempts", maxAttempts)
}

func (c *Client) kill() {
	c.mu.Lock()
	if c.stdin != nil {
		c.stdin.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		c.cmd.Process.Kill()
		c.cmd.Wait()
	}
	c.mu.Unlock()
}

func (c *Client) isConnected() bool {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	select {
	case <-closed:
		return false
	default:
		return true
	}
}

func (c *Client) initialize() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.rawCall(ctx, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "celesnity-harness", "version": "1.0.0"},
	})
	if err != nil {
		return fmt.Errorf("initialize handshake failed: %w", err)
	}

	var initResult struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(result, &initResult); err != nil {
		return fmt.Errorf("parse init result: %w", err)
	}
	if initResult.ProtocolVersion != "2024-11-05" {
		c.logger.Warn("mcp: protocol version mismatch", "server", initResult.ProtocolVersion, "client", "2024-11-05")
	}

	c.sendNotification("notifications/initialized", nil)
	return nil
}

func (c *Client) ListTools(ctx context.Context) ([]core.ToolSpec, error) {
	c.toolsMu.RLock()
	if c.tools != nil {
		defer c.toolsMu.RUnlock()
		return c.tools, nil
	}
	c.toolsMu.RUnlock()

	result, err := c.call(ctx, "tools/list", nil)
	if err != nil {
		return nil, fmt.Errorf("mcp: tools/list: %w", err)
	}

	var listResult struct {
		Tools []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(result, &listResult); err != nil {
		return nil, fmt.Errorf("mcp: parse tools: %w", err)
	}

	specs := make([]core.ToolSpec, len(listResult.Tools))
	for i, t := range listResult.Tools {
		specs[i] = core.ToolSpec{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.InputSchema,
		}
	}

	c.toolsMu.Lock()
	c.tools = specs
	c.toolsMu.Unlock()

	return specs, nil
}

func (c *Client) CallTool(ctx context.Context, name string, args json.RawMessage) (*core.ToolResult, error) {
	result, err := c.call(ctx, "tools/call", map[string]any{
		"name":      name,
		"arguments": json.RawMessage(args),
	})
	if err != nil {
		return nil, fmt.Errorf("mcp: tools/call %q: %w", name, err)
	}

	var callResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(result, &callResult); err != nil {
		return nil, fmt.Errorf("mcp: parse tool result: %w", err)
	}

	var content string
	for _, c := range callResult.Content {
		if c.Type == "text" {
			content += c.Text
		}
	}

	return &core.ToolResult{
		CallID:  "",
		Content: content,
		IsError: callResult.IsError,
	}, nil
}

// call wraps rawCall with auto-reconnect on connection failure.
func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	result, err := c.rawCall(ctx, method, params)
	if err == nil {
		return result, nil
	}

	if ctx.Err() != nil {
		return nil, err
	}

	if !c.isConnected() {
		c.logger.Warn("mcp: connection lost, attempting reconnect", "method", method)
		if reconnErr := c.reconnect(); reconnErr != nil {
			return nil, fmt.Errorf("%w (reconnect also failed: %v)", err, reconnErr)
		}
		return c.rawCall(ctx, method, params)
	}

	return nil, err
}

func (c *Client) rawCall(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.nextID.Add(1)

	c.mu.Lock()
	stdin, closed := c.stdin, c.closed
	c.pending[id] = make(chan callResult, 1)
	ch := c.pending[id]
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	req := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	c.writeMu.Lock()
	_, err = fmt.Fprintf(stdin, "%s\n", data)
	c.writeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("write to mcp server: %w", err)
	}

	select {
	case res := <-ch:
		return res.data, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, fmt.Errorf("mcp server connection closed")
	}
}

func (c *Client) sendNotification(method string, params any) {
	req := jsonrpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}
	data, _ := json.Marshal(req)
	c.mu.Lock()
	stdin := c.stdin
	c.mu.Unlock()
	c.writeMu.Lock()
	fmt.Fprintf(stdin, "%s\n", data)
	c.writeMu.Unlock()
}

func (c *Client) readLoop(scanner *bufio.Scanner, closedCh chan struct{}) {
	defer close(closedCh)
	for scanner.Scan() {
		line := scanner.Bytes()
		var resp jsonrpcResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			c.logger.Warn("mcp: malformed response", "err", err)
			continue
		}

		c.mu.Lock()
		ch, ok := c.pending[resp.ID]
		c.mu.Unlock()
		if !ok {
			c.logger.Warn("mcp: response for unknown request", "id", resp.ID)
			continue
		}

		if resp.Error != nil {
			ch <- callResult{err: fmt.Errorf("mcp: JSON-RPC error %d: %s", resp.Error.Code, resp.Error.Message)}
			continue
		}
		ch <- callResult{data: resp.Result}
	}
}

func (c *Client) Close() error {
	c.kill()
	return nil
}
