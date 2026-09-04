package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

var (
	failRate     = flag.Float64("fail-rate", 0, "probability of tool returning error (0-1)")
	latencyMs    = flag.Int("latency-ms", 0, "artificial latency per tool call in ms")
	dropAfterN   = flag.Int("drop-after-n-calls", 0, "exit after N tool calls (0 = never)")
)

var callCount atomic.Int64

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   *jsonrpcError `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func main() {
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		var req jsonrpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		resp := handleRequest(req)
		if resp == nil {
			continue
		}
		data, _ := json.Marshal(resp)
		fmt.Fprintln(os.Stdout, string(data))
	}
}

func handleRequest(req jsonrpcRequest) *jsonrpcResponse {
	switch req.Method {
	case "initialize":
		return &jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":   map[string]any{"tools": map[string]any{}},
				"serverInfo":     map[string]any{"name": "factory-mock-mcp", "version": "1.0.0"},
			},
		}

	case "notifications/initialized":
		return nil

	case "tools/list":
		return &jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"tools": []map[string]any{
					{
						"name":        "get_machine_status",
						"description": "Get the current operational status of a factory machine",
						"inputSchema": map[string]any{
							"type":       "object",
							"properties": map[string]any{"machine_id": map[string]any{"type": "string", "description": "Machine identifier (e.g. CNC-001)"}},
							"required":   []string{"machine_id"},
						},
					},
					{
						"name":        "search_maintenance_logs",
						"description": "Search maintenance logs for a factory machine",
						"inputSchema": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"query": map[string]any{"type": "string", "description": "Search query"},
								"limit": map[string]any{"type": "integer", "description": "Max results", "default": 10},
							},
							"required": []string{"query"},
						},
					},
					{
						"name":        "create_work_order",
						"description": "Create a maintenance work order for a machine. This has side effects.",
						"inputSchema": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"machine_id":  map[string]any{"type": "string", "description": "Machine identifier"},
								"description": map[string]any{"type": "string", "description": "Work description"},
								"priority":    map[string]any{"type": "string", "enum": []string{"low", "medium", "high", "critical"}},
							},
							"required": []string{"machine_id", "description", "priority"},
						},
					},
				},
			},
		}

	case "tools/call":
		n := callCount.Add(1)
		if *dropAfterN > 0 && int(n) > *dropAfterN {
			os.Exit(1)
		}

		if *latencyMs > 0 {
			time.Sleep(time.Duration(*latencyMs) * time.Millisecond)
		}

		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		json.Unmarshal(req.Params, &params)

		if *failRate > 0 && rand.Float64() < *failRate {
			return &jsonrpcResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: map[string]any{
					"content": []map[string]any{{
						"type": "text",
						"text": fmt.Sprintf("Error: tool %q failed due to simulated fault", params.Name),
					}},
					"isError": true,
				},
			}
		}

		result := executeTool(params.Name, params.Arguments)
		return &jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  result,
		}

	default:
		return &jsonrpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &jsonrpcError{Code: -32601, Message: fmt.Sprintf("method not found: %s", req.Method)},
		}
	}
}

func executeTool(name string, args json.RawMessage) map[string]any {
	switch name {
	case "get_machine_status":
		var p struct{ MachineID string `json:"machine_id"` }
		json.Unmarshal(args, &p)
		return map[string]any{
			"content": []map[string]any{{
				"type": "text",
				"text": fmt.Sprintf(`{"machine_id":"%s","status":"running","temperature_c":72.5,"vibration_mm_s":1.2,"uptime_hours":1847,"last_maintenance":"2026-08-15T10:00:00Z","alerts":["vibration_trending_up"]}`, p.MachineID),
			}},
		}

	case "search_maintenance_logs":
		var p struct {
			Query string `json:"query"`
			Limit int    `json:"limit"`
		}
		json.Unmarshal(args, &p)
		if p.Limit == 0 {
			p.Limit = 10
		}
		logs := make([]map[string]any, 0, p.Limit)
		for i := 0; i < p.Limit && i < 3; i++ {
			logs = append(logs, map[string]any{
				"id":        fmt.Sprintf("LOG-%04d", 1000+i),
				"date":      fmt.Sprintf("2026-08-%02dT09:00:00Z", 10+i),
				"machine":   "CNC-001",
				"type":      "preventive",
				"summary":   fmt.Sprintf("Routine check #%d, replaced coolant filter, calibrated spindle", i+1),
				"technician": "T. Nguyen",
			})
		}
		data, _ := json.Marshal(logs)
		return map[string]any{
			"content": []map[string]any{{
				"type": "text",
				"text": string(data),
			}},
		}

	case "create_work_order":
		var p struct {
			MachineID   string `json:"machine_id"`
			Description string `json:"description"`
			Priority    string `json:"priority"`
		}
		json.Unmarshal(args, &p)
		return map[string]any{
			"content": []map[string]any{{
				"type": "text",
				"text": fmt.Sprintf(`{"work_order_id":"WO-2026-0042","machine_id":"%s","description":"%s","priority":"%s","status":"created","created_at":"2026-09-02T14:30:00Z","assigned_to":"Maintenance Team A"}`, p.MachineID, p.Description, p.Priority),
			}},
		}

	default:
		return map[string]any{
			"content": []map[string]any{{
				"type": "text",
				"text": fmt.Sprintf("Error: unknown tool %q", name),
			}},
			"isError": true,
		}
	}
}
