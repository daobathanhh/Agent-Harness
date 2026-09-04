# Agent Harness

An agent runtime exposed via gRPC that manages sessions, drives a multi-step LLM reasoning loop with MCP tool integration, and supports cancellation, streaming, and crash recovery.

## Quick start

```bash
docker compose up --build -d
```

Or run locally:

```bash
go build -o bin/ ./cmd/...
bin/harness --port 9090 --mcp-cmd bin/mockmcp
```

## Demo

```bash
# Create session and send a task
bin/harnessctl session new
bin/harnessctl send <session-id> "Check CNC-001 status and create a work order"

# Watch events stream in real-time
bin/harnessctl watch <session-id>

# Check run status
bin/harnessctl run <run-id>

# Cancel a running task (use --mock-latency 3s to give time to cancel)
bin/harnessctl cancel <run-id>

# Close a session
bin/harnessctl close <session-id>
```

### Edge case demos

```bash
# Tool failures (100% fail rate). Model sees errors and adapts
bin/harness --port 9090 --mcp-cmd bin/mockmcp --mcp-args="--fail-rate,1.0"

# MCP server drops connection after 1 call, auto-reconnect
bin/harness --port 9090 --mcp-cmd bin/mockmcp --mcp-args="--drop-after-n-calls,1"

# Slow tools + cancel. Start with latency, then cancel mid-run
bin/harness --port 9090 --mcp-cmd bin/mockmcp --mock-latency 3s
```

### With real Anthropic provider

```bash
ANTHROPIC_API_KEY=sk-... bin/harness --port 9090 --provider anthropic --mcp-cmd bin/mockmcp
```

## Tests

```bash
go test -race ./...    # 68 tests, all race-clean
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md).
