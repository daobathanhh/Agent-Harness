# Architecture: Agent Harness

An agent runtime that a platform drives over gRPC. The agent reasons in a loop, calling an LLM, executing MCP tools, and feeding results back until the task is done or a budget is exceeded. Sessions persist in SQLite via event sourcing. Every tool call and model response is stored for traceability and resume.

## Boundaries

```
                ┌─────────────┐
                │  transport/ │  gRPC server-streaming, inproc passthrough
                │  grpc/      │  thin adapter, no business logic
                │  inproc/    │  proves boundary holds under real use
                └──────┬──────┘
                       │ core.AgentCore interface
                ┌──────▼──────┐
                │   agent/    │  loop, budget, cancel, no-progress detection
                │             │  only depends on interfaces in core/
                └──┬───┬───┬──┘
                   │   │   │
         ┌─────────┘   │   └─────────┐
         ▼             ▼             ▼
   core.LLMProvider  core.ToolRegistry  core.EventStore
         │             │                 │
   ┌─────▼─────┐ ┌────▼────┐     ┌─────▼─────┐
   │ provider/ │ │ tools/  │     │  store/   │
   │ anthropic/│ │ mcp/    │     │  sqlite/  │
   │ mock/     │ │         │     │           │
   └───────────┘ └─────────┘     └───────────┘
```

### Why the boundaries are where they are

**`agent/` imports nothing concrete.** The agent loop is the core asset. It decides when to call the model, when to execute tools, when to stop. Everything else (which model, which tools, where to store events) is a deployment decision, not a loop decision. This is enforced by Go's import graph: `agent/` imports only `core/`, which contains only interfaces and types. The result: you can test the full agent loop with in-memory fakes, swap Anthropic for a mock with zero code changes, or replace SQLite with Postgres without touching the loop.

**`transport/` is a thin adapter, not a layer.** The gRPC server is ~80 lines that translate protobuf to domain types and map errors to gRPC status codes. No business logic lives here. No validation beyond field presence, no retry logic, no state. This means the transport is disposable: `inproc/` (30 lines, same interface, no serialization) proves the boundary holds. Six integration tests run the full agent loop through `inproc/`, exercising happy path, cancel, streaming, tool failure, session close, and concurrent run rejection. The agent doesn't know which transport is calling it.

**`core/` is types + interfaces, never logic.** Putting interfaces in a shared package (rather than next to their implementations) prevents import cycles and makes the dependency direction explicit: `agent/` depends on `core/`, `provider/anthropic/` depends on `core/`, but they never depend on each other.

**MCP client is separate from agent.** The agent calls `ToolRegistry.CallTool()`. It doesn't know tools come from a subprocess over JSON-RPC. The MCP client handles its own lifecycle (connect, reconnect, kill), and the agent never sees a pipe or a process. This means tool execution could come from an HTTP service, a local function, or a test stub, all behind the same interface.

## Key decisions

| Decision                       | Why                                                                                                                                                 | Trade-off                                                                                   |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Event sourcing + SQLite        | Solves persistence, resume, and traceability in one design. WAL mode avoids reader-writer deadlock.                                                 | SQLite is single-node. Would need Postgres or a message broker for multi-tenant production. |
| Sequential tool execution      | Simpler to reason about cancellation and error handling. No partial-failure states.                                                                 | Slower when tools are independent. Would use bounded errgroup with more time.               |
| Self-written MCP client        | MCP is just JSON-RPC 2.0 over stdio with NDJSON framing. Writing it proves understanding of the protocol, not dependence on a library.              | More code to maintain.                                                                      |
| System prompt as API parameter | `LLMProvider.Complete()` accepts `systemPrompt` separately, letting the Anthropic provider use the native `System` field. Mock provider ignores it. | Interface is slightly wider than the minimum.                                               |

## Failure handling

| Failure                      | Detection                                                          | Agent behavior                                                                         |
| ---------------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| Tool returns error           | `ToolResult.IsError` or `CallTool` error                           | Error fed back to model as tool_result. Model decides next step                        |
| Tool doesn't exist           | Name not in `toolSpecs`                                            | Structured error with available tool list fed to model                                 |
| Tool timeout                 | Per-tool `context.WithTimeout` (30s default)                       | `tool_call_failed` event, error fed to model                                           |
| No progress                  | Same tool+args called 3 times                                      | Run aborted with `no_progress` error code                                              |
| Budget exceeded              | MaxSteps, MaxTokens, MaxWallClock checked each iteration           | `budget_exceeded` event with reason, then `run_failed`                                 |
| Cancel mid-LLM               | `context.Context` cancellation propagates to provider              | `run_cancelled`                                                                        |
| Cancel mid-tool              | Context propagates through tool timeout ctx                        | `tool_call_cancelled` then `run_cancelled`                                             |
| Server crash                 | On restart, `MarkInterruptedRuns` marks `running` as `interrupted` | Next `SendMessage` rebuilds message history from event log without re-executing tools  |
| MCP server exit              | `readLoop` detects pipe close                                      | Auto-reconnect with exponential backoff (3 attempts). Tools cache cleared on reconnect |
| Large tool result            | Content exceeds 20KB                                               | Truncated for model context. Full content stored in event log for traceability         |
| Context overflow             | Total message chars > 400K                                         | Oldest tool_result content truncated first. User messages and structure preserved      |
| Provider 429/5xx + network   | HTTP status or `net.Error` from Anthropic API                      | Retry with exponential backoff. 429 respects Retry-After header. Max 3 retries         |
| Concurrent send              | `GetActiveRun` check + partial unique index on `runs(session_id)`  | Rejected with `AlreadyExists` gRPC status. One active run per session                  |
| Session closed               | `SessionStatus` check in `SendMessage`                             | Rejected with `FailedPrecondition` gRPC status                                         |
| Invalid tool args JSON       | `json.Unmarshal` into `map[string]any`                             | Error fed to model with corrective message; `tool_call_failed` event emitted           |
| MCP JSON-RPC error           | Server returns error object                                        | Proper Go error propagated (not raw JSON bytes)                                        |
| Terminal event + run state   | `AppendAndUpdateRun` SQLite transaction                            | Atomic — no window where event is committed but run status is stale                    |
| Stream subscriber disconnect | `broadcast` checks `closed` flag under RLock, skips closed subs    | No panic on send-to-closed-channel. Slow consumers get events dropped                  |
| Empty message                | Rejected at both gRPC (`InvalidArgument`) and agent layer          | Prevents creating a run with no user input                                             |
| Graceful shutdown            | `Shutdown()` cancels all active runs before stopping gRPC          | Runs get `run_cancelled` events instead of being orphaned                              |

## What breaks first under production load

1. **SQLite write throughput.** Every event is a write, and SQLite serializes writes even in WAL mode. A single high-throughput session (fast tool calls, verbose events) won't be a problem, but 100+ concurrent sessions will queue behind the write lock. Fix: per-session DB files, or move to Postgres with connection pooling.

2. **In-process fan-out.** `Subscribe` uses Go channels with a 64-event buffer. A slow consumer (e.g., a client on a bad network) drops events silently. With many subscribers per session, broadcast holds a read lock for the duration of iteration. Fix: dedicated event bus (NATS, Redis Streams) with per-consumer cursor and backpressure.

3. **Single-process state.** All cancel functions, active run tracking, and subscriber registries live in memory. If the process restarts, `MarkInterruptedRuns` catches stale runs, but in-flight cancel signals and subscriber connections are lost. Fix: externalize run coordination (e.g., distributed lock, or let the client poll + retry).

4. **No rate limiting.** Any client can create unlimited sessions and runs. A misbehaving caller can exhaust LLM quota, fill the event store, or starve other sessions. Fix: per-session and per-caller rate limits at the transport layer.

5. **Sequential tool execution.** Each tool call blocks the loop. If the model requests 5 independent tools, latency is the sum, not the max. Fix: bounded `errgroup` with partial-result handling on cancel.

## What I'd change with more time

1. **Parallel tool calls**: Bounded errgroup, partial results on cancel, respect tool dependency hints
2. **Idempotency for side-effect tools**: `create_work_order` is dangerous to retry blindly; needs idempotency key + confirmation step before execution
3. **JSON Schema validation**: Validate tool args against the full input schema (not just valid JSON) before execution, return structured validation error to model
4. **Multi-tenant isolation**: per-session resource limits, separate SQLite files or move to Postgres
5. **Event stream scaling**: Push to message broker (NATS/Redis Streams) when multiple consumers need real-time events
6. **Streaming responses**: Stream model output token-by-token to reduce time-to-first-event for the client
7. **Structured error types**: Replace string-matching in gRPC error mapping with typed sentinel errors from the agent layer

## Test coverage

68 tests across 4 packages, all passing with `-race`:

| Package             | Tests | What they cover                                                                                                                                                                                                                                                                                                          |
| ------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `agent/`            | 34    | Happy path, cancel (mid-LLM, mid-tool, finished run, shutdown), budget (steps, tokens, wall-clock), no-progress, tool error, unknown tool, invalid JSON args, partial failure, isError, large result, multi-turn history, resume from crash, session close, empty message, concurrent run, event ordering, stream replay |
| `store/sqlite/`     | 16    | CRUD, active run tracking, atomic event+run, MarkInterruptedRuns, subscribe/broadcast, concurrent unsub, WAL mode, persistence across restart                                                                                                                                                                            |
| `tools/mcp/`        | 12    | Tool listing, caching, tool call, latency, context cancellation, fail rate, reconnect after drop, reconnect failure (permanent), JSON-RPC error, connection state                                                                                                                                                        |
| `transport/inproc/` | 6     | Full agent loop through inproc transport: happy path, streaming, cancel, tool failure, session close, concurrent run rejection                                                                                                                                                                                           |
