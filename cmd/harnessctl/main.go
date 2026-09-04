package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	pb "github.com/daobathanh/celesnity/transport/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	addr := os.Getenv("HARNESS_ADDR")
	if addr == "" {
		addr = "localhost:9090"
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal("connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewHarnessServiceClient(conn)
	ctx := context.Background()

	switch os.Args[1] {
	case "session":
		if len(os.Args) < 3 || os.Args[2] != "new" {
			fatal("usage: harnessctl session new [--model MODEL] [--system-prompt PROMPT]")
		}
		model := "claude-sonnet-4-20250514"
		prompt := "You are a helpful factory operations assistant. You have access to tools for checking machine status, searching maintenance logs, and creating work orders."
		for i := 3; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "--model":
				i++
				model = os.Args[i]
			case "--system-prompt":
				i++
				prompt = os.Args[i]
			}
		}
		resp, err := client.CreateSession(ctx, &pb.CreateSessionRequest{
			Model:        model,
			SystemPrompt: prompt,
		})
		if err != nil {
			fatal("create session: %v", err)
		}
		fmt.Printf("Session created: %s\n", resp.Session.Id)

	case "close":
		if len(os.Args) < 3 {
			fatal("usage: harnessctl close <session-id>")
		}
		_, err := client.CloseSession(ctx, &pb.CloseSessionRequest{SessionId: os.Args[2]})
		if err != nil {
			fatal("close session: %v", err)
		}
		fmt.Println("Session closed")

	case "send":
		if len(os.Args) < 4 {
			fatal("usage: harnessctl send <session-id> <message>")
		}
		sessionID := os.Args[2]
		msg := os.Args[3]
		resp, err := client.SendMessage(ctx, &pb.SendMessageRequest{
			SessionId: sessionID,
			Message:   msg,
		})
		if err != nil {
			fatal("send: %v", err)
		}
		fmt.Printf("Run started: %s\n", resp.RunId)

	case "run":
		if len(os.Args) < 3 {
			fatal("usage: harnessctl run <run-id>")
		}
		resp, err := client.GetRun(ctx, &pb.GetRunRequest{RunId: os.Args[2]})
		if err != nil {
			fatal("get run: %v", err)
		}
		r := resp.Run
		fmt.Printf("Run: %s\n", r.Id)
		fmt.Printf("  Session:  %s\n", r.SessionId)
		fmt.Printf("  Status:   %s\n", r.Status)
		fmt.Printf("  Steps:    %d\n", r.StepCount)
		fmt.Printf("  Tokens:   %d\n", r.TokensUsed)
		fmt.Printf("  Started:  %s\n", r.StartedAt.AsTime().Format(time.RFC3339))
		if r.EndedAt != nil {
			fmt.Printf("  Ended:    %s\n", r.EndedAt.AsTime().Format(time.RFC3339))
		}
		if r.Error != nil {
			fmt.Printf("  Error:    [%s] %s\n", r.Error.Code, r.Error.Message)
		}

	case "watch":
		if len(os.Args) < 3 {
			fatal("usage: harnessctl watch <session-id> [--from-seq N]")
		}
		sessionID := os.Args[2]
		var fromSeq int64
		for i := 3; i < len(os.Args); i++ {
			if os.Args[i] == "--from-seq" && i+1 < len(os.Args) {
				i++
				n, _ := strconv.ParseInt(os.Args[i], 10, 64)
				fromSeq = n
			}
		}
		stream, err := client.StreamEvents(ctx, &pb.StreamEventsRequest{
			SessionId: sessionID,
			FromSeq:   fromSeq,
		})
		if err != nil {
			fatal("stream: %v", err)
		}
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				fatal("recv: %v", err)
			}
			printEvent(event)
		}

	case "cancel":
		if len(os.Args) < 3 {
			fatal("usage: harnessctl cancel <run-id>")
		}
		_, err := client.CancelRun(ctx, &pb.CancelRunRequest{RunId: os.Args[2]})
		if err != nil {
			fatal("cancel: %v", err)
		}
		fmt.Println("Cancel requested")

	default:
		usage()
		os.Exit(1)
	}
}

func printEvent(e *pb.EventMessage) {
	ts := e.At.AsTime().Format("15:04:05.000")
	var detail string
	switch e.Type {
	case "run_started":
		var p struct{ UserMessage string `json:"user_message"` }
		json.Unmarshal([]byte(e.Payload), &p)
		detail = truncate(p.UserMessage, 80)
	case "model_response":
		var p struct {
			Content   string `json:"content"`
			ToolCalls []struct{ Name string `json:"name"` } `json:"tool_calls"`
			Usage     struct{ InputTokens, OutputTokens int } `json:"usage"`
		}
		json.Unmarshal([]byte(e.Payload), &p)
		if len(p.ToolCalls) > 0 {
			names := ""
			for i, tc := range p.ToolCalls {
				if i > 0 { names += ", " }
				names += tc.Name
			}
			detail = fmt.Sprintf("tool_calls=[%s] tokens=%d+%d", names, p.Usage.InputTokens, p.Usage.OutputTokens)
		} else {
			detail = fmt.Sprintf("content=%q tokens=%d+%d", truncate(p.Content, 60), p.Usage.InputTokens, p.Usage.OutputTokens)
		}
	case "tool_call_requested":
		var p struct {
			Name string          `json:"name"`
			Args json.RawMessage `json:"args"`
		}
		json.Unmarshal([]byte(e.Payload), &p)
		detail = fmt.Sprintf("%s(%s)", p.Name, truncate(string(p.Args), 60))
	case "tool_call_succeeded":
		var p struct {
			Name    string `json:"name"`
			Content string `json:"content"`
		}
		json.Unmarshal([]byte(e.Payload), &p)
		detail = fmt.Sprintf("%s -> %s", p.Name, truncate(p.Content, 60))
	case "tool_call_failed":
		var p struct {
			Name    string `json:"name"`
			Content string `json:"content"`
		}
		json.Unmarshal([]byte(e.Payload), &p)
		detail = fmt.Sprintf("%s ERROR: %s", p.Name, truncate(p.Content, 60))
	case "tool_call_cancelled":
		var p struct{ Name string `json:"name"` }
		json.Unmarshal([]byte(e.Payload), &p)
		detail = p.Name + " CANCELLED"
	case "budget_exceeded":
		var p struct {
			Reason string `json:"reason"`
			Limit  string `json:"limit"`
		}
		json.Unmarshal([]byte(e.Payload), &p)
		detail = fmt.Sprintf("%s (limit: %s)", p.Reason, p.Limit)
	case "run_succeeded", "run_failed", "run_cancelled", "run_interrupted":
		var p struct{ Reason string `json:"reason"` }
		json.Unmarshal([]byte(e.Payload), &p)
		if p.Reason != "" {
			detail = p.Reason
		}
	default:
		detail = truncate(e.Payload, 80)
	}

	fmt.Printf("[%s] seq=%d %-22s %s\n", ts, e.Seq, e.Type, detail)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: harnessctl <command>

Commands:
  session new [--model M] [--system-prompt P]   Create a new session
  close <session-id>                             Close a session
  send <session-id> <message>                    Send a message (starts a run)
  run <run-id>                                   Get run status
  watch <session-id> [--from-seq N]              Stream events
  cancel <run-id>                                Cancel a running run
`)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
