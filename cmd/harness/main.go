package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/daobathanh/celesnity/agent"
	"github.com/daobathanh/celesnity/core"
	anthropicprovider "github.com/daobathanh/celesnity/provider/anthropic"
	mockprovider "github.com/daobathanh/celesnity/provider/mock"
	"github.com/daobathanh/celesnity/store/sqlite"
	"github.com/daobathanh/celesnity/tools/mcp"
	grpctransport "github.com/daobathanh/celesnity/transport/grpc"
	pb "github.com/daobathanh/celesnity/transport/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	port := flag.Int("port", 9090, "gRPC listen port")
	dbPath := flag.String("db", "harness.db", "SQLite database path")
	mcpCmd := flag.String("mcp-cmd", "", "MCP server command (e.g. ./mockmcp)")
	mcpArgs := flag.String("mcp-args", "", "MCP server args, comma-separated")
	providerFlag := flag.String("provider", "mock", "LLM provider: mock or anthropic")
	model := flag.String("model", "claude-sonnet-4-6", "Model ID (for anthropic provider)")
	mockLatency := flag.Duration("mock-latency", 0, "Mock provider latency per call (e.g. 2s)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store, err := sqlite.New(*dbPath)
	if err != nil {
		logger.Error("failed to open store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	recovered, err := store.MarkInterruptedRuns(context.Background())
	if err != nil {
		logger.Error("failed to mark interrupted runs", "err", err)
	} else if recovered > 0 {
		logger.Info("marked interrupted runs from previous crash", "count", recovered)
	}

	var provider core.LLMProvider
	switch *providerFlag {
	case "anthropic":
		apiKey := os.Getenv("ANTHROPIC_API_KEY")
		if apiKey == "" {
			logger.Error("ANTHROPIC_API_KEY env var required for anthropic provider")
			os.Exit(1)
		}
		provider = anthropicprovider.New(apiKey, *model)
		logger.Info("using anthropic provider", "model", *model)
	default:
		mp := mockprovider.New()
		mp.Latency = *mockLatency
		provider = mp
		logger.Info("using mock provider", "latency", *mockLatency)
	}

	var mcpArgsList []string
	if *mcpArgs != "" {
		mcpArgsList = splitArgs(*mcpArgs)
	}

	if *mcpCmd == "" {
		logger.Error("--mcp-cmd is required")
		os.Exit(1)
	}

	toolRegistry, err := mcp.NewClient(*mcpCmd, mcpArgsList, logger)
	if err != nil {
		logger.Error("failed to start MCP server", "err", err)
		os.Exit(1)
	}
	defer toolRegistry.Close()

	agentCore := agent.New(provider, toolRegistry, store, logger)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Error("failed to listen", "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	pb.RegisterHarnessServiceServer(srv, grpctransport.NewServer(agentCore))
	reflection.Register(srv)

	go func() {
		logger.Info("harness server started", "port", *port)
		if err := srv.Serve(lis); err != nil {
			logger.Error("server error", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down...")
	agentCore.Shutdown()
	srv.GracefulStop()
}

func splitArgs(s string) []string {
	var args []string
	current := ""
	for _, c := range s {
		if c == ',' {
			if current != "" {
				args = append(args, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		args = append(args, current)
	}
	return args
}
