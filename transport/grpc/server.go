package grpc

import (
	"context"
	"strings"

	"github.com/daobathanh/celesnity/core"
	pb "github.com/daobathanh/celesnity/transport/grpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedHarnessServiceServer
	agent core.AgentCore
}

func NewServer(agent core.AgentCore) *Server {
	return &Server{agent: agent}
}

func (s *Server) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	sess, err := s.agent.CreateSession(ctx, core.SessionOpts{
		Model:        req.Model,
		SystemPrompt: req.SystemPrompt,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.CreateSessionResponse{
		Session: sessionToProto(sess),
	}, nil
}

func (s *Server) CloseSession(ctx context.Context, req *pb.CloseSessionRequest) (*pb.CloseSessionResponse, error) {
	if err := s.agent.CloseSession(ctx, req.SessionId); err != nil {
		return nil, mapError(err)
	}
	return &pb.CloseSessionResponse{}, nil
}

func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	if req.SessionId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "session_id is required")
	}
	if req.Message == "" {
		return nil, status.Errorf(codes.InvalidArgument, "message is required")
	}
	runID, err := s.agent.SendMessage(ctx, req.SessionId, req.Message)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.SendMessageResponse{RunId: runID}, nil
}

func (s *Server) GetRun(ctx context.Context, req *pb.GetRunRequest) (*pb.GetRunResponse, error) {
	run, err := s.agent.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.GetRunResponse{Run: runToProto(run)}, nil
}

func (s *Server) StreamEvents(req *pb.StreamEventsRequest, stream pb.HarnessService_StreamEventsServer) error {
	ch, err := s.agent.StreamEvents(stream.Context(), req.SessionId, req.FromSeq)
	if err != nil {
		return mapError(err)
	}
	for event := range ch {
		if err := stream.Send(eventToProto(&event)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) CancelRun(ctx context.Context, req *pb.CancelRunRequest) (*pb.CancelRunResponse, error) {
	if err := s.agent.CancelRun(ctx, req.RunId); err != nil {
		return nil, mapError(err)
	}
	return &pb.CancelRunResponse{}, nil
}

func sessionToProto(s *core.Session) *pb.SessionMessage {
	return &pb.SessionMessage{
		Id:          s.ID,
		Model:       s.Model,
		SystemPrompt: s.SystemPrompt,
		Status:      string(s.Status),
		CreatedAt:   timestamppb.New(s.CreatedAt),
	}
}

func runToProto(r *core.Run) *pb.RunMessage {
	msg := &pb.RunMessage{
		Id:         r.ID,
		SessionId:  r.SessionID,
		Status:     string(r.Status),
		StepCount:  int32(r.StepCount),
		TokensUsed: int32(r.TokensUsed),
		StartedAt:  timestamppb.New(r.StartedAt),
	}
	if r.EndedAt != nil {
		t := timestamppb.New(*r.EndedAt)
		msg.EndedAt = t
	}
	if r.Error != nil {
		msg.Error = &pb.RunErrorMessage{
			Code:    r.Error.Code,
			Message: r.Error.Message,
		}
	}
	return msg
}

func eventToProto(e *core.Event) *pb.EventMessage {
	return &pb.EventMessage{
		Seq:       e.Seq,
		SessionId: e.SessionID,
		RunId:     e.RunID,
		Type:      string(e.Type),
		Payload:   string(e.Payload),
		At:        timestamppb.New(e.At),
	}
}

func mapError(err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "already has a running run"):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case strings.Contains(msg, "not found"):
		return status.Errorf(codes.NotFound, "%v", err)
	case strings.Contains(msg, "is closed"):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case strings.Contains(msg, "has active run"):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case strings.Contains(msg, "must not be empty"):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case strings.Contains(msg, "not owned"):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
