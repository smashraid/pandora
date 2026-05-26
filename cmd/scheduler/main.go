package main

import (
	"context"
	"log"
	"net"

	pb "github.com/smashraid/pandora/api/loanflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedLoanServiceServer
}

func (s *server) SubmitApplication(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	log.Printf("Received application: %s", req.ApplicationId)
	return &pb.SubmitResponse{TaskId: "task-123"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterLoanServiceServer(s, &server{})

	reflection.Register(s)

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
