package main

import (
	"context"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/smashraid/pandora/api/loanflow/v1"
)

func main() {
	conn, err := grpc.NewClient("localhost:8000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewLoanServiceClient(conn)

	// Submit application
	submitResp, err := client.SubmitApplication(context.Background(), &pb.SubmitRequest{ApplicationId: "loan-456"})
	if err != nil {
		log.Fatalf("submit failed: %v", err)
	}
	taskID := submitResp.TaskId
	log.Printf("Submitted application, task_id: %s", taskID)

	// Open stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.TrackProgress(ctx)
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	// Send task_id
	if err := stream.Send(&pb.TrackRequest{TaskId: taskID}); err != nil {
		log.Fatalf("failed to send task_id: %v", err)
	}

	// Receive progress
	done := make(chan bool)
	go func() {
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("stream error: %v", err)
				break
			}
			log.Printf("Progress: %s - %d%% - %s", event.Step, event.Percent, event.Message)
		}
		done <- true
	}()

	// Cancel after 5 seconds
	time.Sleep(5 * time.Second)
	log.Println("Sending cancel request...")
	if err := stream.Send(&pb.TrackRequest{TaskId: taskID, Cancel: true}); err != nil {
		log.Printf("failed to send cancel: %v", err)
	}

	<-done
	log.Println("Client finished")
}
