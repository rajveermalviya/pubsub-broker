package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rajveermalviya/pubsub-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type topic struct {
	topic     string
	receivers map[uint64]*receiver
}

type receiver struct {
	id           uint64
	clientStream pb.PubSubBroker_SubscribeServer
}

type subscribeRequest struct {
	rID          uint64
	req          *pb.SubscribeReq
	clientStream pb.PubSubBroker_SubscribeServer
}

type cleanupRequest struct {
	topic string
	rID   uint64
}

type broker struct {
	grpc_health_v1.UnimplementedHealthServer
	pb.UnimplementedPubSubBrokerServer

	topics map[string]*topic

	publishMessageChan    chan *pb.PublishReq
	subscribeReceiverChan chan subscribeRequest
	cleanupReceiverChan   chan cleanupRequest

	exitChan chan struct{}
	wg       *sync.WaitGroup
}

var idCounter uint64

func (b *broker) run() {
	defer func() {
		log.Println("broker manager stopped")
		b.wg.Done()
	}()

	for {
		select {
		case r := <-b.publishMessageChan:
			b.publish(r)

		case r := <-b.subscribeReceiverChan:
			b.subscribe(r.rID, r.req, r.clientStream)

		case r := <-b.cleanupReceiverChan:
			b.cleanupReceiver(r.topic, r.rID)

		case <-b.exitChan:
			return
		}
	}
}

func (b *broker) publish(req *pb.PublishReq) {
	t, ok := b.topics[req.Topic]
	if !ok {
		// If topic is not found that means no one is subscribed
		// publish is noop
		return
	}

	if len(t.receivers) == 0 {
		// If topic is found but there are no receivers
		// publish is noop
		return
	}

	log.Println("sending")

	// Send message to all connected receivers
	for _, r := range t.receivers {
		if err := r.clientStream.Send(req.Message); err != nil {
			switch status.Code(err) {
			case codes.Canceled, codes.Unavailable:
				// Do nothing
			default:
				log.Println("unable to send:", err)
			}
		}
	}

	log.Println("sent")
}

func (b *broker) subscribe(rID uint64, req *pb.SubscribeReq, stream pb.PubSubBroker_SubscribeServer) {
	t, ok := b.topics[req.Topic]
	if !ok {
		t = &topic{
			topic:     req.Topic,
			receivers: map[uint64]*receiver{},
		}

		b.topics[t.topic] = t
	}

	t.receivers[rID] = &receiver{
		id:           rID,
		clientStream: stream,
	}

	log.Println("connected", rID)
}

func (b *broker) cleanupReceiver(topic string, rID uint64) {
	t, ok := b.topics[topic]
	if !ok {
		return
	}

	// Remove receiver
	t.receivers[rID] = nil
	delete(t.receivers, rID)

	// If receivers empty, then remove topic
	if len(t.receivers) == 0 {
		// Remove topic
		b.topics[topic] = nil
		delete(b.topics, topic)
	}

	log.Println("cleaned up", rID)
}

func (b *broker) Publish(ctx context.Context, req *pb.PublishReq) (*pb.Empty, error) {
	b.publishMessageChan <- req

	return &pb.Empty{}, nil
}

func (b *broker) Subscribe(req *pb.SubscribeReq, stream pb.PubSubBroker_SubscribeServer) error {
	rID := atomic.AddUint64(&idCounter, 1)

	b.subscribeReceiverChan <- subscribeRequest{
		rID:          rID,
		req:          req,
		clientStream: stream,
	}

	<-stream.Context().Done()

	b.cleanupReceiverChan <- cleanupRequest{
		topic: req.Topic,
		rID:   rID,
	}
	return nil
}

func (b *broker) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	b := &broker{
		topics:                map[string]*topic{},
		publishMessageChan:    make(chan *pb.PublishReq),
		subscribeReceiverChan: make(chan subscribeRequest),
		cleanupReceiverChan:   make(chan cleanupRequest),

		exitChan: make(chan struct{}),
		wg:       &sync.WaitGroup{},
	}

	pb.RegisterPubSubBrokerServer(s, b)
	grpc_health_v1.RegisterHealthServer(s, b)

	// Run broker manager
	log.Println("starting broker manager")
	b.wg.Add(1)
	go b.run()

	log.Println("starting listener on port:", port)
	b.wg.Add(1)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		b.wg.Done()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	log.Println("got signal:", <-c)

	// Send exit signal to broker manager
	log.Println("stopping broker manager")
	close(b.exitChan)

	serverStopped := make(chan struct{})
	b.wg.Add(1)
	go func() {
		s.GracefulStop()
		close(serverStopped)
		b.wg.Done()
	}()

	timer := time.NewTimer(5 * time.Second)
	select {
	case <-timer.C:
		s.Stop()
		log.Println("server stopped forcefully")

	case <-serverStopped:
		timer.Stop()
	}

	b.wg.Wait()
}
