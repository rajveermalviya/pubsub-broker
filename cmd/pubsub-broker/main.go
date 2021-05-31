package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
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
	mu        *sync.RWMutex
	topic     string
	receivers map[uint64]*receiver
}

type receiver struct {
	id           uint64
	clientStream pb.PubSubBroker_SubscribeServer
}

type broker struct {
	grpc_health_v1.UnimplementedHealthServer
	pb.UnimplementedPubSubBrokerServer

	mu     *sync.RWMutex
	topics map[string]*topic

	gcMutex      *sync.RWMutex
	lastCallToGC time.Time
}

var idCounter uint64

func (b *broker) Publish(ctx context.Context, req *pb.PublishReq) (*pb.Empty, error) {
	b.mu.RLock()
	t, ok := b.topics[req.Topic]
	if !ok {
		b.mu.RUnlock()

		// If topic is not found that means no one is subscribed
		// publish is noop
		return &pb.Empty{}, nil
	}
	b.mu.RUnlock()

	t.mu.RLock()
	if len(t.receivers) == 0 {
		t.mu.RUnlock()
		// If topic is found but there are no receivers
		// publish is noop
		return &pb.Empty{}, nil
	}
	t.mu.RUnlock()

	log.Println("sending")

	// Send message to all connected receivers
	t.mu.RLock()
	for _, r := range t.receivers {
		t.mu.RUnlock()

		if err := r.clientStream.Send(req.Message); err != nil {
			switch status.Code(err) {
			case codes.Canceled, codes.Unavailable:
				// Do nothing
			default:
				log.Println("unable to send:", err)
			}
		}

		t.mu.RLock()
	}
	t.mu.RUnlock()

	log.Println("sent")

	return &pb.Empty{}, nil
}

func (b *broker) Subscribe(req *pb.SubscribeReq, stream pb.PubSubBroker_SubscribeServer) error {
	rID := atomic.AddUint64(&idCounter, 1)

	// Keep track of subscriber
	{
		r := &receiver{
			id:           rID,
			clientStream: stream,
		}

		b.mu.RLock()
		t, ok := b.topics[req.Topic]
		if !ok {
			b.mu.RUnlock()

			t = &topic{
				mu:        &sync.RWMutex{},
				topic:     req.Topic,
				receivers: map[uint64]*receiver{rID: r},
			}

			b.mu.Lock()
			b.topics[req.Topic] = t
			b.mu.Unlock()
		} else {
			b.mu.RUnlock()

			t.mu.Lock()
			t.receivers[rID] = r
			t.mu.Unlock()
		}

		log.Println("connected", rID)
	}

	// Wait till client disconnects
	<-stream.Context().Done()

	// Cleanup
	{
		b.mu.RLock()
		t, ok := b.topics[req.Topic]
		if !ok {
			b.mu.RUnlock()
			return nil
		}
		b.mu.RUnlock()

		// Remove receiver
		t.mu.Lock()
		t.receivers[rID] = nil
		delete(t.receivers, rID)
		t.mu.Unlock()

		// Invoke GC manually
		now := time.Now()

		b.gcMutex.RLock()
		if now.Sub(b.lastCallToGC).Seconds() > 60 {
			b.gcMutex.RUnlock()

			b.gcMutex.Lock()
			b.lastCallToGC = now
			b.gcMutex.Unlock()

			log.Println("running GC")
			runtime.GC()
		} else {
			b.gcMutex.RUnlock()
		}

		log.Println("cleaned up", rID)
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
		mu:           &sync.RWMutex{},
		topics:       map[string]*topic{},
		gcMutex:      &sync.RWMutex{},
		lastCallToGC: time.Now(),
	}

	pb.RegisterPubSubBrokerServer(s, b)
	grpc_health_v1.RegisterHealthServer(s, b)

	wg := &sync.WaitGroup{}
	log.Println("starting listener on port:", port)
	wg.Add(1)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	log.Println("got signal:", <-c)

	serverStopped := make(chan struct{})
	wg.Add(1)
	go func() {
		s.GracefulStop()
		close(serverStopped)
		wg.Done()
	}()

	timer := time.NewTimer(5 * time.Second)
	select {
	case <-timer.C:
		s.Stop()
		log.Println("server stopped forcefully")

	case <-serverStopped:
		timer.Stop()
	}

	wg.Wait()
}
