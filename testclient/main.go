package main

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/rajveermalviya/pubsub-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalln("unable to dial: ", err)
	}
	defer conn.Close()

	client := pb.NewPubSubBrokerClient(conn)

	const numSubs = 8000

	msgCount := uint64(0)

	for i := 1; i <= numSubs; i++ {
		c, err2 := client.Subscribe(ctx, &pb.SubscribeReq{Topic: "topic1"})
		if err2 != nil {
			log.Fatalln(i, ":", "unable to subscribe: ", err2)
		}

		go func(i int) {
			for {
				_, err3 := c.Recv()
				if status.Code(err3) == codes.Canceled {
					break
				}
				if err3 != nil {
					log.Println(i, ":", "unable to recv: ", err3)
				}

				atomic.AddUint64(&msgCount, 1)
			}
		}(i)
	}

	for {
		req := &pb.PublishReq{
			Topic:   "topic1",
			Message: &pb.Message{Data: []byte("Hello")},
		}
		if _, err := client.Publish(ctx, req); err != nil {
			log.Println("Publish failed: ", err)
			break
		}

		log.Println(atomic.LoadUint64(&msgCount))
	}
}
