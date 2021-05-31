package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/rajveermalviya/pubsub-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.Dial(":8080", grpc.WithInsecure())
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

	go func() {
		req := &pb.PublishReq{
			Topic:   "topic1",
			Message: &pb.Message{Data: []byte("Hello")},
		}
		for {
			if _, err := client.Publish(ctx, req); err != nil {
				log.Println("Publish failed: ", err)
				break
			}
		}
	}()

	lastCountTimestamp := time.Now()
	lastMsgCount := uint64(0)

	for {
		time.Sleep(5 * time.Second)

		now := time.Now()
		count := atomic.LoadUint64(&msgCount)
		diff := count - lastMsgCount
		timeDiff := now.Sub(lastCountTimestamp)

		throughPut := float64(diff) / timeDiff.Seconds()

		fmt.Printf("\rThrough put: %f msgs/sec     ", throughPut)

		lastCountTimestamp = now
		lastMsgCount = count
	}
}
