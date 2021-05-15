package pubsub

import (
	"context"

	"github.com/rajveermalviya/pubsub-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	c    pb.PubSubBrokerClient
	conn *grpc.ClientConn
}

func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	client := pb.NewPubSubBrokerClient(conn)

	return &Client{client, conn}, nil
}

func (c *Client) Publish(ctx context.Context, topic string, msg *pb.Message) error {
	for {
		err := c.publish(ctx, topic, msg)
		if status.Code(err) == codes.Unavailable {
			// retry
			continue
		}
		if err != nil {
			return err
		}

		return nil
	}
}

func (c *Client) publish(ctx context.Context, topic string, msg *pb.Message) error {
	_, err := c.c.Publish(ctx, &pb.PublishReq{
		Topic: topic,
		Message: &pb.Message{
			Metadata: msg.Metadata,
			Data:     msg.Data,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Subscribe(ctx context.Context, topic string) (pb.PubSubBroker_SubscribeClient, error) {
	for {
		sc, err := c.subscribe(ctx, topic)
		if status.Code(err) == codes.Unavailable {
			// retry
			continue
		}
		if err != nil {
			return nil, err
		}

		return sc, nil
	}
}

func (c *Client) subscribe(ctx context.Context, topic string) (pb.PubSubBroker_SubscribeClient, error) {
	return c.c.Subscribe(ctx, &pb.SubscribeReq{Topic: topic})
}

func (c *Client) Close() error {
	return c.conn.Close()
}
