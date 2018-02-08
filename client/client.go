// Package client provides a gRPC client to the wikipath server.
package client

import (
	"fmt"

	pb "github.com/johnsiilver/wikipath/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Client provides a client to the wikipath service.
type Client struct {
	conn   *grpc.ClientConn
	client pb.SPFServiceClient
}

// New is the constructor for Client.
func New(addr string, port int) (*Client, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", addr, port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	// defer conn.Close()

	return &Client{conn: conn, client: pb.NewSPFServiceClient(conn)}, nil
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// SPF finds the shortest path between wikipedia article from and to.
func (c *Client) SPF(ctx context.Context, from, to string) ([]string, error) {
	resp, err := c.client.Search(ctx, &pb.SPFRequest{From: from, To: to})
	if err != nil {
		return nil, err
	}
	return resp.Path, nil
}
