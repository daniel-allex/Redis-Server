package main

import (
	"context"
	"fmt"
	"io"
	"sync"
)

type Clients struct {
	connections []*RedisConnection
	lock        sync.Mutex
}

func (c *Clients) Add(conn *RedisConnection) {
	c.lock.Lock()
	c.connections = append(c.connections, conn)
	c.lock.Unlock()
}

func handleClient(ctx context.Context, conn *RedisConnection) {
	err := conn.HandleRequests(ctx)
	if err != nil && err != io.EOF {
		fmt.Printf("failed to handle client requests: %v\n", err)
		conn.Close()
	}
}

func (c *Clients) HandleAll(ctx context.Context) {
	c.lock.Lock()

	for _, client := range c.connections {
		go handleClient(ctx, client)
	}

	c.connections = []*RedisConnection{}

	c.lock.Unlock()
}
