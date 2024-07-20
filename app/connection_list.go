package main

import (
	"sync"
)

// TODO: Make ConnectionList and use for list of replicants to easily remove replicants
type ConnectionList struct {
	connections []*RedisConnection
	lock        sync.Mutex
}

func (cl *ConnectionList) Add(c *RedisConnection) {
	cl.lock.Lock()
	cl.connections = append(cl.connections, c)
	cl.lock.Unlock()
}

type ConnectionPredicate func(cq *RedisConnection) bool

func (cl *ConnectionList) Filter(pred ConnectionPredicate) {
	cl.lock.Lock()

	rem := []*RedisConnection{}
	for _, rc := range cl.connections {
		if pred(rc) {
			rem = append(rem, rc)
		}
	}

	cl.connections = rem

	cl.lock.Unlock()
}

func (cl *ConnectionList) Size() int {
	return len(cl.connections)
}
