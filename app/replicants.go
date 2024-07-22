package main

import (
	"context"
	"sync"
	"time"
)

type Replicants struct {
	connections []*ReplicantConnection
	lock        sync.Mutex
}

func NewReplicants() Replicants {
	return Replicants{connections: []*ReplicantConnection{}, lock: sync.Mutex{}}
}

func (r *Replicants) Add(c *ReplicantConnection) {
	r.lock.Lock()
	r.connections = append(r.connections, c)
	r.lock.Unlock()
}

func (r *Replicants) Propogate(resp RESPValue) {
	r.lock.Lock()

	rem := []*ReplicantConnection{}
	for _, replicant := range r.connections {
		err := replicant.conn.Conn.RespondRESP(resp)
		if err != nil {
			continue
		}

		rem = append(rem, replicant)
	}

	r.connections = rem
	r.lock.Unlock()
}

func (r *Replicants) cloneConnections() []*ReplicantConnection {
	r.lock.Lock()
	connections := make([]*ReplicantConnection, len(r.connections))
	copy(connections, r.connections)
	r.lock.Unlock()

	return connections
}

func (r *Replicants) WaitForConsistency(ctx context.Context, replicantsNeeded int, timeout time.Duration, bytesNeeded int) int {
	if timeout.Milliseconds() > 0 {
		c, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ctx = c
	}

	connections := r.cloneConnections()
	done := make(chan bool, len(connections))

	synced := 0
	for _, replicant := range connections {
		if replicant.ProcessedThresh(bytesNeeded) {
			synced += 1
		} else {
			go replicant.WaitUntilConsistent(ctx, done, bytesNeeded)
		}
	}

	for synced < replicantsNeeded {
		select {
		case <-ctx.Done():
			return synced
		case <-done:
			synced += 1
		}
	}

	return synced
}

func (r *Replicants) Size() int {
	return len(r.connections)
}
