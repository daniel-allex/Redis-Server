package main

import (
	"context"
)

type ReplicantConnection struct {
	conn *RedisConnection
}

func (rc *ReplicantConnection) ProcessedThresh(thresh int) bool {
	maxProcessed := 0
	for len(rc.conn.Processed) > 0 {
		processed := <-rc.conn.Processed
		maxProcessed = Max(maxProcessed, processed)
	}

	return maxProcessed >= thresh
}

func (rc *ReplicantConnection) sendAcknowledgement() error {
	err := rc.conn.Conn.RespondRESP(RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "GETACK"}, {BulkString, "*"}}})
	if err != nil {
		return err
	}

	return nil
}

func (rc *ReplicantConnection) WaitUntilConsistent(ctx context.Context, done chan bool, processedThresh int) {
	for {
		err := rc.sendAcknowledgement()
		if err != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case processed := <-rc.conn.Processed:
			if processed >= processedThresh {
				done <- true
				return
			}
		}
	}
}

func (rc *ReplicantConnection) Close() error {
	return rc.conn.Close()
}
