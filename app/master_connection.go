package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type MasterConnection struct {
	conn *RedisConnection
}

func (mc *MasterConnection) handshakeStage(ctx context.Context, request RESPValue) (RESPValue, error) {
	err := mc.conn.Conn.RespondRESP(request)
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to run handshake: %v", err)
	}

	response, err := mc.conn.Conn.NextRESP(ctx)
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to run handshake: %v", err)
	}

	return response, nil
}

type stage struct {
	request  RESPValue
	expected string
}

func (mc *MasterConnection) verifyResponses(ctx context.Context, stages []stage) error {
	for i, stage := range stages {
		val, err := mc.handshakeStage(ctx, stage.request)
		if err != nil {
			return fmt.Errorf("failed to handshake stage %d: %v", i, err)
		}
		if val.Value.(string) != stage.expected {
			return fmt.Errorf("failed to handshake stage %d. Expected %s, received %s: %v", i, stage.expected, val.Value.(string), err)
		}
	}

	return nil
}

func (mc *MasterConnection) handshakePING(ctx context.Context) error {
	request := RESPValue{Array, []RESPValue{{BulkString, "PING"}}}
	return mc.verifyResponses(ctx, []stage{{request: request, expected: "PONG"}})
}

func (mc *MasterConnection) handshakeREPLCONF(ctx context.Context) error {
	request1 := RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "listening-port"}, {BulkString, mc.conn.Server.ServerInfo.Replication.Port}}}
	request2 := RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "capa"}, {BulkString, "psync2"}}}

	return mc.verifyResponses(ctx, []stage{{request: request1, expected: "OK"}, {request: request2, expected: "OK"}})
}

func (mc *MasterConnection) handshakePSYNC(ctx context.Context) error {
	request := RESPValue{Array, []RESPValue{{BulkString, "PSYNC"}, {BulkString, "?"}, {BulkString, "-1"}}}
	val, err := mc.handshakeStage(ctx, request)
	if err != nil {
		return err
	}

	_, args, _ := strings.Cut(val.Value.(string), " ")
	masterReplid, args, _ := strings.Cut(args, " ")
	masterReplOffset, _, _ := strings.Cut(args, " ")

	offset, err := strconv.Atoi(masterReplOffset)
	if err != nil {
		return err
	}

	mc.conn.Server.ServerInfo.Replication.MasterReplid = masterReplid
	mc.conn.Server.ServerInfo.Replication.MasterReplOffset = offset

	// handle RDB file
	mc.conn.Conn.NextRESP(ctx)

	return nil
}

func (mc *MasterConnection) Handshake(ctx context.Context) error {
	err := mc.handshakePING(ctx)
	if err != nil {
		return err
	}

	err = mc.handshakeREPLCONF(ctx)
	if err != nil {
		return err
	}

	err = mc.handshakePSYNC(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (mc *MasterConnection) HandleMaster(ctx context.Context) error {
	for {
		resp, err := mc.conn.Conn.NextRESP(ctx)
		if err != nil {
			return err
		}

		parseInfo, err := mc.conn.Conn.GetArgs(resp)
		if err != nil {
			return err
		}

		vals := mc.conn.ResponseFromArgs(ctx, parseInfo)
		if isAcknowledgementRequest(parseInfo) {
			err := mc.conn.Conn.RespondRESPValues(vals)
			if err != nil {
				return err
			}
		}

		err = mc.conn.Server.ProcessBytes(resp)
		if err != nil {
			return err
		}
	}
}

func (mc *MasterConnection) Close() error {
	return mc.conn.Close()
}
