package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type RedisConnection struct {
	Conn      *RESPConnection
	Server    *RedisServer
	Processed chan int
}

func NewRedisConnection(conn *RESPConnection, server *RedisServer) *RedisConnection {
	return &RedisConnection{Conn: conn, Server: server, Processed: make(chan int)}
}

func isWriteCommand(parseInfo ParseInfo) bool {
	return parseInfo.Command == "SET"
}

func isAcknowledgementRequest(parseInfo ParseInfo) bool {
	if parseInfo.Command == "REPLCONF" && len(parseInfo.Args) > 1 {
		arg, ok := parseInfo.Args[0].Value.(string)
		return ok && arg == "GETACK"
	}

	return false
}

func isAcknowledgementResponse(parseInfo ParseInfo) bool {
	if parseInfo.Command == "REPLCONF" && len(parseInfo.Args) > 1 {
		arg, ok := parseInfo.Args[0].Value.(string)
		return ok && arg == "ACK"
	}

	return false
}

func (rc *RedisConnection) HandleRequests(ctx context.Context) error {
	for {
		resp, err := rc.Conn.NextRESP(ctx)
		if err != nil {
			return err
		}

		parseInfo, err := rc.Conn.GetArgs(resp)
		if err != nil {
			return err
		}

		if isWriteCommand(parseInfo) {
			rc.Server.ServerInfo.Replication.Replicants.Propogate(resp)
			rc.Server.ProcessBytes(resp)
		}

		responses := rc.ResponseFromArgs(ctx, parseInfo)
		err = rc.Conn.RespondRESPValues(responses)
		if err != nil {
			return err
		}
	}
}

func (rc *RedisConnection) responsePING(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: SimpleString, Value: "PONG"}}
}

func (rc *RedisConnection) responseECHO(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: BulkString, Value: parseInfo.Args[0].Value.(string)}}
}

func (rc *RedisConnection) responseGET(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	key := parseInfo.Args[0].Value.(string)
	return []RESPValue{rc.Server.GetValue(key)}
}

func (rc *RedisConnection) responseSET(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	key := parseInfo.Args[0].Value.(string)
	value := parseInfo.Args[1]
	expiry := -1

	if len(parseInfo.Args) >= 4 &&
		strings.ToUpper(parseInfo.Args[2].Value.(string)) == "PX" {
		expiryStr := parseInfo.Args[3].Value.(string)

		exp, err := strconv.Atoi(expiryStr)
		if err != nil {
			return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: fmt.Sprintf("failed to parse expiry: %v", err)}}}
		}

		expiry = exp
	}

	rc.Server.SetValue(key, value, expiry)
	return []RESPValue{{Type: SimpleString, Value: "OK"}}
}

func (rc *RedisConnection) responseINFO(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	category := parseInfo.Args[0].Value.(string)

	switch category {
	case "replication":
		return []RESPValue{{Type: BulkString, Value: rc.Server.ServerInfo.Replication.ToString()}}
	}

	return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to specify a valid info error"}}}
}

func getBytesProcessed(parseInfo ParseInfo) (int, error) {
	processed, ok := parseInfo.Args[1].Value.(string)
	if !ok {
		return 0, fmt.Errorf("failed to get bytes processed: processed not a string")
	}

	asInt, err := strconv.Atoi(processed)
	if err != nil {
		return 0, fmt.Errorf("failed to get bytes processed: %v", err)
	}

	return asInt, nil
}

func (rc *RedisConnection) responseREPLCONF(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	if isAcknowledgementRequest(parseInfo) {
		bytesProcessed := strconv.Itoa(rc.Server.ServerInfo.Replication.MasterReplOffset)
		res := []RESPValue{{Type: BulkString, Value: "REPLCONF"}, {Type: BulkString, Value: "ACK"}, {Type: BulkString, Value: bytesProcessed}}
		return []RESPValue{{Type: Array, Value: res}}
	} else if isAcknowledgementResponse(parseInfo) {
		bytes, _ := getBytesProcessed(parseInfo)
		rc.Processed <- bytes
		return []RESPValue{}
	}

	return []RESPValue{{Type: SimpleString, Value: "OK"}}
}

func emptyRDBRESP() RESPValue {
	rdbFileHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	decoded, err := hex.DecodeString(rdbFileHex)
	if err != nil {
		return RESPValue{Type: SimpleError, Value: RESPError{Error: "ERR", Message: fmt.Sprintf("failed to decode RDB File hex: %v", err)}}
	}
	return RESPValue{Type: RDBFile, Value: string(decoded)}
}

func (rc *RedisConnection) responsePSYNC(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	fullResync := RESPValue{Type: SimpleString, Value: fmt.Sprintf("FULLRESYNC %s 0", ReplicationID)}
	emptyRDB := emptyRDBRESP()

	rc.Server.ServerInfo.Replication.Replicants.Add(&ReplicantConnection{conn: rc})

	return []RESPValue{fullResync, emptyRDB}
}

func (rc *RedisConnection) responseWAIT(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	replicants, err := strconv.Atoi(parseInfo.Args[0].Value.(string))
	if err != nil {
		return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: fmt.Sprintf("number of replicants for WAIT command could not be converted to an int: %v", err)}}}
	}

	timeout, err := strconv.Atoi(parseInfo.Args[1].Value.(string))
	if err != nil {
		return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: fmt.Sprintf("deadline for WAIT command could not be converted to an int: %v", err)}}}
	}
	processedThresh := rc.Server.ServerInfo.Replication.MasterReplOffset
	consistent := rc.Server.ServerInfo.Replication.Replicants.WaitForConsistency(ctx, replicants, time.Millisecond*time.Duration(timeout), processedThresh)
	return []RESPValue{{Type: Integer, Value: consistent}}
}

func (rc *RedisConnection) responseCONFIG(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	action, ok := parseInfo.Args[0].Value.(string)
	if !ok {
		return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to convert CONFIG arg 0 to string"}}}
	}

	if action != "GET" {
		return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "CONFIG arg must be GET"}}}
	}

	arg, ok := parseInfo.Args[1].Value.(string)
	if !ok {
		return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to convert CONFIG arg 1 to string"}}}
	}

	val := ""
	if arg == "dir" {
		val = rc.Server.ServerInfo.Persistence.Dir
	} else if arg == "dbfilename" {
		val = rc.Server.ServerInfo.Persistence.Dbfilename
	} else {
		return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "Invalid CONFIG arg 2"}}}
	}

	res := RESPValue{Type: Array, Value: []RESPValue{{Type: BulkString, Value: arg}, {Type: BulkString, Value: val}}}

	return []RESPValue{res}
}

func typeFromVal(val RESPValue) string {
	switch val.Type {
	case NullBulkString:
		return "none"
	case Stream:
		return "stream"
	default:
		return "string"
	}
}

func (rc *RedisConnection) responseTYPE(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	key := parseInfo.Args[0].Value.(string)
	val := rc.Server.GetValue(key)
	return []RESPValue{{Type: SimpleString, Value: typeFromVal(val)}}
}

func (rc *RedisConnection) responseXADD(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	streamName := parseInfo.Args[0].Value.(string)
	id := parseInfo.Args[1].Value.(string)
	fields := []Pair{}

	i := 2
	for i+1 < len(parseInfo.Args) {
		argName := parseInfo.Args[i].Value.(string)
		argVal := parseInfo.Args[i+1].Value.(string)
		fields = append(fields, Pair{Key: argName, Val: argVal})
		i += 2
	}

	streamEntry := StreamEntry{Id: id, Fields: fields}

	entries := []StreamEntry{}
	existing := rc.Server.GetValue(streamName)
	if existing.Type == Stream {
		entries = existing.Value.(StreamLog).Entries
	}

	entries = append(entries, streamEntry)
	rc.Server.SetValue(streamName, RESPValue{Type: Stream, Value: StreamLog{Name: streamName, Entries: entries}}, -1)

	return []RESPValue{{Type: SimpleString, Value: id}}
}

func (rc *RedisConnection) ResponseFromArgs(ctx context.Context, parseInfo ParseInfo) []RESPValue {
	switch parseInfo.Command {
	case "PING":
		return rc.responsePING(ctx, parseInfo)
	case "ECHO":
		return rc.responseECHO(ctx, parseInfo)
	case "GET":
		return rc.responseGET(ctx, parseInfo)
	case "SET":
		return rc.responseSET(ctx, parseInfo)
	case "INFO":
		return rc.responseINFO(ctx, parseInfo)
	case "REPLCONF":
		return rc.responseREPLCONF(ctx, parseInfo)
	case "PSYNC":
		return rc.responsePSYNC(ctx, parseInfo)
	case "WAIT":
		return rc.responseWAIT(ctx, parseInfo)
	case "CONFIG":
		return rc.responseCONFIG(ctx, parseInfo)
	case "TYPE":
		return rc.responseTYPE(ctx, parseInfo)
	case "XADD":
		return rc.responseXADD(ctx, parseInfo)
	}

	return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "command not found"}}}
}

func (rc *RedisConnection) Close() error {
	return rc.Conn.Close()
}
