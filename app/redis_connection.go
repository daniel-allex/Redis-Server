package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

type RedisConnection struct {
	Conn   *RESPConnection
	Server *RedisServer
}

func NewRedisConnection(conn *RESPConnection, server *RedisServer) *RedisConnection {
	return &RedisConnection{Conn: conn, Server: server}
}

func createHandleReplicantFunc(resp RESPValue) ConnectionPredicate {
	return func(replicant *RedisConnection) bool {
		err := replicant.Conn.RespondRESP(resp)
		return err == nil
	}
}

func (rc *RedisConnection) updateReplicants(resp RESPValue) {
	handleReplicant := createHandleReplicantFunc(resp)
	rc.Server.ServerInfo.Replication.Replicants.Filter(handleReplicant)
}

func isWriteCommand(parseInfo ParseInfo) bool {
	return parseInfo.Command == "SET"
}

func isAcknowledgement(parseInfo ParseInfo) bool {
	if parseInfo.Command == "REPLCONF" && len(parseInfo.Args) > 0 {
		arg, ok := parseInfo.Args[0].Value.(string)
		return ok && arg == "GETACK"
	}

	return false
}

func (rc *RedisConnection) HandleRequests() error {
	for {
		resp, err := rc.Conn.NextRESP()
		if err != nil {
			return err
		}

		parseInfo, err := rc.Conn.GetArgs(resp)
		if err != nil {
			return err
		}

		if isWriteCommand(parseInfo) {
			rc.updateReplicants(resp)
		}

		responses := rc.ResponseFromArgs(parseInfo)
		err = rc.Conn.RespondRESPValues(responses)
		if err != nil {
			return err
		}
	}
}

func (rc *RedisConnection) HandleMaster() error {
	for {
		parseInfo, err := rc.Conn.NextArgs()
		if err != nil {
			return err
		}

		fmt.Printf("HandleMaster: %s\n", parseInfo.Command)

		vals := rc.ResponseFromArgs(parseInfo)
		if isAcknowledgement(parseInfo) {
			err := rc.Conn.RespondRESPValues(vals)
			if err != nil {
				return err
			}
		}
	}
}

func (rc *RedisConnection) responsePING(parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: SimpleString, Value: "PONG"}}
}

func (rc *RedisConnection) responseECHO(parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: BulkString, Value: parseInfo.Args[0].Value.(string)}}
}

func (rc *RedisConnection) responseGET(parseInfo ParseInfo) []RESPValue {
	key := parseInfo.Args[0].Value.(string)
	return []RESPValue{rc.Server.GetValue(key)}
}

func (rc *RedisConnection) responseSET(parseInfo ParseInfo) []RESPValue {
	key := parseInfo.Args[0].Value.(string)
	value := parseInfo.Args[1]
	expiry := -1

	if len(parseInfo.Args) >= 4 &&
		strings.ToUpper(parseInfo.Args[2].Value.(string)) == "PX" {
		expiryStr := parseInfo.Args[3].Value.(string)

		exp, err := strconv.Atoi(expiryStr)
		if err != nil {
			return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to parse expiry"}}}
		}

		expiry = exp
	}

	rc.Server.SetValue(key, value, expiry)
	return []RESPValue{{Type: SimpleString, Value: "OK"}}
}

func (rc *RedisConnection) responseINFO(parseInfo ParseInfo) []RESPValue {
	category := parseInfo.Args[0].Value.(string)

	switch category {
	case "replication":
		return []RESPValue{{Type: BulkString, Value: rc.Server.ServerInfo.Replication.ToString()}}
	}

	return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to specify a valid info error"}}}
}

func (rc *RedisConnection) responseREPLCONF(parseInfo ParseInfo) []RESPValue {
	if isAcknowledgement(parseInfo) {
		res := []RESPValue{{Type: BulkString, Value: "REPLCONF"}, {Type: BulkString, Value: "ACK"}, {Type: BulkString, Value: "0"}}
		return []RESPValue{{Type: Array, Value: res}}
	}

	return []RESPValue{{Type: SimpleString, Value: "OK"}}
}

func emptyRDBRESP() RESPValue {
	rdbFileHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	decoded, err := hex.DecodeString(rdbFileHex)
	if err != nil {
		return RESPValue{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to decode RDB file hex"}}
	}
	return RESPValue{Type: RDBFile, Value: string(decoded)}
}

func (rc *RedisConnection) responsePSYNC(parseInfo ParseInfo) []RESPValue {
	fullResync := RESPValue{Type: SimpleString, Value: fmt.Sprintf("FULLRESYNC %s 0", ReplicationID)}
	emptyRDB := emptyRDBRESP()

	rc.Server.ServerInfo.Replication.Replicants.Add(rc)

	return []RESPValue{fullResync, emptyRDB}
}

func (rc *RedisConnection) ResponseFromArgs(parseInfo ParseInfo) []RESPValue {
	switch parseInfo.Command {
	case "PING":
		return rc.responsePING(parseInfo)
	case "ECHO":
		return rc.responseECHO(parseInfo)
	case "GET":
		return rc.responseGET(parseInfo)
	case "SET":
		return rc.responseSET(parseInfo)
	case "INFO":
		return rc.responseINFO(parseInfo)
	case "REPLCONF":
		return rc.responseREPLCONF(parseInfo)
	case "PSYNC":
		return rc.responsePSYNC(parseInfo)
	}

	return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "command not found"}}}
}

func (rc *RedisConnection) handshakeStage(request RESPValue) (RESPValue, error) {
	err := rc.Conn.RespondRESP(request)
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to run handshake: %v", err)
	}

	response, err := rc.Conn.NextRESP()
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to run handshake: %v", err)
	}

	return response, nil
}

type stage struct {
	request  RESPValue
	expected string
}

func (rc *RedisConnection) verifyResponses(stages []stage) error {
	for i, stage := range stages {
		val, err := rc.handshakeStage(stage.request)
		if err != nil {
			return fmt.Errorf("failed to handshake stage %d: %v", i, err)
		}
		if val.Value.(string) != stage.expected {
			return fmt.Errorf("failed to handshake stage %d. Expected %s, received %s: %v", i, stage.expected, val.Value.(string), err)
		}
	}

	return nil
}

func (rc *RedisConnection) handshakePING() error {
	request := RESPValue{Array, []RESPValue{{BulkString, "PING"}}}
	return rc.verifyResponses([]stage{{request: request, expected: "PONG"}})
}

func (rc *RedisConnection) handshakeREPLCONF() error {
	request1 := RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "listening-port"}, {BulkString, rc.Server.ServerInfo.Replication.Port}}}
	request2 := RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "capa"}, {BulkString, "psync2"}}}

	return rc.verifyResponses([]stage{{request: request1, expected: "OK"}, {request: request2, expected: "OK"}})
}

func (rc *RedisConnection) handshakePSYNC() error {
	request := RESPValue{Array, []RESPValue{{BulkString, "PSYNC"}, {BulkString, "?"}, {BulkString, "-1"}}}
	val, err := rc.handshakeStage(request)
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

	fmt.Printf("masterReplid: %s | offset: %d\n", masterReplid, offset)

	rc.Server.ServerInfo.Replication.MasterReplid = masterReplid
	rc.Server.ServerInfo.Replication.MasterReplOffset = offset

	// handle RDB file
	rc.Conn.NextRESP()

	return nil
}

func (rc *RedisConnection) Handshake() error {
	err := rc.handshakePING()
	if err != nil {
		return err
	}

	err = rc.handshakeREPLCONF()
	if err != nil {
		return err
	}

	err = rc.handshakePSYNC()
	if err != nil {
		return err
	}

	return nil
}

func (rc *RedisConnection) Close() error {
	return rc.Conn.Close()
}
