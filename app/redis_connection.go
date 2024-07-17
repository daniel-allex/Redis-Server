package main

import (
	"fmt"
	"strconv"
	"strings"
)

type RedisConnection struct {
	Conn   *TCPConnection
	Server *RedisServer
}

func NewRedisConnection(conn *TCPConnection, server *RedisServer) *RedisConnection {
	return &RedisConnection{Conn: conn, Server: server}
}

func (rc *RedisConnection) respondRESP(value RESPValue) error {
	message, err := value.ToString()
	if err != nil {
		return fmt.Errorf("error responding to connection: %v", err)
	}

	err = rc.Conn.Write(message)
	if err != nil {
		return fmt.Errorf("error responding to connection: %v", err)
	}

	return nil
}

func (rc *RedisConnection) nextRESP() (RESPValue, error) {
	input, err := rc.Conn.Read()
	if err != nil {
		return RESPValue{}, err
	}

	parseInfo, err := rc.Server.Parse(input)
	if err != nil {
		return RESPValue{}, err
	}

	return parseInfo, nil
}

func (rc *RedisConnection) nextString() (string, error) {
	val, err := rc.nextRESP()
	if err != nil {
		return "", err
	}

	return val.ToString()
}

func (rc *RedisConnection) nextArgs() (ParseInfo, error) {
	val, err := rc.nextRESP()
	if err != nil {
		return ParseInfo{}, err
	}

	return rc.Server.GetArgs(val)
}

func (rc *RedisConnection) HandleRequests() error {
	for {
		parseInfo, err := rc.nextArgs()
		if err != nil {
			return err
		}

		response := rc.Server.ResponseFromArgs(parseInfo)
		err = rc.respondRESP(response)
		if err != nil {
			return err
		}
	}
}

func (rc *RedisConnection) handshakeStage(request RESPValue) (RESPValue, error) {
	err := rc.respondRESP(request)
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to run handshake: %v", err)
	}

	response, err := rc.nextRESP()
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

	args := strings.Split(val.Value.(string), " ")
	masterReplid := args[1]
	masterReplOffset, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}

	rc.Server.ServerInfo.Replication.MasterReplid = masterReplid
	rc.Server.ServerInfo.Replication.MasterReplOffset = masterReplOffset

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
