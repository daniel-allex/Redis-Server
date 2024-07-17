package main

import (
	"fmt"
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

func (rc *RedisConnection) handshakeStage(request RESPValue, expected string) error {
	err := rc.respondRESP(request)
	if err != nil {
		return fmt.Errorf("failed to run handshake: %v", err)
	}

	response, err := rc.nextString()
	if err != nil {
		return fmt.Errorf("failed to run handshake: %v", err)
	}
	if response != expected {
		return fmt.Errorf("expected response %s, got %s", expected, response)
	}

	return nil
}

func (rc *RedisConnection) Handshake() error {
	request1 := RESPValue{Array, []RESPValue{{BulkString, "PING"}}}
	request2 := RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "listening-port"}, {BulkString, rc.Server.ServerInfo.Replication.Port}}}
	request3 := RESPValue{Array, []RESPValue{{BulkString, "REPLCONF"}, {BulkString, "capa"}, {BulkString, "psync2"}}}

	var stages = []struct {
		request  RESPValue
		expected string
	}{
		{request1, "+PONG\r\n"},
		{request2, "+OK\r\n"},
		{request3, "+OK\r\n"},
	}

	for i, stage := range stages {
		err := rc.handshakeStage(stage.request, stage.expected)
		if err != nil {
			return fmt.Errorf("failed to handshake stage %d: %v", i, err)
		}
	}

	return nil
}

func (rc *RedisConnection) Close() error {
	return rc.Conn.Close()
}
