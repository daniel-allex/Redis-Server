package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type RedisServer struct {
	Database   *Database
	ServerInfo ServerInfo
	Parser     *Parser
}

func createServerInfo(port string, replicaOf string) ServerInfo {
	role := "master"
	masterPort := port
	if replicaOf != "" {
		role = "slave"
		_, masterPort, _ = strings.Cut(replicaOf, " ")
	}

	replicationInfo := ReplicationInfo{Role: role, Port: port, MasterPort: masterPort, MasterReplid: ReplicationID, MasterReplOffset: 0}
	return ServerInfo{Replication: replicationInfo}
}

func (rs *RedisServer) handshake() error {
	if rs.ServerInfo.Replication.Role == "slave" {
		masterTCP, err := DialTCPConnection(":" + rs.ServerInfo.Replication.MasterPort)
		if err != nil {
			return fmt.Errorf("error dialing connection: %v", err)
		}

		masterRedisConn := NewRedisConnection(masterTCP, rs)
		err = masterRedisConn.Handshake()
		if err != nil {
			return fmt.Errorf("failed to run handshake: %v", err)
		}
	}

	return nil
}

func NewRedisServer(port string, replicaOf string) (*RedisServer, error) {
	rs := &RedisServer{Database: NewDatabase(), ServerInfo: createServerInfo(port, replicaOf), Parser: NewParser()}
	err := rs.handshake()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (rs *RedisServer) Listen() (net.Listener, error) {
	return net.Listen("tcp", "0.0.0.0:"+rs.ServerInfo.Replication.Port)
}

func (rs *RedisServer) GetValue(key string) RESPValue {
	return rs.Database.GetValue(key)
}

func (rs *RedisServer) SetValue(key string, value RESPValue, expiry int) {
	rs.Database.SetValue(key, value, expiry)
}

func (rs *RedisServer) Parse(input string) (RESPValue, error) {
	return rs.Parser.Parse(input)
}

func (rs *RedisServer) GetArgs(arr RESPValue) (ParseInfo, error) {
	return rs.Parser.GetArgs(arr)
}

func (rs *RedisServer) responsePING(parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: SimpleString, Value: "PONG"}}
}

func (rs *RedisServer) responseECHO(parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: BulkString, Value: parseInfo.Args[0].Value.(string)}}
}

func (rs *RedisServer) responseGET(parseInfo ParseInfo) []RESPValue {
	key := parseInfo.Args[0].Value.(string)
	return []RESPValue{rs.GetValue(key)}
}

func (rs *RedisServer) responseSET(parseInfo ParseInfo) []RESPValue {
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

	rs.SetValue(key, value, expiry)
	return []RESPValue{{Type: SimpleString, Value: "OK"}}
}

func (rs *RedisServer) responseINFO(parseInfo ParseInfo) []RESPValue {
	category := parseInfo.Args[0].Value.(string)

	switch category {
	case "replication":
		return []RESPValue{{Type: BulkString, Value: rs.ServerInfo.Replication.ToString()}}
	}

	return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "info error", Message: "failed to specify a valid info error"}}}
}

func (rs *RedisServer) responseREPLCONF(parseInfo ParseInfo) []RESPValue {
	return []RESPValue{{Type: SimpleString, Value: "OK"}}
}

func emptyRDBRESP() RESPValue {
	rdbFileHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	decoded, err := hex.DecodeString(rdbFileHex)
	if err != nil {
		return RESPValue{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "failed to decode RDB file hex"}}
	}
	decodedStr := string(decoded)
	return RESPValue{Type: RawString, Value: fmt.Sprintf("$%d\r\n%s", len(decodedStr), string(decodedStr))}
}

func (rs *RedisServer) responsePSYNC(parseInfo ParseInfo) []RESPValue {
	fullResync := RESPValue{Type: SimpleString, Value: fmt.Sprintf("FULLRESYNC %s 0", ReplicationID)}
	emptyRDB := emptyRDBRESP()

	return []RESPValue{fullResync, emptyRDB}
}

func (rs *RedisServer) ResponseFromArgs(parseInfo ParseInfo) []RESPValue {
	switch parseInfo.Command {
	case "PING":
		return rs.responsePING(parseInfo)
	case "ECHO":
		return rs.responseECHO(parseInfo)
	case "GET":
		return rs.responseGET(parseInfo)
	case "SET":
		return rs.responseSET(parseInfo)
	case "INFO":
		return rs.responseINFO(parseInfo)
	case "REPLCONF":
		return rs.responseREPLCONF(parseInfo)
	case "PSYNC":
		return rs.responsePSYNC(parseInfo)
	}

	return []RESPValue{{Type: SimpleError, Value: RESPError{Error: "ERR", Message: "command not found"}}}
}
