package main

import (
	"fmt"
	"io"
	"net"
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

func (rs *RedisServer) handleMaster(masterConn *RedisConnection) {
	defer masterConn.Close()

	err := masterConn.HandleMaster()
	if err != nil && err != io.EOF {
		fmt.Printf("failed to handle master requests: %v\n", err)
	}
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

		go rs.handleMaster(masterRedisConn)
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
