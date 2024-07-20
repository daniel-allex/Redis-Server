package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type RedisServer struct {
	Database         *Database
	ServerInfo       ServerInfo
	connectionBuffer ConnectionList
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

		masterRedisConn := NewRedisConnection(NewRESPConnection(masterTCP), rs)
		err = masterRedisConn.Handshake()
		if err != nil {
			return fmt.Errorf("failed to run handshake: %v", err)
		}

		go rs.handleMaster(masterRedisConn)
	}

	return nil
}

func NewRedisServer(port string, replicaOf string) (*RedisServer, error) {
	rs := &RedisServer{Database: NewDatabase(), ServerInfo: createServerInfo(port, replicaOf), connectionBuffer: ConnectionList{}}
	return rs, nil
}

func (rs *RedisServer) Run() error {
	listener, err := rs.listen()
	if err != nil {
		return err
	}

	go rs.takeConnections(listener)

	err = rs.handshake()
	if err != nil {
		return err
	}

	rs.handleClients()

	return nil
}

func handleClient(conn *RedisConnection) {
	defer conn.Close()
	err := conn.HandleRequests()
	if err != nil && err != io.EOF {
		fmt.Printf("failed to handle client requests: %v\n", err)
	}
}

func spawnClientHandler(conn *RedisConnection) bool {
	go handleClient(conn)

	return false
}

func (rs *RedisServer) handleClients() {
	for {
		rs.connectionBuffer.Filter(spawnClientHandler)
	}
}

func (rs *RedisServer) takeConnections(listener net.Listener) {
	for {
		conn, err := AcceptTCPConnection(listener)
		if err != nil {
			fmt.Printf("error accepting connection: %v\n", err)
			os.Exit(1)
		}

		rs.connectionBuffer.Add(NewRedisConnection(NewRESPConnection(conn), rs))
	}
}

func (rs *RedisServer) listen() (net.Listener, error) {
	return net.Listen("tcp", "0.0.0.0:"+rs.ServerInfo.Replication.Port)
}

func (rs *RedisServer) GetValue(key string) RESPValue {
	return rs.Database.GetValue(key)
}

func (rs *RedisServer) SetValue(key string, value RESPValue, expiry int) {
	rs.Database.SetValue(key, value, expiry)
}
