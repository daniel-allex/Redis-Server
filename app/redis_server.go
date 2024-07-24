package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type RedisServer struct {
	Database         *Database
	ServerInfo       ServerInfo
	connectionBuffer Clients
}

func createServerInfo(port string, replicaOf string, dir string, dbfilename string) ServerInfo {
	role := "master"
	masterPort := port
	if replicaOf != "" {
		role = "slave"
		_, masterPort, _ = strings.Cut(replicaOf, " ")
	}

	persistenceInfo := PersistenceInfo{Dir: dir, Dbfilename: dbfilename}
	replicationInfo := ReplicationInfo{Role: role, Port: port, MasterPort: masterPort, MasterReplid: ReplicationID, MasterReplOffset: 0}
	return ServerInfo{Persistence: persistenceInfo, Replication: replicationInfo}
}

func (rs *RedisServer) handleMaster(ctx context.Context, masterConn *MasterConnection) {
	defer masterConn.Close()

	err := masterConn.HandleMaster(ctx)
	if err != nil && err != io.EOF {
		fmt.Printf("failed to handle master requests: %v\n", err)
	}
}

func (rs *RedisServer) handshake(ctx context.Context) error {
	if rs.ServerInfo.Replication.Role == "slave" {
		masterTCP, err := DialTCPConnection(":" + rs.ServerInfo.Replication.MasterPort)
		if err != nil {
			return fmt.Errorf("error dialing connection: %v", err)
		}

		master := &MasterConnection{NewRedisConnection(NewRESPConnection(masterTCP), rs)}
		err = master.Handshake(ctx)
		if err != nil {
			return fmt.Errorf("failed to run handshake: %v", err)
		}

		go rs.handleMaster(ctx, master)
	}

	return nil
}

func NewRedisServer(port string, replicaOf string, dir string, dbfilename string) (*RedisServer, error) {
	rs := &RedisServer{Database: NewDatabase(), ServerInfo: createServerInfo(port, replicaOf, dir, dbfilename), connectionBuffer: Clients{}}
	return rs, nil
}

func (rs *RedisServer) Run(ctx context.Context) error {
	listener, err := rs.listen()
	if err != nil {
		return err
	}

	go rs.takeConnections(listener)

	err = rs.handshake(ctx)
	if err != nil {
		return err
	}

	rs.handleClients(ctx)

	return nil
}

func (rs *RedisServer) handleClients(ctx context.Context) {
	for {
		rs.connectionBuffer.HandleAll(ctx)
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

func GetBytes(resp RESPValue) (int, error) {
	str, err := resp.ToString()
	if err != nil {
		return 0, err
	}

	return len([]byte(str)), nil
}

func (rs *RedisServer) ProcessBytes(resp RESPValue) error {
	bytes, err := GetBytes(resp)
	if err != nil {
		return err
	}

	rs.ServerInfo.Replication.MasterReplOffset += bytes

	return nil
}
