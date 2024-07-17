package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

const ReplicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

func handleClient(conn *RedisConnection) {
	defer conn.Close()
	err := conn.HandleRequests()
	if err != nil && err != io.EOF {
		fmt.Printf("failed to handle client requests: %v\n", err)
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := flag.String("port", "6379", "port for redis server to use")
	replicaOf := flag.String("replicaof", "", "port that this server is a replica of")
	flag.Parse()

	rs, err := NewRedisServer(*port, *replicaOf)
	if err != nil {
		fmt.Printf("failed to create redis server: %v\n", err)
		os.Exit(1)
	}

	listener, err := rs.Listen()
	if err != nil {
		fmt.Println("failed to bind to port " + *port)
		os.Exit(1)
	}

	for {
		conn, err := AcceptTCPConnection(listener)
		if err != nil {
			fmt.Printf("error accepting connection: %v\n", err)
			os.Exit(1)
		}

		redisConn := NewRedisConnection(conn, rs)
		go handleClient(redisConn)
	}

}
