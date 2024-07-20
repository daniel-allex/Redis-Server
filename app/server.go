package main

import (
	"flag"
	"fmt"
	"os"
)

const ReplicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

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

	err = rs.Run()
	if err != nil {
		fmt.Printf("failed to create redis server: %v\n", err)
		os.Exit(1)
	}
}
