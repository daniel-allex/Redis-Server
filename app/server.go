package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

const ReplicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

func main() {
	fmt.Println("Redis Server Started")

	port := flag.String("port", "6379", "port for redis server to use")
	replicaOf := flag.String("replicaof", "", "port that this server is a replica of")
	dir := flag.String("dir", "./", "directory of RDB file")
	dbfilename := flag.String("dbfilename", "./", "file name of RDB file")
	flag.Parse()

	rs, err := NewRedisServer(*port, *replicaOf, *dir, *dbfilename)
	if err != nil {
		fmt.Printf("failed to create redis server: %v\n", err)
		os.Exit(1)
	}

	err = rs.Run(context.Background())
	if err != nil {
		fmt.Printf("failed to create redis server: %v\n", err)
		os.Exit(1)
	}
}
