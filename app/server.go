package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func respondRESP(conn *TCPConnection, value RESPValue) {
	message, err := value.ToString()
	if err != nil {
		fmt.Printf("error responding to connection: %v\n", err)
		os.Exit(1)
	}

	err = conn.Write(message)
	if err != nil {
		fmt.Printf("error responding to connection: %v\n", err)
		os.Exit(1)
	}
}

func responseFromArgs(parseInfo ParseInfo, database *Database, serverInfo ServerInfo) RESPValue {
	switch parseInfo.Command {
	case "PING":
		return RESPValue{Type: SimpleString, Value: "PONG"}
	case "ECHO":
		return RESPValue{Type: BulkString, Value: parseInfo.Args[0].Value.(string)}
	case "GET":
		key := parseInfo.Args[0].Value.(string)
		return database.GetValue(key)
	case "SET":
		key := parseInfo.Args[0].Value.(string)
		value := parseInfo.Args[1]
		expiry := -1

		if len(parseInfo.Args) >= 4 &&
			strings.ToUpper(parseInfo.Args[2].Value.(string)) == "PX" {
			expiryStr := parseInfo.Args[3].Value.(string)
			expiry, _ = strconv.Atoi(expiryStr)
		}
		database.SetValue(key, value, expiry)
		return RESPValue{Type: SimpleString, Value: "OK"}
	case "INFO":
		category := parseInfo.Args[0].Value.(string)

		switch category {
		case "replication":
			return RESPValue{Type: BulkString, Value: serverInfo.Replication.ToString()}
		}

		return RESPValue{Type: SimpleError, Value: RESPError{Error: "info error", Message: "failed to specify a valid info error"}}
	}

	err := RESPError{Error: "ERR", Message: "command not found"}
	return RESPValue{Type: SimpleError, Value: err}

}

func handleClient(conn *TCPConnection, database *Database, serverInfo ServerInfo) {
	defer conn.Close()

	for {
		input, err := conn.Read()
		if err != nil && err != io.EOF {
			fmt.Printf("error reading input: %v\n", err)
			os.Exit(1)
		}

		if err == io.EOF {
			return
		}

		parseInfo, err := parse(input)
		if err != nil {
			fmt.Printf("error parsing input data: %v\n", err)
			os.Exit(1)
		}

		response := responseFromArgs(parseInfo, database, serverInfo)
		respondRESP(conn, response)
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := flag.String("port", "6379", "port for redis server to use")
	replicaOf := flag.String("replicaof", "", "port that this server is a replica of")
	flag.Parse()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("failed to bind to port " + *port)
		os.Exit(1)
	}

	const ReplicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

	database := NewDatabase()
	role := "master"
	if *replicaOf != "" {
		role = "slave"
	}

	replicationInfo := ReplicationInfo{Role: role, MasterReplid: ReplicationID, MasterReplOffset: 0}
	serverInfo := ServerInfo{Replication: replicationInfo}

	if role == "slave" {
		_, masterPort, _ := strings.Cut(*replicaOf, " ")

		master, err := DialTCPConnection(":" + masterPort)
		if err != nil {
			fmt.Printf("error dialing connection: %v\n", err)
			os.Exit(1)
		}

		respondRESP(master, RESPValue{Array, []RESPValue{RESPValue{BulkString, "PING"}}})
	}

	for {
		conn, err := AcceptTCPConnection(listener)
		if err != nil {
			fmt.Printf("error accepting connection: %v\n", err)
			os.Exit(1)
		}

		go handleClient(conn, database, serverInfo)
	}

}
