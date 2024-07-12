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

func responseFromArgs(parseInfo ParseInfo, database *Database) RESPValue {
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
	}

	err := RESPError{Error: "ERR", Message: "command not found"}
	return RESPValue{Type: SimpleError, Value: err}
}

func handleClient(conn *TCPConnection, database *Database) {
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

		response := responseFromArgs(parseInfo, database)
		respondRESP(conn, response)
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := flag.String("port", "6379", "port for redis server to use")
	flag.Parse()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("failed to bind to port " + *port)
		os.Exit(1)
	}

	database := NewDatabase()
	for {
		conn, err := AcceptTCPConnection(listener)
		if err != nil {
			fmt.Printf("error accepting connection: %v\n", err)
			os.Exit(1)
		}

		go handleClient(conn, database)
	}

}
