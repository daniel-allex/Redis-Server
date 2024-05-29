package main

import (
	"fmt"
	"io"
	"net"
	"os"
	// Uncomment this block to pass the first stage
	// "net"
	// "os"
)

func respond(conn *TCPConnection, message string) {
	err := conn.Respond(message)

	if err != nil {
		fmt.Printf("Error responding to connection: %v\n", err)
		os.Exit(1)
	}
}

func handleClient(conn *TCPConnection) {
	defer conn.Close()

	for {
		input, err := conn.Read()
		if err != nil && err != io.EOF {
			fmt.Printf("Error reading input: %v\n", err)
			os.Exit(1)
		}

		parseInfo, err := parse(input)
		if err != nil {
			fmt.Printf("Error parsing input data: %v\n", err)
			os.Exit(1)
		}

		switch parseInfo.Command {
		case "PING":
			respond(conn, "PONG")
		case "ECHO":
			respond(conn, parseInfo.Args[0])
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := AcceptTCPConnection(listener)
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			os.Exit(1)
		}

		go handleClient(conn)
	}

}
