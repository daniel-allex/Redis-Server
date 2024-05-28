package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	// Uncomment this block to pass the first stage
	// "net"
	// "os"
)

func handleClient(conn *TCPConnection) {
	defer conn.Close()

	for {
		input, err := conn.Read()
		if err != nil && err != io.EOF {
			fmt.Println("Error reading input: ", err.Error())
			os.Exit(1)
		}

		if len(input) == 0 {
			return
		}

		if strings.Contains(input, "PING") {
			err = conn.Respond("PONG")

			if err != nil {
				fmt.Println("Error responding to connection: ", err.Error())
				os.Exit(1)
			}
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

	conn, err := AcceptTCPConnection(listener)
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	handleClient(conn)

}
