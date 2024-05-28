package main

import (
	"bufio"
	"net"
)

type TCPConnection struct {
	conn *net.Conn
	io   *bufio.ReadWriter
}

func NewTCPConnection(conn *net.Conn) *TCPConnection {
	reader := bufio.NewReader(*conn)
	writer := bufio.NewWriter(*conn)

	readWriter := bufio.NewReadWriter(reader, writer)
	return &TCPConnection{conn: conn, io: readWriter}
}

func AcceptTCPConnection(listener net.Listener) (*TCPConnection, error) {
	conn, err := listener.Accept()
	if err != nil {
		return nil, err
	}

	return NewTCPConnection(&conn), nil
}

func (conn *TCPConnection) ReadLine() (string, error) {
	return conn.io.ReadString('\n')
}

func (conn *TCPConnection) Read() (string, error) {
	buf := make([]byte, 1024)
	n, err := conn.io.Read(buf)

	return string(buf[:n]), err
}

func (conn *TCPConnection) Write(message string) error {
	_, err := conn.io.WriteString(message)
	if err != nil {
		return err
	}

	err = conn.io.Flush()
	return err
}

func (conn *TCPConnection) WriteLine(message string) error {
	err := conn.Write(message + "\n")
	return err
}

func (conn *TCPConnection) Respond(message string) error {
	err := conn.WriteLine("+" + message + "\r")
	return err
}

func (conn *TCPConnection) Close() error {
	return (*conn.conn).Close()
}
