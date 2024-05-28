package main

import "net"

func Write(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message))
	return err
}

func WriteLine(conn net.Conn, message string) error {
	err := Write(conn, message+"\n")
	return err
}

func Respond(conn net.Conn, message string) error {
	err := WriteLine(conn, "+"+message+"\r")
	return err
}
