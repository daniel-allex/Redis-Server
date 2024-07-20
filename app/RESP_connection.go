package main

import (
	"fmt"
	"io"
)

type RESPConnection struct {
	conn   *TCPConnection
	parser *Parser
}

func NewRESPConnection(conn *TCPConnection) *RESPConnection {
	return &RESPConnection{conn: conn, parser: NewParser()}
}

func (rc *RESPConnection) NextRESP() (RESPValue, error) {
	resp, err := rc.parser.ParseNext()
	if err != nil && err != io.EOF {
		return RESPValue{}, err
	}

	if err == io.EOF {
		input, err := rc.conn.Read()
		if err != nil {
			return RESPValue{}, err
		}

		fmt.Printf("reading next input: %s\n", input)

		resp, err = rc.parser.ParseInput(input)
		if err != nil {
			return RESPValue{}, err
		}
	}

	asStr, _ := resp.ToString()
	fmt.Printf("processing: %s\n", asStr)

	return resp, nil
}

func (rc *RESPConnection) NextArgs() (ParseInfo, error) {
	val, err := rc.NextRESP()
	if err != nil {
		return ParseInfo{}, err
	}

	return rc.GetArgs(val)
}

func (rc *RESPConnection) GetArgs(val RESPValue) (ParseInfo, error) {
	return rc.parser.GetArgs(val)
}

func (rc *RESPConnection) RespondRESP(value RESPValue) error {
	message, err := value.ToString()
	if err != nil {
		return fmt.Errorf("error responding to connection: %v", err)
	}

	err = rc.conn.Write(message)
	if err != nil {
		return fmt.Errorf("error responding to connection: %v", err)
	}

	return nil
}

func (rc *RESPConnection) RespondRESPValues(responses []RESPValue) error {
	for _, r := range responses {
		err := rc.RespondRESP(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rc *RESPConnection) Close() error {
	return rc.conn.Close()
}
