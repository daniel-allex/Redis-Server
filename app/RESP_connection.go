package main

import (
	"context"
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

func (rc *RESPConnection) NextRESP(ctx context.Context) (RESPValue, error) {
	resp, err := rc.parser.ParseNext()
	if err != nil && err != io.EOF {
		return RESPValue{}, err
	}

	if err == io.EOF {
		input, err := rc.conn.Read(ctx)
		if err != nil {
			return RESPValue{}, err
		}

		resp, err = rc.parser.ParseInput(input)
		if err != nil {
			return RESPValue{}, err
		}
	}

	return resp, nil
}

func (rc *RESPConnection) NextArgs(ctx context.Context) (ParseInfo, error) {
	val, err := rc.NextRESP(ctx)
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
