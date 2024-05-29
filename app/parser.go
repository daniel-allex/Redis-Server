package main

import (
	"strconv"
	"strings"
)

type ParseInfo struct {
	Command string
	Args    []string
}

func parse(data string) (ParseInfo, error) {
	tokens := strings.Split(data, "\r\n")

	if len(tokens) < 3 {
		return ParseInfo{}, nil
	}

	numArgs, err := strconv.Atoi(tokens[0][1:])

	if err != nil {
		return ParseInfo{}, err
	}

	command := strings.ToUpper(tokens[2])
	var args []string

	for i := 0; i < numArgs-1; i += 1 {
		args = append(args, tokens[4+2*i])
	}

	return ParseInfo{Command: command, Args: args}, nil
}
