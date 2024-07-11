package main

import (
	"strings"
)

type ParseInfo struct {
	Command string
	Args    []RESPValue
}

func parse(data string) (ParseInfo, error) {
	val, err := RESPFromString(data)
	if err != nil {
		return ParseInfo{}, err
	}
	args := val.Value.([]RESPValue)
	command := strings.ToUpper(args[0].Value.(string))
	return ParseInfo{Command: command, Args: args[1:]}, nil
}
