package main

import (
	"fmt"
	"strconv"
)

type HeaderStack struct {
	stack []RESPListHeader
}

func listHeaderFromToken(token string) (RESPListHeader, error) {
	switch token[0] {
	case '*':
		size, err := strconv.Atoi(token[1:])
		if err != nil {
			return RESPListHeader{}, fmt.Errorf("failed to get RESP array size from token %s: %v", token, err)
		}

		return RESPListHeader{Type: Array, Size: size, Remaining: size}, nil
	case '$':
		if token[1:] != "-1" {
			size, err := strconv.Atoi(token[1:])
			if err != nil {
				return RESPListHeader{}, fmt.Errorf("failed to get RESP array size from token %s: %v", token, err)
			}
			return RESPListHeader{Type: BulkString, Size: size, Remaining: size}, nil
		}
	}

	return RESPListHeader{Type: RawString, Size: 0, Remaining: 0}, nil
}

func (headerStack *HeaderStack) Decrement(n int) []RESPListHeader {
	var res []RESPListHeader
	for !headerStack.Empty() && n > 0 {
		removed := Min(headerStack.Size(), n)
		n -= removed
		headerStack.stack[headerStack.Size()-1].Remaining -= removed

		res = append(res, headerStack.RemoveEmpty()...)
	}

	return res
}

func (headerStack *HeaderStack) RemoveEmpty() []RESPListHeader {
	var res []RESPListHeader
	for headerStack.stack[headerStack.Size()-1].Remaining == 0 {
		res = append(res, headerStack.Pop())
	}

	return res
}

func (headerStack *HeaderStack) ProcessToken(token string) error {
	header, err := listHeaderFromToken(token)
	if err != nil {
		return err
	}

	if header.Type != RawString {
		headerStack.stack = append(headerStack.stack, header)
	}

	headerStack.RemoveEmpty()

	return nil
}

func (headerStack *HeaderStack) Pop() RESPListHeader {
	val := headerStack.stack[headerStack.Size()-1]
	headerStack.stack = headerStack.stack[:headerStack.Size()-1]
	return val
}

func (headerStack *HeaderStack) Size() int {
	return len(headerStack.stack)
}

func (headerStack *HeaderStack) Empty() bool {
	return headerStack.Size() == 0
}
