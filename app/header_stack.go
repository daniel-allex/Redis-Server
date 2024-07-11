package main

import (
	"fmt"
	"strconv"
	"strings"
)

type HeaderStack struct {
	stack []RESPListHeader
}

func NewHeaderStack() *HeaderStack {
	return &HeaderStack{stack: []RESPListHeader{}}
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
		removed := Min(headerStack.Last().Remaining, n)
		n -= removed
		headerStack.Last().Remaining -= removed

		res = append(res, headerStack.RemoveEmpty()...)
	}

	return res
}

func (headerStack *HeaderStack) RemoveEmpty() []RESPListHeader {
	var res []RESPListHeader
	for !headerStack.Empty() && headerStack.Last().Remaining == 0 {
		res = append(res, headerStack.Pop())
	}

	return res
}

func (headerStack *HeaderStack) Last() *RESPListHeader {
	return &headerStack.stack[headerStack.Size()-1]
}

func (headerStack *HeaderStack) AddHeaderFromToken(token string) error {
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
	val := headerStack.Last()
	headerStack.stack = headerStack.stack[:headerStack.Size()-1]
	return *val
}

func (headerStack *HeaderStack) Size() int {
	return len(headerStack.stack)
}

func (headerStack *HeaderStack) Empty() bool {
	return headerStack.Size() == 0
}

func (headerStack *HeaderStack) ToString() string {
	return headerListToString(headerStack.stack)
}

func headerListToString(headers []RESPListHeader) string {
	res := make([]string, len(headers))

	for i, elem := range headers {
		res[i] = elem.ToString()
	}

	return fmt.Sprintf("{%s}", strings.Join(res, ", "))
}
