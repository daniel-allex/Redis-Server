package main

import (
	"fmt"
	"strconv"
	"strings"
)

type ValueStack struct {
	stack []RESPValue
}

func NewValueStack() *ValueStack {
	return &ValueStack{stack: []RESPValue{}}
}

func RESPFromToken(token string) (RESPValue, error) {
	switch token[0] {
	case '+':
		return RESPValue{Type: SimpleString, Value: token[1:]}, nil
	case '$':
		if token[1:] == "-1" {
			return RESPValue{Type: NullBulkString, Value: ""}, nil
		} else {
			return RESPValue{Type: Invalid}, nil
		}
	case ':':
		val, err := strconv.Atoi(token[1:])
		if err != nil {
			return RESPValue{}, fmt.Errorf("failed to get RESP Integer from token %s: %v", token, err)
		}

		return RESPValue{Type: Integer, Value: val}, nil
	case '_':
		return RESPValue{Type: Null, Value: nil}, nil
	case '-':
		err, message, _ := strings.Cut(token[1:], " ")
		val := RESPError{Error: err, Message: message}
		return RESPValue{Type: SimpleError, Value: val}, nil
	case '*':
		return RESPValue{Type: Invalid}, nil
	default:
		return RESPValue{Type: RawString, Value: token}, nil
	}
}

func (valueStack *ValueStack) Push(val RESPValue) {
	valueStack.stack = append(valueStack.stack, val)
}

func (valueStack *ValueStack) Pop() RESPValue {
	val := valueStack.stack[valueStack.Size()-1]
	valueStack.PopN(1)
	return val
}

func (valueStack *ValueStack) PopN(n int) {
	valueStack.stack = valueStack.stack[:valueStack.Size()-n]
}

func (valueStack *ValueStack) Size() int {
	return len(valueStack.stack)
}

func (valueStack *ValueStack) groupToRESP(arrType RESPType, arr []RESPValue) RESPValue {
	switch arrType {
	case Array:
		return RESPValue{Type: Array, Value: arr}
	case BulkString:
		return RESPValue{Type: BulkString, Value: arr[0].Value.(string)}
	default:
		return RESPValue{Type: Invalid}
	}
}

func (valueStack *ValueStack) MergeN(n int, arrType RESPType) {
	vals := valueStack.stack[valueStack.Size()-n:]
	grouped := make([]RESPValue, len(vals))
	copy(grouped, vals)

	valueStack.PopN(n)
	val := valueStack.groupToRESP(arrType, grouped)

	if val.Type != Invalid {
		valueStack.Push(val)
	}
}

func (valueStack *ValueStack) MergeValuesFromHeaders(headers []RESPListHeader) {
	for _, header := range headers {
		size := header.Size
		if header.Type == BulkString {
			size = 1
		}
		valueStack.MergeN(size, header.Type)
	}
}

func (valueStack *ValueStack) AddValueFromToken(token string) (int, error) {
	val, err := RESPFromToken(token)
	if err != nil {
		return 0, err
	}

	if val.Type != Invalid {
		valueStack.Push(val)

		added := 1
		if val.Type == RawString {
			added += len(val.Value.(string))
		}

		return added, nil
	}

	return 0, nil
}

func (valueStack *ValueStack) ToString() string {
	strs, _ := RESPValuesToStrings(valueStack.stack)
	return fmt.Sprintf("{%s}", strings.Join(strs, ", "))
}
