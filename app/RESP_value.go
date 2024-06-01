package main

import (
	"errors"
	"fmt"
	"strings"
)

type RESPValue struct {
	Type  RESPType
	Value interface{}
}

type RESPError struct {
	Error   string
	Message string
}

type RESPListHeader struct {
	Type      RESPType
	Size      int
	Remaining int
}

type RESPType int

const (
	RawString RESPType = iota
	SimpleString
	SimpleError
	Integer
	BulkString
	Array
	Null
	NullBulkString
)

func RESPValuesToStrings(list []RESPValue) ([]string, error) {
	res := make([]string, len(list))

	for i, elem := range list {
		str, err := elem.ToString()
		if err != nil {
			return []string{}, err
		}

		res[i] = str
	}

	return res, nil
}

func (rv *RESPValue) ToString() (string, error) {
	switch rv.Type {
	case RawString:
		return rv.Value.(string), nil
	case SimpleString:
		return fmt.Sprintf("+%s\r\n", rv.Value.(string)), nil
	case BulkString:
		val := rv.Value.(string)
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val), nil
	case Integer:
		return fmt.Sprintf(":%d\r\n", rv.Value.(int)), nil
	case Array:
		val := rv.Value.([]RESPValue)
		strVals, err := RESPValuesToStrings(val)
		if err != nil {
			return "", err
		}

		str := strings.Join(strVals, "\r\n")
		return fmt.Sprintf("*%d\r\n%s", len(val), str), nil
	case Null:
		return "_\r\n", nil
	case NullBulkString:
		return "$-1\r\n", nil
	case SimpleError:
		val := rv.Value.(RESPError)
		return fmt.Sprintf("-%s %s\r\n", val.Error, val.Message), nil
	default:
		return "", errors.New(fmt.Sprintf("failed to convert RESP to string: unknown type %d for value %s", rv.Type, rv.Value.(string)))
	}
}

func RESPFromString(str string) (RESPValue, error) {
	tokens := strings.Split(str, "\r\n")
	var valStack *ValueStack
	var headerStack *HeaderStack

	for _, token := range tokens {
		removed, err := valStack.ProcessToken(token)
		if err != nil {
			return RESPValue{}, err
		}

		headers := headerStack.Decrement(removed)
		valStack.ProcessHeaders(headers)

		err = headerStack.ProcessToken(token)
		if err != nil {
			return RESPValue{}, err
		}
	}

	return valStack.Pop(), nil
}
