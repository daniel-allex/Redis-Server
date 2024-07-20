package main

import (
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
	Invalid RESPType = iota
	RawString
	SimpleString
	SimpleError
	Integer
	BulkString
	Array
	Null
	NullBulkString
	RDBFile
)

func (listHeader *RESPListHeader) ToString() string {
	return fmt.Sprintf(
		"{Type:%d, Size:%d, Remaining:%d}",
		listHeader.Type,
		listHeader.Size,
		listHeader.Remaining,
	)
}

func RESPValuesToStrings(list []RESPValue) ([]string, error) {
	res := make([]string, len(list))

	for i, _ := range list {
		str, err := list[i].ToString()
		if err != nil {
			return []string{}, err
		}

		res[i] = str
	}

	return res, nil
}

func (rv *RESPValue) arrayToString() (string, error) {
	val := rv.Value.([]RESPValue)
	strVals, err := RESPValuesToStrings(val)
	if err != nil {
		return "", err
	}

	str := strings.Join(strVals, "")
	return fmt.Sprintf("*%d\r\n%s", len(val), str), nil
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
		return rv.arrayToString()
	case Null:
		return "_\r\n", nil
	case NullBulkString:
		return "$-1\r\n", nil
	case SimpleError:
		val := rv.Value.(RESPError)
		return fmt.Sprintf("-%s %s\r\n", val.Error, val.Message), nil
	case RDBFile:
		val := rv.Value.(string)
		return fmt.Sprintf("$%d\r\n%s", len(val), val), nil
	default:
		return "", fmt.Errorf("failed to convert RESP to string: unknown type %d for value %s", rv.Type, rv.Value.(string))
	}
}
