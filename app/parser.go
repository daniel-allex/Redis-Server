package main

import (
	"fmt"
	"strconv"
	"strings"
)

type ParseInfo struct {
	Command string
	Args    []RESPValue
}

type Parser struct {
	ts *TokenStream
}

func NewParser() *Parser {
	return &Parser{ts: NewTokenStream([]string{})}
}

func (p *Parser) parseSimpleString(token string) RESPValue {
	p.ts.Advance()
	return RESPValue{Type: SimpleString, Value: token[1:]}
}

func (p *Parser) parseBulkString(token string) RESPValue {
	if token[1:] == "-1" {
		p.ts.Advance()
		return RESPValue{Type: NullBulkString, Value: nil}
	}

	p.ts.Advance()
	str := p.ts.Curr()
	p.ts.Advance()
	return RESPValue{Type: BulkString, Value: str}
}

func (p *Parser) parseInteger(token string) (RESPValue, error) {
	num, err := strconv.Atoi(token[1:])
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to convert integer token %s into number: %v", token[1:], err)
	}

	p.ts.Advance()
	return RESPValue{Type: Integer, Value: num}, nil
}

func (p *Parser) parseArray(token string) (RESPValue, error) {
	size, err := strconv.Atoi(token[1:])
	if err != nil {
		return RESPValue{}, fmt.Errorf("failed to convert array token %s into number: %v", token[1:], err)
	}

	p.ts.Advance()

	elements := []RESPValue{}
	for i := 0; i < size; i++ {
		val, err := p.parseExpression()
		if err != nil {
			return RESPValue{}, fmt.Errorf("failed to parse array element at index %d: %v", i, err)
		}

		elements = append(elements, val)
	}

	return RESPValue{Type: Array, Value: elements}, nil
}

func (p *Parser) parseNull() RESPValue {
	p.ts.Advance()
	return RESPValue{Type: Null, Value: nil}
}

func (p *Parser) parseSimpleError(token string) RESPValue {
	p.ts.Advance()
	return RESPValue{Type: SimpleError, Value: token[1:]}
}

func (p *Parser) parseExpression() (RESPValue, error) {
	token := p.ts.Curr()

	if token == EOFToken {
		return RESPValue{}, fmt.Errorf("cannot parse EOF Token")
	}

	switch token[0] {
	case '+':
		return p.parseSimpleString(token), nil
	case '$':
		return p.parseBulkString(token), nil
	case ':':
		return p.parseInteger(token)
	case '*':
		return p.parseArray(token)
	case '_':
		return p.parseNull(), nil
	case '-':
		return p.parseSimpleError(token), nil
	default:
		return RESPValue{}, fmt.Errorf("failed to get expression from token %s", token)
	}
}

func (p *Parser) Parse(input string) (RESPValue, error) {
	p.ts.NextInput(strings.Split(input, "\r\n"))

	return p.parseExpression()
}

func (p *Parser) GetArgs(arr RESPValue) (ParseInfo, error) {
	args, ok := arr.Value.([]RESPValue)
	if !ok {
		str, err := arr.ToString()
		if err != nil {
			return ParseInfo{}, fmt.Errorf("RESPValue is not an array and could not convert to string: %v", err)
		}
		return ParseInfo{}, fmt.Errorf("RESPValue is not an array, value is %s", str)
	}

	command := strings.ToUpper(args[0].Value.(string))
	return ParseInfo{Command: command, Args: args[1:]}, nil
}
