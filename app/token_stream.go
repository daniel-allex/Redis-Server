package main

const EOFToken = "___EOF___"

type TokenStream struct {
	tokens []string
	index  int
}

func NewTokenStream(tokens []string) *TokenStream {
	return &TokenStream{tokens: tokens, index: 0}
}

func (ts *TokenStream) isTerminated(i int) bool {
	return i >= len(ts.tokens)
}

func (ts *TokenStream) peek(i int) string {
	if ts.isTerminated(ts.index + i) {
		return EOFToken
	}

	return ts.tokens[ts.index+i]
}

func (ts *TokenStream) Curr() string {
	return ts.peek(0)
}

func (ts *TokenStream) Peek() string {
	return ts.peek(1)
}

func (ts *TokenStream) Advance() {
	if !ts.isTerminated(ts.index) {
		ts.index += 1
	}
}

func (ts *TokenStream) NextInput(tokens []string) {
	ts.index = 0
	ts.tokens = tokens
}
