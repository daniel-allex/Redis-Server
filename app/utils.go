package main

import "strings"

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func WriteLine(sb *strings.Builder, line string) {
	sb.WriteString(line + "\n")
}
