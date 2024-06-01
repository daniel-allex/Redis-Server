package main

var database map[string]RESPValue

func getValue(key string) RESPValue {
	val, ok := database[key]

	if !ok {
		return RESPValue{Type: NullBulkString, Value: nil}
	}

	return val
}

func setValue(key string, val RESPValue) {
	database[key] = val
}
