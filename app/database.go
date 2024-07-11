package main

import (
	"fmt"
	"sync"
	"time"
)

type ResultData struct {
	Value  RESPValue
	Expiry time.Time
}

type Database struct {
	data map[string]ResultData
	lock sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{data: map[string]ResultData{}, lock: sync.RWMutex{}}
}

func (database *Database) readerAcquire() {
	database.lock.RLock()
}

func (database *Database) readerRelease() {
	database.lock.RUnlock()
}

func (database *Database) writerAcquire() {
	database.lock.Lock()
}

func (database *Database) writerRelease() {
	database.lock.Unlock()
}

func (database *Database) GetValue(key string) RESPValue {
	database.deleteIfExpired(key)

	database.readerAcquire()
	val, ok := database.data[key]
	database.readerRelease()

	if !ok {
		return RESPValue{Type: NullBulkString, Value: nil}
	}

	return val.Value
}

func (database *Database) deleteIfExpired(key string) {
	val, ok := database.data[key]

	if ok {
		empty := time.Time{}
		if val.Expiry != empty && time.Now().After(val.Expiry) {
			fmt.Printf("expired\n")
			database.deleteKey(key)
		}
	}
}

func (database *Database) deleteKey(key string) {
	database.writerAcquire()
	delete(database.data, key)
	database.writerRelease()
}

func (database *Database) SetValue(key string, val RESPValue, expiry int) {
	database.writerAcquire()
	timeStamp := time.Time{}
	if expiry != -1 {
		timeStamp = time.Now().Add(time.Millisecond * time.Duration(expiry))
	}
	database.data[key] = ResultData{Value: val, Expiry: timeStamp}
	database.writerRelease()
}
