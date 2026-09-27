package main

import (
	"fmt"

	"github.com/deepanker/deepstore/internal/store"
)

func main() {
	sm := store.NewStateMachine()
	sm.Apply(store.Command{Type: store.CommandTypeSet, Key: "key1", Value: "value1"})
	sm.Apply(store.Command{Type: store.CommandTypeSet, Key: "key2", Value: "value2"})
	sm.Apply(store.Command{Type: store.CommandTypeSet, Key: "key3", Value: "value3"})
	value, ok := sm.Get("key1")
	fmt.Println(value, ok)
	sm.Apply(store.Command{Type: store.CommandTypeDelete, Key: "key2"})
	value, ok = sm.Get("key2")
	fmt.Println(value, ok)
	sm.Apply(store.Command{Type: store.CommandTypeClear})
	value, ok = sm.Get("key3")
	fmt.Println(value, ok)
}
