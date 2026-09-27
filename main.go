package main

import (
	"fmt"

	"github.com/deepanker/deepstore/internal/store"
)

func main() {
	store := store.NewStore()
	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("key3", "value3")
	fmt.Println(store.Get("key1"))
	fmt.Println(store.List())
	fmt.Println(store.Len())
	store.Delete("key2")
	fmt.Println(store.List())
	fmt.Println(store.Len())
}
