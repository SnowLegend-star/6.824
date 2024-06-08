package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"6.5840/mr"
)

// Map function
func Map(filename string, contents string) []mr.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(contents, ff)
	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
		fmt.Println(kva)
	}
	return kva
}

// Reduce function
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

// Example usage of Map and Reduce functions
func main() {
	// Simulate reading a file
	filename := "example.txt"
	contents := "apple apple bee cat dog bee"

	// Call the Map function
	intermediate := Map(filename, contents) //
	fmt.Println("Intermediate results:", intermediate)

	// Simulate grouping by key for the Reduce function
	reduceInput := make(map[string][]string)
	for _, kv := range intermediate {
		reduceInput[kv.Key] = append(reduceInput[kv.Key], kv.Value)
	}

	// Call the Reduce function for each key
	for key, values := range reduceInput {
		result := Reduce(key, values)
		fmt.Printf("Reduce result for key %s: %s\n", key, result)
	}
}
