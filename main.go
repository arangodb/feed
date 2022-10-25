package main

import (
	"fmt"

	"os"

	"github.com/neunhoef/feed/pkg/feedlang"
	"github.com/neunhoef/feed/pkg/sample"
)

func main() {
	fmt.Printf("Hello world!\n")

	sample.Doit()

	feedlang.Init()
	theProggy := []string{
		"[",
		"{",
		"dummy 1",
		"dummy 2",
		"dummy 3",
		"}",
		"{",
		"dummy 4",
		"dummy 3",
		"}",
		"]",
	}
	prog, err := feedlang.Parse(theProggy)
	if err != nil {
		fmt.Printf("Error in parse: %v\n", err)
		os.Exit(17)
	}
	err = prog.Execute()
	if err != nil {
		fmt.Printf("Error in execution: %v\n", err)
		os.Exit(18)
	}
}
