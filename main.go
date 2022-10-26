package main

import (
	"fmt"

	"os"

	"github.com/neunhoef/feed/pkg/feedlang"
	"github.com/neunhoef/feed/pkg/sample"
	"github.com/spf13/cobra"
)

var (
	cmd = &cobra.Command{
		Short: "The 'feed' tool feeds ArangoDB with generated data, quickly.",
		RunE:  mainExecute,
	}
	Endpoints []string
	Verbose   bool
	Jwt       string
	Username  string
	Password  string
)

func init() {
	flags := cmd.PersistentFlags()
	flags.StringSliceVar(&Endpoints, "endpoint", []string{"http://localhost:8529"}, "Endpoint of server where data should be written.")
	flags.BoolVarP(&Verbose, "verbose", "v", false, "Verbose output")
	flags.StringVar(&Jwt, "jwt", "", "Verbose output")
	flags.StringVar(&Username, "username", "root", "User name for database access.")
	flags.StringVar(&Password, "password", "", "Password for database access.")
}

func mainExecute(cmd *cobra.Command, _ []string) error {
	fmt.Printf("Hello world, this is 'feed'!\n")

	sample.Doit(Endpoints, Jwt, Username, Password)

	//feedlang.Init()
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
		os.Exit(1)
	}
	err = prog.Execute()
	if err != nil {
		fmt.Printf("Error in execution: %v\n", err)
		os.Exit(2)
	}
	return nil
}

func main() {
	cmd.Execute()
}
