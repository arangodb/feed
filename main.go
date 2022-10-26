package main

import (
	"fmt"

	"bufio"
	"os"
	"sync"

	"github.com/neunhoef/feed/pkg/feedlang"
	"github.com/spf13/cobra"
)

var (
	cmd = &cobra.Command{
		Short: "The 'feed' tool feeds ArangoDB with generated data, quickly.",
		RunE:  mainExecute,
	}
	Endpoints   []string
	Verbose     bool
	Jwt         string
	Username    string
	Password    string
	ProgName    string
	OutputMutex sync.Mutex
)

func init() {
	flags := cmd.PersistentFlags()
	flags.StringSliceVar(&Endpoints, "endpoint", []string{"http://localhost:8529"}, "Endpoint of server where data should be written.")
	flags.BoolVarP(&Verbose, "verbose", "v", false, "Verbose output")
	flags.StringVar(&Jwt, "jwt", "", "Verbose output")
	flags.StringVar(&Username, "username", "root", "User name for database access.")
	flags.StringVar(&Password, "password", "", "Password for database access.")
	flags.StringVar(&ProgName, "execute", "prog.feed", "Filename of program to execute.")
}

func mainExecute(cmd *cobra.Command, _ []string) error {
	fmt.Printf("Hello world, this is 'feed'!\n")

	// sample.Doit(Endpoints, Jwt, Username, Password)

	inputLines := make([]string, 0, 100)
	file, err := os.Open(ProgName)
	if err != nil {
		fmt.Printf("Could not open file %s to execute program.\n", ProgName)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		inputLines = append(inputLines, scanner.Text())
	}
	file.Close()

	if err := scanner.Err(); err != nil {
		fmt.Printf("Could not read file %s to execute program.\n", ProgName)
		os.Exit(2)
	}

	prog, err := feedlang.Parse(inputLines)
	if err != nil {
		fmt.Printf("Error in parse: %v\n", err)
		os.Exit(3)
	}
	err = prog.Execute()
	if err != nil {
		fmt.Printf("Error in execution: %v\n", err)
		os.Exit(4)
	}
	return nil
}

func main() {
	cmd.Execute()
}
