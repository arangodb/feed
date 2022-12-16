package main

import (
	"fmt"

	"bufio"
	"net/http"
	"os"
	"strconv"

	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/operations"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var (
	cmd = &cobra.Command{
		Short: "The 'feed' tool feeds ArangoDB with generated data, quickly.",
		RunE:  mainExecute,
	}
	ProgName string
)

func init() {
	flags := cmd.PersistentFlags()
	flags.StringSliceVar(&config.Endpoints, "endpoints", []string{"http://localhost:8529"}, "Endpoint of server where data should be written.")
	flags.BoolVarP(&config.Verbose, "verbose", "v", false, "Verbose output")
	flags.StringVar(&config.Jwt, "jwt", "", "Verbose output")
	flags.StringVar(&config.Username, "username", "root", "User name for database access.")
	flags.StringVar(&config.Password, "password", "", "Password for database access.")
	flags.StringVar(&ProgName, "execute", "doit.feed", "Filename of program to execute.")
	flags.StringVar(&config.Protocol, "protocol", "vst", "Protocol (http1, http2, vst)")
	flags.IntVar(&config.MetricsPort, "metricsPort", 8888, "Metrics port (0 for no metrics)")
}

func mainExecute(cmd *cobra.Command, _ []string) error {

	fmt.Printf("Hello world, this is 'feed'!\n\n")

	// Expose metrics:
	if config.MetricsPort != 0 {
		fmt.Printf("Exposing Prometheus metrics on port %d under /metrics...\n\n",
			config.MetricsPort)
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(":"+strconv.Itoa(config.MetricsPort), nil)
		}()
	}

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

	operations.Init() // set up operations for the parser

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
