package main

import (
	"fmt"

	"bufio"
	"encoding/json"
	"net/http"
	"os"
	"strconv"

	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/operations"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

const (
	Version = "0.9.1"
)

var (
	cmd = &cobra.Command{
		Short: "The 'feed' tool feeds ArangoDB with generated data, quickly.",
		RunE:  mainExecute,
	}
	ProgName  string
	MoreWords int
)

func init() {
	flags := cmd.PersistentFlags()
	flags.StringSliceVar(&config.Endpoints, "endpoints", []string{"http://localhost:8529"}, "Endpoint of server where data should be written.")
	flags.BoolVarP(&config.Verbose, "verbose", "v", false, "Verbose output")
	flags.StringVar(&config.Jwt, "jwt", "", "JWT token for database access (if provided username and password are ignored).")
	flags.StringVar(&config.Username, "username", "root", "User name for database access.")
	flags.StringVar(&config.Password, "password", "", "Password for database access.")
	flags.StringVar(&ProgName, "execute", "doit.feed", "Filename of program to execute.")
	flags.StringVar(&config.Protocol, "protocol", "vst", "Protocol (http1, http2, vst)")
	flags.StringVar(&config.JSONOutput, "jsonOutputFile", "feed.json", "Filename for JSON result output.")
	flags.IntVar(&config.MetricsPort, "metricsPort", 8888, "Metrics port (0 for no metrics)")
	flags.IntVar(&MoreWords, "moreWords", 0, "Number of additional random terms in long word list")
}

func produceJSONResult(prog feedlang.Program, fileName string) {
	var res interface{} = prog.StatsJSON()
	by, err := json.Marshal(res)
	if err != nil {
		fmt.Printf("Error in producing JSON output of results: %v, object: %v\n",
			err, res)
		return
	}
	err = os.WriteFile(fileName, by, 0644)
	if err != nil {
		fmt.Printf("Could not create JSON output file: %s, error: %v\n",
			fileName, err)
		return
	}
}

func produceResult(prog feedlang.Program) {
	var out []string = prog.StatsOutput()
	fmt.Printf("\n\n---------------\n--- RESULTS ---\n---------------\n\n")
	for i := 0; i < len(out); i += 1 {
		fmt.Print(out[i])
	}
	fmt.Printf("\n-------------------------------------------------\n")
}

func mainExecute(cmd *cobra.Command, _ []string) error {

	fmt.Printf("Hello world, this is 'feed' version %s !\n\n", Version)

	datagen.ExtendLongWordList(MoreWords)

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
	produceJSONResult(prog, config.JSONOutput)
	produceResult(prog)
	return nil
}

func main() {
	cmd.Execute()
}
