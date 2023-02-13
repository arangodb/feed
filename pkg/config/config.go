package config

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/arangodb/feed/pkg/client"
	"github.com/arangodb/go-driver"
)

var (
	Endpoints   []string
	MetricsPort int
	Verbose     bool
	Jwt         string
	Username    string
	Password    string
	ProgName    string
	OutputMutex sync.Mutex
	Protocol    string
	JSONOutput  string
)

// MakeClient produces a client with connection to the database.
func MakeClient() (driver.Client, error) {
	var cl driver.Client
	var err error
	var endpoints []string = make([]string, 0, len(Endpoints))
	for _, e := range Endpoints {
		endpoints = append(endpoints, e)
	}
	rand.Shuffle(len(endpoints), func(i int, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
	endpoints = endpoints[0:1] // Restrict to the first
	if Jwt != "" {
		cl, err = client.NewClient(endpoints, driver.RawAuthentication(Jwt), Protocol)
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(Username, Password), Protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("Could not connect to database at %v: %v\n", Endpoints, err)
	}
	return cl, nil
}

func MakeStatsOutput(source []string, text []string) []string {
	res := make([]string, 0, len(source)+len(text)+1)
	res = append(res, "Source lines:\n")
	res = append(res, source...)
	for _, l := range source {
		res = append(res, l+"\n")
	}
	res = append(res, "\n")
	res = append(res, text...)
	return res
}
