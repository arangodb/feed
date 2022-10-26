package operations

import (
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/feed/pkg/client"
	"github.com/neunhoef/feed/pkg/config"
	"github.com/neunhoef/feed/pkg/database"
	"github.com/neunhoef/feed/pkg/feedlang"

	"fmt"
	"strconv"
	"strings"
)

type NormalProg struct {
	// General parameters:
	Database   string
	Collection string
	SubCommand string

	// Parameters for creation:
	NumberOfShards    int64
	ReplicationFactor int64
	Drop              bool

	// Parameters for batch import:
	SizePerDoc   int64
	Size         int64
	Parallelism  int64
	StartDelay   int64
	BatchSize    int64
	WithGeo      bool
	WithWords    bool
	KeySize      int64
	NumberFields int64
}

func CheckInt64Parameter(value *int64, name string, input string) error {
	i, e := strconv.ParseInt(input, 10, 64)
	if e != nil {
		fmt.Printf("Could not parse %s argument to number: %s, error: %v\n", name, input, e)
		return e
	}
	*value = i
	return nil
}

func NewNormalProg(args []string) (feedlang.Program, error) {
	// This function parses the command line args and fills the values in
	// the struct.
	// Defaults:
	var np *NormalProg = &NormalProg{
		Database:          "_system",
		Collection:        "batchimport",
		Drop:              false,
		SubCommand:        "create",
		NumberOfShards:    3,
		ReplicationFactor: 3,
		SizePerDoc:        128,
		Size:              16 * 1024 * 1024 * 1024,
		Parallelism:       16,
		StartDelay:        5,
		BatchSize:         1000,
		WithGeo:           false,
		WithWords:         false,
		KeySize:           32,
		NumberFields:      3,
	}
	for i, s := range args {
		pair := strings.Split(s, "=")
		if len(pair) == 1 {
			if i == 0 {
				np.SubCommand = strings.TrimSpace(pair[0])
				if np.SubCommand != "create" &&
					np.SubCommand != "insert" {
					return nil, fmt.Errorf("Unknown subcommand %s", np.SubCommand)
				}
			} else {
				return nil, fmt.Errorf("Found argument without = sign: %s", pair[0])
			}
		} else if len(pair) == 2 {
			switch strings.TrimSpace(pair[0]) {
			case "database":
				np.Database = strings.TrimSpace(pair[1])
			case "collection":
				np.Collection = strings.TrimSpace(pair[1])
			case "numberOfShards":
				e := CheckInt64Parameter(&np.NumberOfShards, "numberOfShards",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "replicationFactor":
				e := CheckInt64Parameter(&np.ReplicationFactor, "replicationFactor",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "parallelism":
				e := CheckInt64Parameter(&np.Parallelism, "parallelism",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "startDelay":
				e := CheckInt64Parameter(&np.StartDelay, "startDelay",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "batchSize":
				e := CheckInt64Parameter(&np.BatchSize, "batchSize",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "keySize":
				e := CheckInt64Parameter(&np.KeySize, "keySize",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "numberFields":
				e := CheckInt64Parameter(&np.NumberFields, "numberFields",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "drop":
				x := strings.TrimSpace(pair[1])
				np.Drop = x == "true" || x == "TRUE" || x == "True" ||
					x == "1" || x == "yes" || x == "Yes" || x == "YES"
			case "withGeo":
				x := strings.TrimSpace(pair[1])
				np.WithGeo = x == "true" || x == "TRUE" || x == "True" ||
					x == "1" || x == "yes" || x == "Yes" || x == "YES"
			case "withWords":
				x := strings.TrimSpace(pair[1])
				np.WithWords = x == "true" || x == "TRUE" || x == "True" ||
					x == "1" || x == "yes" || x == "Yes" || x == "YES"
				// All other cases are ignored intentionally!
			}
		} else {
			return nil, fmt.Errorf("Found argument with more than one = sign: %s", s)
		}
	}
	return np, nil
}

func (np *NormalProg) Execute() error {
	// This actually executes the NormalProg:
	var cl driver.Client
	var err error
	if config.Jwt != "" {
		cl, err = client.NewClient(config.Endpoints, driver.RawAuthentication(config.Jwt))
	} else {
		cl, err = client.NewClient(config.Endpoints, driver.BasicAuthentication(config.Username, config.Password))
	}
	if err != nil {
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	switch np.SubCommand {
	case "create":
		db, err := database.CreateOrGetDatabase(nil, cl, np.Database,
			&driver.CreateDatabaseOptions{})
		if err != nil {
			return fmt.Errorf("Could not create/open database %s: %v\n", np.Database, err)
		}
		ec, err := db.Collection(nil, np.Collection)
		if err == nil {
			if !np.Drop {
				return fmt.Errorf("Found collection %s already, setup is already done.", np.Collection)
			}
			err = ec.Remove(nil)
			if err != nil {
				return fmt.Errorf("Could not drop collection %s: %v\n", np.Collection, err)
			}
		} else if !driver.IsNotFound(err) {
			return fmt.Errorf("Error: could not look for collection %s: %v\n", np.Collection, err)
		}

		// Now create the batchimport collection:
		_, err = db.CreateCollection(nil, np.Collection, &driver.CreateCollectionOptions{
			Type:              driver.CollectionTypeDocument,
			NumberOfShards:    int(np.NumberOfShards),
			ReplicationFactor: int(np.ReplicationFactor),
		})
		if err != nil {
			return fmt.Errorf("Error: could not create collection %s: %v", np.Collection, err)
		}
		fmt.Printf("normal: Database %s and collection %s successfully created.\n", np.Database, np.Collection)
	case "insert":
	}
	return nil
}
