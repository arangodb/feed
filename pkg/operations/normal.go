package operations

import (
	"github.com/arangodb/feed/pkg/client"
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/database"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/go-driver"

	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
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
	DocConfig   datagen.DocumentConfig
	Parallelism int64
	StartDelay  int64
	BatchSize   int64
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
		DocConfig: datagen.DocumentConfig{
			SizePerDoc:   128,
			Size:         16 * 1024 * 1024 * 1024,
			WithGeo:      false,
			WithWords:    0,
			KeySize:      32,
			NumberFields: 3,
		},
		Parallelism: 16,
		StartDelay:  5,
		BatchSize:   1000,
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
			case "size":
				e := CheckInt64Parameter(&np.DocConfig.Size, "size",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "documentSize":
				e := CheckInt64Parameter(&np.DocConfig.SizePerDoc, "documentSize",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "keySize":
				e := CheckInt64Parameter(&np.DocConfig.KeySize, "keySize",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "numberFields":
				e := CheckInt64Parameter(&np.DocConfig.NumberFields, "numberFields",
					strings.TrimSpace(pair[1]))
				if e != nil {
					return nil, e
				}
			case "withWords":
				e := CheckInt64Parameter(&np.DocConfig.WithWords, "withWords",
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
				np.DocConfig.WithGeo = x == "true" || x == "TRUE" || x == "True" ||
					x == "1" || x == "yes" || x == "Yes" || x == "YES"
				// All other cases are ignored intentionally!
			}
		} else {
			return nil, fmt.Errorf("Found argument with more than one = sign: %s", s)
		}
	}
	return np, nil
}

func (np *NormalProg) Create(cl driver.Client) error {
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
	return nil
}

func (np *NormalProg) Insert(cl driver.Client) error {
	if np.DocConfig.KeySize < 1 || np.DocConfig.KeySize > 64 {
		np.DocConfig.KeySize = 64
	}

	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	// Number of batches to put into the collection:
	number := (np.DocConfig.Size / np.DocConfig.SizePerDoc) / np.BatchSize

	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will write %d batches of %d docs across %d goroutines...\n\n", number, np.BatchSize, np.Parallelism)
	config.OutputMutex.Unlock()

	if err := writeSomeBatchesParallel(np.Parallelism, number, np.StartDelay, np.BatchSize, np.Collection, &np.DocConfig, db); err != nil {
		return fmt.Errorf("can not do some batch imports")
	}

	return nil
}

// writeSomeBatchesParallel does some batch imports in parallel
func writeSomeBatchesParallel(parallelism int64, number int64, startDelay int64, batchSize int64, collectionName string, docConfig *datagen.DocumentConfig, db driver.Database) error {
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	for i := 0; i <= int(parallelism)-1; i++ {
		time.Sleep(time.Duration(startDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Starting go routine...\n")
				config.OutputMutex.Unlock()
			}
			err := writeSomeBatches(number/parallelism, int64(i), batchSize, collectionName, docConfig, db)
			if err != nil {
				fmt.Printf("writeSomeBatches error: %v\n", err)
				haveError = true
			}
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Go routine %d done\n", i)
				config.OutputMutex.Unlock()
			}
		}(&wg, i)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	batchesPerSec := float64(number) / (float64(totaltime) / float64(time.Second))
	docspersec := float64(number*batchSize) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nnormal: Total number of documents written: %d,\n  total time: %v,\n  total batches per second: %f,\n  total docs per second: %f\n\n", number*batchSize, totaltimeend.Sub(totaltimestart), batchesPerSec, docspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in writeSomeBatches.\n")
	return fmt.Errorf("Error in writeSomeBatches.")
}

// writeSomeBatches writes `nrBatches` batches with `batchSize` documents.
func writeSomeBatches(nrBatches int64, id int64, batchSize int64, collectionName string, docConfig *datagen.DocumentConfig, db driver.Database) error {
	edges, err := db.Collection(nil, collectionName)
	if err != nil {
		fmt.Printf("writeSomeBatches: could not open `%s` collection: %v\n", collectionName, err)
		return err
	}

	docs := make([]datagen.Doc, 0, batchSize)
	times := make([]time.Duration, 0, batchSize)
	cyclestart := time.Now()
	last100start := cyclestart

	source := rand.New(rand.NewSource(int64(id) + rand.Int63()))

	for i := int64(1); i <= nrBatches; i++ {
		start := time.Now()
		for j := int64(1); j <= batchSize; j++ {
			var doc datagen.Doc
			doc.ShaKey((id*nrBatches+i-1)*batchSize+j-1, int(docConfig.KeySize))
			doc.FillData(docConfig, source)
			docs = append(docs, doc)
		}
		ctx, cancel := context.WithTimeout(driver.WithOverwriteMode(context.Background(), driver.OverwriteModeIgnore), time.Hour)
		_, _, err := edges.CreateDocuments(ctx, docs)
		cancel()
		if err != nil {
			fmt.Printf("writeSomeBatches: could not write batch: %v\n", err)
			return err
		}
		docs = docs[0:0]
		times = append(times, time.Now().Sub(start))
		if i%100 == 0 {
			dur := float64(time.Now().Sub(last100start)) / float64(time.Second)

			// Intermediate report:
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: %s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur)
				config.OutputMutex.Unlock()
			}
		}
	}

	totaltime := time.Now().Sub(cyclestart)
	nrDocs := batchSize * nrBatches
	docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))

	WriteStatisticsForTimes(times,
		fmt.Sprintf("\nnormal: Times for inserting %d batches.\n  docs per second in this go routine: %f", nrBatches, docspersec))

	return nil
}

// Execute executes a program of type NormalProg, depending on which
// subcommand is chosen in the arguments.
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
		return np.Create(cl)
	case "insert":
		return np.Insert(cl)
	}
	return nil
}
