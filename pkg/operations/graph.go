package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/database"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/graphgen"
	"github.com/arangodb/go-driver"

	"context"
	"fmt"
	"math/rand"
	"time"
)

type GraphProg struct {
	NormalProg     // embedded
	GraphName      string
	VertexCollName string
	EdgeCollName   string
	GraphType      string
	GraphSize      int64
	OutFormat      string
}

var (
	graphSubprograms = map[string]struct{}{
		"create":         {},
		"insertVertices": {},
		"insertEdges":    {},
	}
)

func NewGraphProg(args []string) (feedlang.Program, error) {
	// This function parses the command line args and fills the values in
	// the struct.

	subCmd, m := ParseArguments(args)

	// First parse as a normal program:
	np := parseNormalArgs(subCmd, m)

	// Now parse for the graph specific stuff:
	var gp *GraphProg = &GraphProg{
		NormalProg:     *(np), // shallow copy
		GraphName:      GetStringValue(m, "name", "G"),
		VertexCollName: GetStringValue(m, "vertexColl", "V"),
		EdgeCollName:   GetStringValue(m, "edgeColl", "E"),
		GraphType:      GetStringValue(m, "type", "cyclic"),
		GraphSize:      GetInt64Value(m, "graphSize", 2000),
		OutFormat:      GetStringValue(m, "outFormat", ""),
	}

	if _, hasKey := graphSubprograms[subCmd]; !hasKey {
		return nil, fmt.Errorf("Unknown subcommand %s", gp.SubCommand)
	}
	return gp, nil
}

func (gp *GraphProg) Create() error {
	cl, err := config.MakeClient()
	if err != nil {
		return fmt.Errorf("Can not make client: %v", err)
	}
	db, err := database.CreateOrGetDatabase(nil, cl, gp.Database,
		&driver.CreateDatabaseOptions{})
	if err != nil {
		return fmt.Errorf("Could not create/open database %s: %v\n", gp.Database, err)
	}
	g, err := db.Graph(nil, gp.GraphName)
	if err == nil {
		if !gp.Drop {
			return fmt.Errorf("Found graph %s already, setup is already done.", gp.GraphName)
		}
		ctx := driver.WithDropCollections(context.Background(), true)
		err = g.Remove(ctx)
		if err != nil {
			return fmt.Errorf("Could not drop graph %s: %v\n", gp.GraphName, err)
		}
	} else if !driver.IsNotFound(err) {
		return fmt.Errorf("Error: could not look for graph %s: %v\n", gp.GraphName, err)
	}

	// Now create the graph:
	_, err = db.CreateGraphV2(nil, gp.GraphName, &driver.CreateGraphOptions{
		IsSmart:           false,
		NumberOfShards:    int(gp.NumberOfShards),
		ReplicationFactor: int(gp.ReplicationFactor),
		EdgeDefinitions: []driver.EdgeDefinition{
			{
				Collection: gp.EdgeCollName,
				From:       []string{gp.VertexCollName},
				To:         []string{gp.VertexCollName},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("Error: could not create graph %s: %v", gp.GraphName, err)
	}
	fmt.Printf("graph: Graph %s in database %s successfully created.\n", gp.GraphName, gp.Database)
	return nil
}

func (gp *GraphProg) Insert(what string) error {
	isJSON, err := ValidateOutFormat(gp.OutFormat)
	if err != nil {
		return fmt.Errorf("\ngraph: can not obtain output format, %v", err)
	}

	// Derive and check some arguments:
	if gp.DocConfig.KeySize < 1 || gp.DocConfig.KeySize > 64 {
		gp.DocConfig.KeySize = 64
	}

	var gg graphgen.GraphGenerator

	switch gp.GraphType {
	case "cyclic":
		gg, _ = (&graphgen.CycleGraphParameters{uint64(gp.GraphSize),
			graphgen.GeneralParameters{"", 0, 0}}).MakeGraphGenerator(true, true)
	default:
		return fmt.Errorf("Unknown graph type: %s", gp.GraphType)
	}

	var numberDocs int64
	var numberBatches int64
	var ch chan *datagen.Doc
	if what == "vertices" {
		// Number of batches to put into the collection:
		numberDocs = int64(gg.NumberVertices())
		numberBatches = (numberDocs + gp.BatchSize - 1) / gp.BatchSize
		ch = gg.VertexChannel()
	} else if what == "edges" {
		numberDocs = int64(gg.NumberEdges())
		numberBatches = (numberDocs + gp.BatchSize - 1) / gp.BatchSize
		ch = gg.EdgeChannel()
	} else {
		return fmt.Errorf("unknown insert task %s", what)
	}
	gp.DocConfig.SizePerDoc = gp.DocConfig.Size / numberDocs

	config.OutputMutex.Lock()
	fmt.Printf("\ngraph (%s): Will write %d batches of %d docs across %d goroutines...\n\n",
		what, numberBatches, gp.BatchSize, gp.Parallelism)
	config.OutputMutex.Unlock()

	err = database.RunParallel(gp.Parallelism, gp.StartDelay, "graph",
		func(id int64) error {
			// Let's use our own private client and connection here:
			cl, err := config.MakeClient()
			if err != nil {
				return fmt.Errorf("Can not make client: %v", err)
			}
			db, err := cl.Database(context.Background(), gp.Database)
			if err != nil {
				return fmt.Errorf("Can not get database: %s", gp.Database)
			}

			var coll driver.Collection
			if what == "vertices" {
				coll, err = db.Collection(nil, gp.VertexCollName)
			} else {
				coll, err = db.Collection(nil, gp.EdgeCollName)
			}
			if err != nil {
				config.OutputMutex.Lock()
				fmt.Printf("graph write %s: could not open `%s` collection: %v\n", what, coll.Name(), err)
				config.OutputMutex.Unlock()
				return err
			}

			docs := make([]datagen.Doc, 0, gp.BatchSize)
			times := make([]time.Duration, 0, numberBatches/gp.Parallelism)
			cyclestart := time.Now()
			last100start := cyclestart

			// It is crucial that every go routine has its own random source,
			// otherwise we create a lot of contention.
			source := rand.New(rand.NewSource(int64(id) + rand.Int63()))

			i := int64(0)
			nrDocs := int64(0)
			for done := false; !done; {
				start := time.Now()
				for j := int64(1); j <= gp.BatchSize; j++ {
					doc, ok := <-ch
					if !ok {
						done = true
						break
					}
					doc.FillData(&gp.DocConfig, source)
					docs = append(docs, *doc)
				}
				if len(docs) > 0 {
					ctx, cancel := context.WithTimeout(driver.WithOverwriteMode(context.Background(), driver.OverwriteModeIgnore), time.Hour)
					_, _, err := coll.CreateDocuments(ctx, docs)
					cancel()
					if err != nil {
						fmt.Printf("writeSomeBatches: could not write batch: %v\n", err)
						return err
					}
					nrDocs += int64(len(docs))
				}
				docs = docs[0:0]
				times = append(times, time.Now().Sub(start))
				i += 1
				if i%100 == 0 {
					dur := float64(time.Now().Sub(last100start)) / float64(time.Second)
					last100start = time.Now()

					// Intermediate report:
					if config.Verbose {
						config.OutputMutex.Lock()
						fmt.Printf("graph: %s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur)
						config.OutputMutex.Unlock()
					}
				}
			}

			totaltime := time.Now().Sub(cyclestart)
			docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))

			if isJSON {
				err = WriteStatisticsForTimes(times,
					fmt.Sprintf("{graph: {numBatches: %d,  docsPerSec: %f}}", i, docspersec), isJSON)
			} else {
				err = WriteStatisticsForTimes(times,
					fmt.Sprintf("\ngraph: Times for inserting %d batches.\n  docs per second in this go routine: %f", i, docspersec), isJSON)
			}
			if err != nil {
				return fmt.Errorf("\ngraph: can not write statistics in JSON format %v", err)
			}
			return nil
		},
		func(totaltime time.Duration, haveError bool) error {
			batchesPerSec := float64(numberBatches) / (float64(totaltime) / float64(time.Second))
			docspersec := float64(numberDocs) / (float64(totaltime) / float64(time.Second))
			var msg string
			if isJSON {
				msg = fmt.Sprintf("graph: {totalDocsWritten: %d, totalTime: %v, totalBatchesPerSec: %f, totalDocsPerSec: %f, hadErrors: %v}}",
					numberDocs, totaltime, batchesPerSec, docspersec,
					haveError)
				msg, err = PrettyPrintToJSON(msg)
				if err != nil {
					return fmt.Errorf("can not write statistics in JSON format: %v", err)
				}
			} else {
				msg = fmt.Sprintf("\ngraph: Total number of documents written: %d,\n  total time: %v,\n  total batches per second: %f,\n  total docs per second: %f\n  had errors: %v\n\n",
					numberDocs, totaltime, batchesPerSec, docspersec,
					haveError)
			}
			config.OutputMutex.Lock()
			fmt.Printf(msg)
			config.OutputMutex.Unlock()
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("can not do some batch imports for graph %s", what)
	}

	return nil
}

// Execute executes a program of type NormalProg, depending on which
// subcommand is chosen in the arguments.
func (gp *GraphProg) Execute() error {
	// This actually executes the GraphProg:
	switch gp.SubCommand {
	case "create":
		return gp.Create()
	case "insertVertices":
		return gp.Insert("vertices")
	case "insertEdges":
		return gp.Insert("edges")
	}
	return nil
}
