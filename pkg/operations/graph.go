package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/database"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/graphgen"
	"github.com/arangodb/feed/pkg/metrics"
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
	GraphDepth     int64
	GraphBranching int64
	GraphDirection string
	feedlang.ProgMeta
}

func (g *GraphProg) Lines() (int, int) {
	return g.StartLine, g.EndLine
}

func (g *GraphProg) SetSource(lines []string) {
	g.Source = lines
}

func (g *GraphProg) StatsOutput() []string {
	return config.MakeStatsOutput(g.Source, []string{
		fmt.Sprintf("graph: Have run for %v (lines %d..%d of script)\n",
			g.EndTime.Sub(g.StartTime), g.StartLine, g.EndLine),
		fmt.Sprintf("       Start time: %v\n", g.StartTime),
		fmt.Sprintf("       End time  : %v\n", g.EndTime),
	})
}

func (g *GraphProg) StatsJSON() interface{} {
	g.Type = "graph"
	return g
}

var (
	graphSubprograms = map[string]struct{}{
		"create":          {},
		"insertVertices":  {},
		"insertEdges":     {},
		"randomTraversal": {},
	}
)

func NewGraphProg(args []string, line int) (feedlang.Program, error) {
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
		GraphDepth:     GetInt64Value(m, "graphDepth", 10),
		GraphBranching: GetInt64Value(m, "graphBranching", 2),
		GraphDirection: GetStringValue(m, "graphDirection", "downwards"),
		ProgMeta:       feedlang.ProgMeta{StartLine: line, EndLine: line},
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
		&driver.CreateDatabaseOptions{
			Options: driver.CreateDatabaseDefaultOptions{
				ReplicationVersion: driver.DatabaseReplicationVersion(gp.ReplicationVersion),
			},
		})
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
	// Derive and check some arguments:
	if gp.DocConfig.KeySize < 1 || gp.DocConfig.KeySize > 64 {
		gp.DocConfig.KeySize = 64
	}

	var gg graphgen.GraphGenerator

	makeVertices := what == "vertices"
	makeEdges := what == "edges"
	var prefix string
	if makeVertices {
		prefix = gp.VertexCollName + "/"
	} else {
		prefix = gp.EdgeCollName + "/"
	}

	switch gp.GraphType {
	case "cyclic":
		gg, _ = (&graphgen.CycleGraphParameters{Length: uint64(gp.GraphSize),
			KeySize: gp.DocConfig.KeySize,
			GeneralParams: graphgen.GeneralParameters{
				Prefix:             prefix,
				EdgePrefix:         gp.VertexCollName + "/",
				StartIndexVertices: 0,
				StartIndexEdges:    0}}).MakeGraphGenerator(makeVertices, makeEdges)
	case "tree":
		gg, _ = (&graphgen.Tree{
			BranchingDegree: uint64(gp.GraphBranching),
			Depth:           uint64(gp.GraphDepth),
			DirectionType:   gp.GraphDirection,
			KeySize:         gp.DocConfig.KeySize,
			GeneralParams: graphgen.GeneralParameters{
				Prefix:             "",
				EdgePrefix:         gp.VertexCollName + "/",
				StartIndexVertices: 0,
				StartIndexEdges:    0}}).MakeGraphGenerator(makeVertices, makeEdges)
	case "collatz":
		gg = &graphgen.CollatzParameters{
			Size:    uint64(gp.GraphSize),
			KeySize: gp.DocConfig.KeySize,
			GeneralParams: graphgen.GeneralParameters{
				Prefix:             prefix,
				EdgePrefix:         gp.VertexCollName + "/",
				StartIndexVertices: 0,
				StartIndexEdges:    0}}
	case "regular":
		gg = &graphgen.RegularParameters{
			Size:    uint64(gp.GraphSize),
			Degree:  uint64(gp.GraphBranching),
			KeySize: gp.DocConfig.KeySize,
			GeneralParams: graphgen.GeneralParameters{
				Prefix:             prefix,
				EdgePrefix:         gp.VertexCollName + "/",
				StartIndexVertices: 0,
				StartIndexEdges:    0}}
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

	PrintTS(fmt.Sprintf("\ngraph (%s): Will write %d batches of %d docs across %d goroutines...\n\n",
		what, numberBatches, gp.BatchSize, gp.Parallelism))

	err := RunParallel(gp.Parallelism, gp.StartDelay, "graph",
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
				PrintTS(fmt.Sprintf("graph write %s: could not open `%s` collection: %v\n", what, coll.Name(), err))
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
						PrintTS(fmt.Sprintf("writeSomeBatches: could not write batch: %v\n", err))
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
						PrintTS(fmt.Sprintf("graph: %s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur))
					}
				}
			}

			totaltime := time.Now().Sub(cyclestart)
			docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))
			stats := NormalStatsOneThread{
				TotalTime:    totaltime,
				NumberOps:    nrDocs,
				OpsPerSecond: docspersec,
			}
			stats.FillInStats(times)
			PrintStatistics(&stats, fmt.Sprintf("graph (insert):\n  Times for inserting %d batches.\n  docs per second in this go routine: %f", numberBatches, docspersec))

			// Report back:
			gp.Stats.Mutex.Lock()
			gp.Stats.Threads = append(gp.Stats.Threads, stats)
			gp.Stats.Mutex.Unlock()

			return nil
		},
		func(totaltime time.Duration, haveError bool) error {
			// Here, we aggregate the data from all threads and report for the
			// whole command:
			gp.Stats.Overall = AggregateStats(gp.Stats.Threads, totaltime)
			gp.Stats.Overall.TotalTime = totaltime
			gp.Stats.Overall.HaveError = haveError
			batchesPerSec := float64(numberBatches) / (float64(totaltime) / float64(time.Second))
			docspersec := float64(numberDocs) / (float64(totaltime) / float64(time.Second))

			msg := fmt.Sprintf("graph (insert):\n  Total number of documents written: %d,\n  total batches per second: %f,\n  total docs per second: %f,\n  with errors: %v", numberDocs, batchesPerSec, docspersec, haveError)
			statsmsg := gp.Stats.Overall.StatsToStrings()
			PrintTSs(msg, statsmsg)

			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("can not do some batch imports for graph %s", what)
	}

	return nil
}

func (gp *GraphProg) RandomTraversal() error {
	Print("\n")
	PrintTS(fmt.Sprintf("graph: Will perform random traversals.\n\n"))

	if err := runRandomTraversalInParallel(gp); err != nil {
		return fmt.Errorf("can not run random travesal: %v", err)
	}

	return nil
}

func runRandomTraversalInParallel(gp *GraphProg) error {
	err := RunParallel(gp.Parallelism, gp.StartDelay, "randomTraversal", func(id int64) error {
		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("randomTraversal: Can not make client: %v", err)
		}
		db, err := cl.Database(context.Background(), gp.Database)
		if err != nil {
			return fmt.Errorf("randomTraversal: Can not get database: %s", gp.Database)
		}
		// Get the vertex count:
		coll, err := db.Collection(nil, gp.VertexCollName)
		if err != nil {
			PrintTS(fmt.Sprintf("randomTraversal: could not open `%s` vertex collection: %v\n", gp.VertexCollName, err))
			return err
		}

		ctx := context.Background()
		colSize, err := coll.Count(ctx)
		if err != nil {
			PrintTS(fmt.Sprintf("randomTraversal: could not count num of vertices for `%s` vertex collection: %v\n", gp.VertexCollName, err))
			return err
		}

		times := make([]time.Duration, 0, gp.LoadPerThread)
		cyclestart := time.Now()

		// It is crucial that every go routine has its own random source,
		// otherwise we create a lot of contention.
		source := rand.New(rand.NewSource(int64(id) + rand.Int63()))

		for i := int64(1); i <= gp.LoadPerThread; i++ {
			start := time.Now()
			randIntId := source.Int63n(colSize)
			var doc datagen.Doc
			doc.ShaKey(randIntId, int(gp.DocConfig.KeySize))

			queryStr := fmt.Sprintf("FOR v IN 1..%d  OUTBOUND \"%s/%s\" GRAPH %s OPTIONS {\"uniqueVertices\":\"global\", \"order\":\"bfs\"} RETURN v._key", gp.GraphDepth, gp.VertexCollName, doc.Key, gp.GraphName)
			_, err = db.Query(ctx, queryStr, nil)

			if err != nil {
				return fmt.Errorf("Can not execute graph traversal: %v\n", err)
			}
			metrics.GraphTraversals.Inc()
			times = append(times, time.Now().Sub(start))
		}
		totaltime := time.Now().Sub(cyclestart)
		randomtraversalspersec := float64(gp.LoadPerThread) / (float64(totaltime) / float64(time.Second))
		stats := NormalStatsOneThread{}
		stats.TotalTime = totaltime
		stats.NumberOps = gp.LoadPerThread
		stats.OpsPerSecond = randomtraversalspersec
		stats.FillInStats(times)
		PrintStatistics(&stats, fmt.Sprintf("graph (randomTraversals):\n  Times for running %d random traversals.\n  traversals per second in this go routine: %f", gp.LoadPerThread, randomtraversalspersec))

		// Report back:
		gp.Stats.Mutex.Lock()
		gp.Stats.Threads = append(gp.Stats.Threads, stats)
		gp.Stats.Mutex.Unlock()
		return nil
	},
		func(totaltime time.Duration, haveError bool) error {
			// Here, we aggregate the data from all threads and report for the
			// whole command:
			gp.Stats.Overall = AggregateStats(gp.Stats.Threads, totaltime)
			gp.Stats.Overall.TotalTime = totaltime
			gp.Stats.Overall.HaveError = haveError
			randtraversalsspersec := float64(gp.Parallelism*gp.LoadPerThread) / (float64(totaltime) / float64(time.Second))
			msg := fmt.Sprintf("graph (randomTraverals):\n  Total number of random traversals: %d,\n  total traversals per second: %f,\n  with errors: %v", gp.Parallelism*gp.LoadPerThread, randtraversalsspersec, haveError)
			statsmsg := gp.Stats.Overall.StatsToStrings()
			PrintTSs(msg, statsmsg)
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("randomTraverals: cannot run traversals in parallel: %v", err)
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
	case "randomTraversal":
		return gp.RandomTraversal()
	}
	return nil
}
