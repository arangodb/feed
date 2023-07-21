package operations

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arangodb/feed/pkg/client"
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/database"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/metrics"
	"github.com/arangodb/go-driver"
)

type NormalStatsOneThread struct {
	Average                  time.Duration `json:"average"`
	Median                   time.Duration `json:"median"`
	Percentile90             time.Duration `json:"percentile90"`
	Percentile99             time.Duration `json:"percentile99"`
	Minimum                  time.Duration `json:"minimum"`
	Maximum                  time.Duration `json:"maximum"`
	OpsPerSecond             float64       `json:"opsPerSecond"`
	NumberOps                int64         `json:"numberOps"`
	TotalTime                time.Duration `json:"totalTime"`
	NumReplaceWriteConflicts int64         `json:"numberReplaceWriteConflicts"`
	NumUpdateWriteConflicts  int64         `json:"numberUpdateWriteConflicts"`
	HaveError                bool          `json:"haveError"`
}

func (ns *NormalStatsOneThread) FillInStats(times []time.Duration) {
	sort.Sort(DurationSlice(times))
	var sum int64 = 0
	for _, t := range times {
		sum = sum + int64(t)
	}
	nr := int64(len(times))

	ns.Median = times[int(float64(0.5)*float64(nr))]
	ns.Percentile90 = times[int(float64(0.9)*float64(nr))]
	ns.Percentile99 = times[int(float64(0.99)*float64(nr))]
	ns.Average = time.Duration(sum / nr)
	ns.Minimum = times[0]
	ns.Maximum = times[len(times)-1]
}

func AggregateStats(stats []NormalStatsOneThread, totalTime time.Duration) NormalStatsOneThread {
	var t NormalStatsOneThread
	len := len(stats)
	for _, s := range stats {
		t.Average += s.Average
		if t.Minimum == 0 || s.Minimum < t.Minimum {
			t.Minimum = s.Minimum
		}
		if s.Maximum > t.Maximum {
			t.Maximum = s.Maximum
		}
		t.Median += s.Median             // we take the average of the medians
		t.Percentile90 += s.Percentile90 // same
		t.Percentile99 += s.Percentile99 // same
		t.OpsPerSecond += s.OpsPerSecond
		t.NumberOps += s.NumberOps // sum here
		t.NumUpdateWriteConflicts += s.NumUpdateWriteConflicts
		t.NumReplaceWriteConflicts += s.NumReplaceWriteConflicts
	}
	t.Average /= time.Duration(len)
	t.Median /= time.Duration(len)
	t.Percentile90 /= time.Duration(len)
	t.Percentile99 /= time.Duration(len)
	t.OpsPerSecond /= float64(len)
	// We intentionally leave the TotalTime and HaveError empty, this has
	// to be supplied from the outside!
	return t
}

type NormalStats struct {
	Threads []NormalStatsOneThread `json:"threads"`
	Overall NormalStatsOneThread   `json:"overall"`
	Mutex   sync.Mutex             `json:"-"`
	feedlang.ProgMeta
}

type NormalProg struct {
	// General parameters:
	Database   string
	Collection string
	SubCommand string

	// Parameters for creation:
	NumberOfShards    int64
	ReplicationFactor int64
	OneShard          bool
	Drop              bool

	// Parameters for batch import:
	DocConfig   datagen.DocumentConfig
	Parallelism int64
	StartDelay  int64
	BatchSize   int64

	// Parameters for random:
	LoadPerThread int64

	// Parameter for query on index:
	QueryLimit int64

	// Parameter for index creation or query on index
	IdxName string

	// Timeout for individual batch operation (in seconds)
	Timeout int64

	// Number of retries if an error (or timeout) happens
	Retries int64

	// Add random _from and _to attributes for edge collections, to be compatible
	// with smart graphs, we add a random smart graph attribute if the Smart
	// flag is set.
	AddFromTo      bool
	UseAql         bool
	Smart          bool
	VertexCollName string

	WaitForSync bool

	ReplicationVersion string

	// Statistics:
	Stats NormalStats
}

type WriteConflictStats struct {
	numUpdateWriteConflicts  int64
	numReplaceWriteConflicts int64
}

func (n *NormalProg) Lines() (int, int) {
	return n.Stats.StartLine, n.Stats.EndLine
}

func (n *NormalProg) SetSource(lines []string) {
	n.Stats.Source = lines
}

func (s *NormalStatsOneThread) StatsToStrings() []string {
	return []string{
		fmt.Sprintf("  NumberOps : %d\n", s.NumberOps),
		fmt.Sprintf("  OpsPerSec : %f\n", s.OpsPerSecond),
		fmt.Sprintf("  Median    : %v\n", s.Median),
		fmt.Sprintf("  90%%ile    : %v\n", s.Percentile90),
		fmt.Sprintf("  99%%ile    : %v\n", s.Percentile99),
		fmt.Sprintf("  Average   : %v\n", s.Average),
		fmt.Sprintf("  Minimum   : %v\n", s.Minimum),
		fmt.Sprintf("  Maximum   : %v\n", s.Maximum),
	}
}

func (n *NormalProg) StatsOutput() []string {
	res := config.MakeStatsOutput(n.Stats.Source, []string{
		fmt.Sprintf("normal (%s): Have run for %v (lines %d..%d of script)\n",
			n.SubCommand, n.Stats.EndTime.Sub(n.Stats.StartTime), n.Stats.StartLine,
			n.Stats.EndLine),
		fmt.Sprintf("  Start time: %v\n", n.Stats.StartTime),
		fmt.Sprintf("  End time  : %v\n", n.Stats.EndTime),
	})
	res = append(res, n.Stats.Overall.StatsToStrings()...)
	return res
}

func (n *NormalProg) StatsJSON() interface{} {
	return &n.Stats
}

var (
	normalSubprograms = map[string]struct{}{"create": {}, "insert": {},
		"randomRead": {}, "randomUpdate": {}, "randomReplace": {},
		"createIdx": {}, "dropIdx": {}, "queryOnIdx": {}, "drop": {},
		"truncate": {}, "dropDatabase": {},
	}
	writeConflictStats WriteConflictStats
)

func parseNormalArgs(subCmd string, m map[string]string) *NormalProg {
	return &NormalProg{
		Database:          GetStringValue(m, "database", "_system"),
		Collection:        GetStringValue(m, "collection", "batchimport"),
		Drop:              GetBoolValue(m, "drop", false),
		SubCommand:        subCmd,
		NumberOfShards:    GetInt64Value(m, "numberOfShards", 3),
		ReplicationFactor: GetInt64Value(m, "replicationFactor", 3),
		OneShard:          GetBoolValue(m, "oneShard", false),
		DocConfig: datagen.DocumentConfig{
			SizePerDoc:   GetInt64Value(m, "documentSize", 128),
			Size:         GetInt64Value(m, "size", 16*1024*1024*1024),
			WithGeo:      GetBoolValue(m, "withGeo", false),
			WithWords:    GetInt64Value(m, "withWords", 0),
			KeySize:      GetInt64Value(m, "keySize", 32),
			NumberFields: GetInt64Value(m, "numberFields", 1),
		},

		Parallelism:        GetInt64Value(m, "parallelism", 16),
		StartDelay:         GetInt64Value(m, "startDelay", 5),
		BatchSize:          GetInt64Value(m, "batchSize", 1000),
		LoadPerThread:      GetInt64Value(m, "loadPerThread", 50),
		QueryLimit:         GetInt64Value(m, "queryLimit", 1),
		IdxName:            GetStringValue(m, "idxName", ""),
		Timeout:            GetInt64Value(m, "timeout", 3600),
		Retries:            GetInt64Value(m, "retries", 0),
		AddFromTo:          GetBoolValue(m, "addFromTo", false),
		UseAql:             GetBoolValue(m, "useAql", false),
		Smart:              GetBoolValue(m, "smart", false),
		WaitForSync:        GetBoolValue(m, "waitForSync", false),
		ReplicationVersion: GetStringValue(m, "replicationVersion", ""),
		VertexCollName:     GetStringValue(m, "vertexCollName", "V"),
	}
}

func NewNormalProg(args []string, line int) (feedlang.Program, error) {
	// This function parses the command line args and fills the values in
	// the struct.
	// Defaults:
	subCmd, m := ParseArguments(args)
	np := parseNormalArgs(subCmd, m)
	if _, hasKey := normalSubprograms[np.SubCommand]; !hasKey {
		return nil, fmt.Errorf("Unknown subcommand %s", np.SubCommand)
	}
	np.Stats.StartLine = line
	np.Stats.EndLine = line
	np.Stats.Type = "normal (" + subCmd + ")"
	return np, nil
}

func (np *NormalProg) Create(cl driver.Client) error {
	start := time.Now()
	dbCreationOptions := driver.CreateDatabaseDefaultOptions{
		ReplicationVersion: driver.DatabaseReplicationVersion(np.ReplicationVersion),
	}
	if np.OneShard {
		dbCreationOptions.Sharding = "single"
	}

	db, err := database.CreateOrGetDatabase(nil, cl, np.Database,
		&driver.CreateDatabaseOptions{
			Options: dbCreationOptions})
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

	collectionCreateOptions := &driver.CreateCollectionOptions{
		Type:              driver.CollectionTypeDocument,
		ReplicationFactor: int(np.ReplicationFactor),
		WaitForSync:       np.WaitForSync,
	}

	if !np.OneShard {
		collectionCreateOptions.NumberOfShards = int(np.NumberOfShards)
	}

	// Now create the batchimport collection:
	_, err = db.CreateCollection(nil, np.Collection, collectionCreateOptions)
	if err != nil {
		return fmt.Errorf("Error: could not create collection %s: %v", np.Collection, err)
	}
	PrintTS(fmt.Sprintf("normal: Database %s and collection %s successfully created.\n", np.Database, np.Collection))
	metrics.CollectionsCreated.Inc()
	totaltime := time.Now().Sub(start)

	// Report back:
	np.Stats.Mutex.Lock()
	np.Stats.Overall = SingleStats(totaltime, float64(time.Second)/float64(totaltime))
	np.Stats.Mutex.Unlock()
	return nil
}

func (np *NormalProg) DoDrop(cl driver.Client) error {
	start := time.Now()
	db, err := database.CreateOrGetDatabase(nil, cl, np.Database,
		&driver.CreateDatabaseOptions{})
	if err != nil {
		return fmt.Errorf("Could not create/open database %s: %v\n", np.Database, err)
	}
	ec, err := db.Collection(nil, np.Collection)
	if err == nil {
		err = ec.Remove(nil)
		if err != nil {
			return fmt.Errorf("Could not drop collection %s: %v\n", np.Collection, err)
		}
	} else if !driver.IsNotFound(err) {
		return fmt.Errorf("Error: could not look for collection %s: %v\n", np.Collection, err)
	}

	Print("\n")
	PrintTS(fmt.Sprintf("normal: Database %s found and collection %s successfully dropped (line %d of script).\n", np.Database, np.Collection, np.Stats.StartLine))
	metrics.CollectionsDropped.Inc()
	totaltime := time.Now().Sub(start)

	// Report back:
	np.Stats.Mutex.Lock()
	np.Stats.Overall = SingleStats(totaltime, float64(time.Second)/float64(totaltime))
	np.Stats.Mutex.Unlock()
	return nil
}

func (np *NormalProg) DropDatabase(cl driver.Client) error {
	start := time.Now()
	if np.Database == "_system" {
		return fmt.Errorf("Cannot drop _system database.")
	}
	db, err := cl.Database(nil, np.Database)
	if err != nil {
		return fmt.Errorf("Could not open database %s: %v\n", np.Database, err)
	}
	err = db.Remove(nil)
	if err != nil {
		return fmt.Errorf("Could not drop database %s: %v\n", np.Database, err)
	}

	Print("\n")
	PrintTS(fmt.Sprintf("normal: Database %s dropped successfully (line %d of script).\n", np.Database, np.Stats.StartLine))
	metrics.DatabasesDropped.Inc()
	totaltime := time.Now().Sub(start)

	// Report back:
	np.Stats.Mutex.Lock()
	np.Stats.Overall = SingleStats(totaltime, float64(time.Second)/float64(totaltime))
	np.Stats.Mutex.Unlock()
	return nil
}

func (np *NormalProg) Truncate(cl driver.Client) error {
	start := time.Now()
	db, err := database.CreateOrGetDatabase(nil, cl, np.Database,
		&driver.CreateDatabaseOptions{})
	if err != nil {
		return fmt.Errorf("Could not create/open database %s: %v\n", np.Database, err)
	}
	ec, err := db.Collection(nil, np.Collection)
	if err == nil {
		err = ec.Truncate(nil)
		if err != nil {
			return fmt.Errorf("Could not truncate collection %s: %v\n", np.Collection, err)
		}
	} else if !driver.IsNotFound(err) {
		return fmt.Errorf("Error: could not look for collection %s: %v\n", np.Collection, err)
	}

	Print("\n")
	PrintTS(fmt.Sprintf("normal: Database %s found and collection %s successfully truncated (line %d of script).\n", np.Database, np.Collection, np.Stats.StartLine))
	metrics.CollectionsTruncated.Inc()
	totaltime := time.Now().Sub(start)

	// Report back:
	np.Stats.Mutex.Lock()
	np.Stats.Overall = SingleStats(totaltime, float64(time.Second)/float64(totaltime))
	np.Stats.Mutex.Unlock()
	return nil
}

func (np *NormalProg) Insert() error {
	if np.DocConfig.KeySize < 1 || np.DocConfig.KeySize > 64 {
		np.DocConfig.KeySize = 64
	}

	// Number of batches to put into the collection:
	number := (np.DocConfig.Size / np.DocConfig.SizePerDoc) / np.BatchSize

	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will write %d batches of %d docs across %d goroutines... (line %d of script)\n", number, np.BatchSize, np.Parallelism, np.Stats.StartLine))

	if err := writeSomeBatchesParallel(np, number); err != nil {
		return fmt.Errorf("can not do some batch imports")
	}

	return nil
}

func (np *NormalProg) RunQueryOnIdx() error {
	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will perform query on idx.\n\n"))

	if err := runQueryOnIdxInParallel(np); err != nil {
		return fmt.Errorf("can not run query on idx: %v", err)
	}

	return nil
}

func (np *NormalProg) CreateIdx() error {
	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will perform index creation.\n\n"))

	if err := createIdx(np); err != nil {
		return fmt.Errorf("can not create index")
	}
	metrics.IndexesCreated.Inc()

	return nil
}

func (np *NormalProg) DropIdx() error {
	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will perform index drop.\n\n"))

	if err := dropIdx(np); err != nil {
		return fmt.Errorf("can not create drop")
	}
	metrics.IndexesDropped.Inc()

	return nil
}

func (np *NormalProg) RandomReplace() error {
	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will perform random replaces.\n\n"))

	if err := replaceRandomlyInParallel(np); err != nil {
		return fmt.Errorf("can not replace randomly")
	}

	return nil
}

func (np *NormalProg) RandomUpdate() error {
	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will perform random updates.\n\n"))

	if err := updateRandomlyInParallel(np); err != nil {
		return fmt.Errorf("can not update randomly")
	}

	return nil
}

func (np *NormalProg) RandomRead() error {
	Print("\n")
	PrintTS(fmt.Sprintf("\nnormal: Will perform random reads.\n\n"))

	if err := readRandomlyInParallel(np); err != nil {
		return fmt.Errorf("can not read randomly")
	}

	return nil
}

func runQueryOnIdxInParallel(np *NormalProg) error {
	err := RunParallel(np.Parallelism, np.StartDelay, "queryOnIdx", func(id int64) error {
		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("queryOnIdx: Can not make client: %v", err)
		}
		db, err := cl.Database(context.Background(), np.Database)
		if err != nil {
			return fmt.Errorf("queryOnIdx: Can not get database: %s", np.Database)
		}

		coll, err := db.Collection(nil, np.Collection)
		if err != nil {
			PrintTS(fmt.Sprintf("queryOnIdx: could not open `%s` collection: %v\n", np.Collection, err))
			return err
		}

		ctx := context.Background()
		idxs, err := coll.Indexes(ctx)
		// to not lookup name in array
		idxsToAttrs := make(map[string][]string)
		for _, idx := range idxs {
			idxsToAttrs[idx.UserName()] = idx.Fields()
		}

		var idxAttr string

		if strings.HasPrefix(np.IdxName, "\"") {
			np.IdxName = np.IdxName[1 : len(np.IdxName)-1]
		}

		if len(np.IdxName) == 0 {
			idxAttr = idxs[1].Fields()[0]
		} else if np.IdxName == "primary" {
			idxAttr = idxs[0].Fields()[0]
		} else {
			idxAttrs, exists := idxsToAttrs[np.IdxName]
			if !exists {
				return fmt.Errorf("query on index execution: index name " + np.IdxName + " not found")
			}
			idxAttr = idxAttrs[0]
		}

		times := make([]time.Duration, 0, np.LoadPerThread)
		cyclestart := time.Now()

		// It is crucial that every go routine has its own random source,
		// otherwise we create a lot of contention.
		source := rand.New(rand.NewSource(int64(id) + rand.Int63()))

		for i := int64(1); i <= np.LoadPerThread; i++ {
			start := time.Now()
			randLength := source.Intn(100)
			randWord := datagen.MakeRandomStringWithSpaces(randLength, source)

			queryStr := "FOR doc IN " + np.Collection + " SORT doc." + idxAttr + " DESC FILTER doc." + idxAttr + " >= \"" + randWord + "\" LIMIT " + strconv.FormatInt(np.QueryLimit, 10) + " RETURN doc"
			_, err = db.Query(ctx, queryStr, nil)

			if err != nil {
				return fmt.Errorf("Can not execute query on index %v\n", err)
			}
			metrics.DocumentsRead.Add(float64(np.QueryLimit))
			metrics.BatchesRead.Inc()
			times = append(times, time.Now().Sub(start))
		}
		totaltime := time.Now().Sub(cyclestart)
		queriesonidxpersec := float64(np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
		stats := NormalStatsOneThread{}
		stats.TotalTime = totaltime
		stats.NumberOps = np.LoadPerThread
		stats.OpsPerSecond = queriesonidxpersec
		stats.FillInStats(times)
		PrintStatistics(&stats, fmt.Sprintf("normal (queryOnIdx):\n  Times for reading %d documents.\n  queries per second in this go routine: %f", np.LoadPerThread*np.QueryLimit, queriesonidxpersec))

		// Report back:
		np.Stats.Mutex.Lock()
		np.Stats.Threads = append(np.Stats.Threads, stats)
		np.Stats.Mutex.Unlock()
		return nil
	},
		func(totaltime time.Duration, haveError bool) error {
			// Here, we aggregate the data from all threads and report for the
			// whole command:
			np.Stats.Overall = AggregateStats(np.Stats.Threads, totaltime)
			np.Stats.Overall.TotalTime = totaltime
			np.Stats.Overall.HaveError = haveError
			idxqueriespersec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
			msg := fmt.Sprintf("normal (queryOnIdx):\n  Total number of documents read: %d,\n  total queries per second: %f,\n  with errors: %v", np.Parallelism*np.LoadPerThread*np.QueryLimit, idxqueriespersec, haveError)
			statsmsg := np.Stats.Overall.StatsToStrings()
			PrintTSs(msg, statsmsg)
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("queryOnIdx: can not run random traversals in parallel: %v", err)
	}
	return nil
}

func createIdx(np *NormalProg) error {
	totaltimestart := time.Now()
	haveError := false
	if config.Verbose {
		PrintTS("normal (idxCreate): Starting idxCreation...\n")
	}
	// Let's use our own private client and connection here:
	cl, err := config.MakeClient()
	if err != nil {
		return fmt.Errorf("idxCreation: Can not make client: %v", err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("idxCreation: Can not get database: %s", np.Database)
	}

	coll, err := db.Collection(nil, np.Collection)
	if err != nil {
		PrintTS(fmt.Sprintf("idxCreation: could not open `%s` collection: %v\n", np.Collection, err))
		return err
	}

	ctx := context.Background()

	idxTypes := make([]string, 0, np.LoadPerThread)

	idxTypes = append(idxTypes, "persistent")
	if np.DocConfig.WithGeo {
		idxTypes = append(idxTypes, "geo")
	}

	idxFields := make([]string, 0, 17) // Paylaod from 1 to F + Words = 17

	for i := int64(1); i <= np.DocConfig.NumberFields; i++ {
		idxFields = append(idxFields, "payload"+fmt.Sprintf("%x", i))
	}

	var idxName string
	if strings.HasPrefix(np.IdxName, "\"") {
		np.IdxName = np.IdxName[1 : len(np.IdxName)-1]
	}
	if len(np.IdxName) > 0 {
		idxName = np.IdxName
	} else {
		idxName = "idx" + strconv.Itoa(int(rand.Uint32()))
	}
	if np.DocConfig.WithGeo {
		var options driver.EnsureGeoIndexOptions
		options.Name = idxName
		//the method didn't accept a simple array, so has to do the following to add the field as an argument
		geoPayload := make([]string, 0, 1)
		geoPayload = append(geoPayload, "geo")
		_, _, err = coll.EnsureGeoIndex(ctx, geoPayload, &options)
	} else {
		var options driver.EnsurePersistentIndexOptions
		options.Name = idxName
		_, _, err = coll.EnsurePersistentIndex(ctx, idxFields, &options)
	}

	if err != nil {
		haveError = true
	}

	if config.Verbose {
		PrintTS(fmt.Sprintf("normal: idxCreation done\n"))
	}

	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	idxspersec := 1 / (float64(totaltime) / float64(time.Second))

	// Report back:
	np.Stats.Mutex.Lock()
	np.Stats.Overall = SingleStats(totaltime, idxspersec)
	np.Stats.Mutex.Unlock()

	if !haveError {
		return nil
	}
	PrintTS(fmt.Sprintf("Error in idxCreation %v \n", err))
	return fmt.Errorf("Error in idxCreation.")
}

func dropIdx(np *NormalProg) error {
	totaltimestart := time.Now()
	haveError := false
	if config.Verbose {
		PrintTS("normal (idxDrop): Starting idx drop...\n")
	}
	// Let's use our own private client and connection here:
	cl, err := config.MakeClient()
	if err != nil {
		return fmt.Errorf("idxDrop: Can not make client: %v", err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("idxDrop: Can not get database: %s", np.Database)
	}

	coll, err := db.Collection(nil, np.Collection)
	if err != nil {
		PrintTS(fmt.Sprintf("idxDrop: could not open `%s` collection: %v\n", np.Collection, err))
		return err
	}

	var idxName string
	if strings.HasPrefix(np.IdxName, "\"") {
		np.IdxName = np.IdxName[1 : len(np.IdxName)-1]
	}
	if len(np.IdxName) > 0 {
		idxName = np.IdxName
	} else {
		PrintTS(fmt.Sprintf("idxDrop: no index name given to drop in collection %s: %v\n",
			np.Collection, err))
		return fmt.Errorf("idxDrop: no index name given")
	}

	ctx := context.Background()
	indexes, err := coll.Indexes(ctx)
	if err != nil {
		PrintTS(fmt.Sprintf("idxDrop: cannot list indexes of collection %s: %v\n", np.Collection, err))
		haveError = true
	} else {
		found := false
		for _, ind := range indexes {
			if ind.UserName() == idxName {
				found = true
				err = ind.Remove(ctx)
				if err != nil {
					haveError = true
				}
				break
			}
		}
		if !found {
			haveError = true
			PrintTS(fmt.Sprintf("idxDrop: Did not find index with name %s in collection %s in database %s",
				idxName, np.Collection, np.Database))
			err = fmt.Errorf("Did not find index with name %s in collection %s in database %s",
				idxName, np.Collection, np.Database)
		}
	}

	if config.Verbose && !haveError {
		PrintTS(fmt.Sprintf("normal: idxDrop done\n"))
	}

	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	idxspersec := 1 / (float64(totaltime) / float64(time.Second))

	// Report back:
	np.Stats.Mutex.Lock()
	np.Stats.Overall = SingleStats(totaltime, idxspersec)
	np.Stats.Mutex.Unlock()

	if !haveError {
		return nil
	}
	PrintTS(fmt.Sprintf("Error in idxCreation %v \n", err))
	return fmt.Errorf("Error in idxCreation.")
}

func replaceRandomlyInParallel(np *NormalProg) error {
	err := RunParallel(np.Parallelism, np.StartDelay, "randomReplace", func(id int64) error {
		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("randomReplace: Can not make client: %v", err)
		}
		db, err := cl.Database(context.Background(), np.Database)
		if err != nil {
			return fmt.Errorf("randomReplace: Can not get database: %s", np.Database)
		}

		coll, err := db.Collection(nil, np.Collection)
		if err != nil {
			PrintTS(fmt.Sprintf("randomReplace: could not open `%s` collection: %v\n", np.Collection, err))
			return err
		}

		ctx := context.Background()
		colSize, err := coll.Count(ctx)
		if err != nil {
			PrintTS(fmt.Sprintf("randomReplace: could not count num of docs for `%s` collection: %v\n", np.Collection, err))
			return err
		}

		var randIntId int64
		var batchSizeLimit int64
		if np.BatchSize > colSize {
			batchSizeLimit = colSize
		} else {
			batchSizeLimit = np.BatchSize
		}
		times := make([]time.Duration, 0, np.LoadPerThread)
		cyclestart := time.Now()

		writeConflicts := int64(0)

		// It is crucial that every go routine has its own random source, otherwise
		// we create a lot of contention.
		source := rand.New(rand.NewSource(int64(id) + rand.Int63()))
		stats := NormalStatsOneThread{}

		for i := int64(1); i <= np.LoadPerThread; i++ {
			keys := make([]string, 0, batchSizeLimit)
			docs := make([]datagen.Doc, 0, batchSizeLimit)

			for j := int64(1); j <= batchSizeLimit; j++ {
				var doc datagen.Doc
				randIntId = source.Int63n(colSize)
				doc.ShaKey(randIntId, int(np.DocConfig.KeySize))
				keys = append(keys, doc.Key)

				doc.FillData(&np.DocConfig, source)
				docs = append(docs, doc)
			}
			start := time.Now()

			var err error
			var errSlice driver.ErrorSlice
			var cursor driver.Cursor
			if np.UseAql {
				query := `
				  LET amountOfEntries = COUNT(@keys) - 1
				  FOR position IN 0..(amountOfEntries)
				    LET key = NTH(@keys, position)
				    LET doc = NTH(@docs, position)
				    REPLACE key WITH doc IN @@collectionName
				`

				bindVars := map[string]interface{}{
					"@collectionName": coll.Name(),
					"docs":            docs,
					"keys":            keys,
				}

				cursor, err = db.Query(ctx, query, bindVars)
				if err != nil {
					return fmt.Errorf("QueryError: Can not replace documents %v\n", err)
				} else {
					PrintTS(fmt.Sprintf("QuerySuccess!.\n\n"))
				}
				defer cursor.Close()
			} else {
				_, errSlice, err = coll.ReplaceDocuments(ctx, keys, docs)
			}

			if err != nil {
				// if there's a write/write conflict, we ignore it, but count for
				// statistics, err is not supposed to return a write conflict, only
				// the ErrorSlice, but doesn't hurt performance much to test it
				if driver.IsPreconditionFailed(err) {
					writeConflicts++
				} else {
					stats.NumReplaceWriteConflicts += writeConflicts
					return fmt.Errorf("Can not replace documents %v\n", err)
				}
			}
			if errSlice != nil {
				for _, err2 := range errSlice {
					if err2 != nil {
						if driver.IsPreconditionFailed(err2) {
							writeConflicts++
						}
					}
				}
			}
			metrics.DocumentsReplaced.Add(float64(batchSizeLimit))
			metrics.BatchesReplaced.Inc()

			times = append(times, time.Now().Sub(start))
		}
		totaltime := time.Now().Sub(cyclestart)
		replacespersec := float64(np.LoadPerThread) * float64(batchSizeLimit) / (float64(totaltime) / float64(time.Second))
		stats.TotalTime = totaltime
		stats.NumberOps = np.LoadPerThread * batchSizeLimit
		stats.OpsPerSecond = replacespersec
		stats.FillInStats(times)
		PrintStatistics(&stats, fmt.Sprintf("normal (replace):\n  Times for replacing %d batches (line %d of script).\n  docs per second in this go routine: %f", np.Stats.StartLine, np.LoadPerThread, replacespersec))

		// Report back:
		np.Stats.Mutex.Lock()
		np.Stats.Threads = append(np.Stats.Threads, stats)
		np.Stats.Mutex.Unlock()
		stats.NumReplaceWriteConflicts += writeConflicts
		return nil
	}, func(totaltime time.Duration, haveError bool) error {
		// Here, we aggregate the data from all threads and report for the
		// whole command:
		np.Stats.Overall = AggregateStats(np.Stats.Threads, totaltime)
		np.Stats.Overall.TotalTime = totaltime
		np.Stats.Overall.HaveError = haveError
		batchesPerSec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
		replacespersec := batchesPerSec * float64(np.BatchSize)
		msg := fmt.Sprintf("normal (replace):\n  Total number of documents replaced: %d (line %d of script),\n  total batches per second: %f,\n  total docs per second: %f,\n  with errors: %v", np.Parallelism*np.LoadPerThread*np.BatchSize, np.Stats.StartLine, batchesPerSec, replacespersec, haveError)
		statsmsg := np.Stats.Overall.StatsToStrings()
		PrintTSs(msg, statsmsg)
		return nil
	},
	)
	if err != nil {
		return fmt.Errorf("randomReplace: can not replace randomly in parallel: %v", err)
	}
	return nil
}

func updateRandomlyInParallel(np *NormalProg) error {
	err := RunParallel(np.Parallelism, np.StartDelay, "randomUpdate", func(id int64) error {
		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("randomUpdate: Can not make client: %v", err)
		}
		db, err := cl.Database(context.Background(), np.Database)
		if err != nil {
			return fmt.Errorf("randomUpdate: Can not get database: %s", np.Database)
		}

		coll, err := db.Collection(nil, np.Collection)
		if err != nil {
			PrintTS(fmt.Sprintf("randomUpdate: could not open `%s` collection: %v\n", np.Collection, err))
			return err
		}

		ctx := context.Background()
		colSize, err := coll.Count(ctx)
		if err != nil {
			PrintTS(fmt.Sprintf("randomUpdate: could not count num of docs for `%s` collection: %v\n", np.Collection, err))
			return err
		}

		var randIntId int64
		var batchSizeLimit int64
		if np.BatchSize > colSize {
			batchSizeLimit = colSize
		} else {
			batchSizeLimit = np.BatchSize
		}
		times := make([]time.Duration, 0, np.LoadPerThread)
		cyclestart := time.Now()
		writeConflicts := int64(0)

		// It is crucial that every go routine has its own random source, otherwise
		// we create a lot of contention.
		source := rand.New(rand.NewSource(int64(id) + rand.Int63()))
		stats := NormalStatsOneThread{}

		for i := int64(1); i <= np.LoadPerThread; i++ {
			keys := make([]string, 0, batchSizeLimit)
			docs := make([]datagen.Doc, 0, batchSizeLimit)

			for j := int64(1); j <= batchSizeLimit; j++ {
				var doc datagen.Doc
				randIntId = source.Int63n(colSize)
				doc.ShaKey(randIntId, int(np.DocConfig.KeySize))
				keys = append(keys, doc.Key)
				doc.FillData(&np.DocConfig, source)
				docs = append(docs, doc)
			}

			start := time.Now()

			_, errSlice, err := coll.UpdateDocuments(ctx, keys, docs)
			if err != nil {
				//if there's a write/write conflict, we ignore it, but count for statistics, err is not supposed to return a write conflict, only the ErrorSlice, but doesn't hurt performance much to test it
				if driver.IsPreconditionFailed(err) {
					writeConflicts++
				} else {
					stats.NumUpdateWriteConflicts += writeConflicts
					return fmt.Errorf("Can not update documents %v\n", err)
				}
			}
			if errSlice != nil {
				for _, err2 := range errSlice {
					if err2 != nil {
						if driver.IsPreconditionFailed(err2) {
							writeConflicts++
						}
					}
				}
			}
			metrics.BatchesUpdated.Add(float64(batchSizeLimit))
			metrics.BatchesUpdated.Inc()
			times = append(times, time.Now().Sub(start))
		}
		totaltime := time.Now().Sub(cyclestart)
		updatespersec := float64(np.LoadPerThread) * float64(batchSizeLimit) / (float64(totaltime) / float64(time.Second))
		stats.TotalTime = totaltime
		stats.NumberOps = np.LoadPerThread * batchSizeLimit
		stats.OpsPerSecond = updatespersec
		stats.FillInStats(times)
		PrintStatistics(&stats, fmt.Sprintf("normal (update):\n  Times for updating %d batches (line %d of script).\n  docs per second in this go routine: %f", np.LoadPerThread, np.Stats.StartLine, updatespersec))

		// Report back:
		np.Stats.Mutex.Lock()
		np.Stats.Threads = append(np.Stats.Threads, stats)
		np.Stats.Mutex.Unlock()
		stats.NumReplaceWriteConflicts += writeConflicts
		return nil
	},

		func(totaltime time.Duration, haveError bool) error {
			// Here, we aggregate the data from all threads and report for the
			// whole command:
			np.Stats.Overall = AggregateStats(np.Stats.Threads, totaltime)
			np.Stats.Overall.TotalTime = totaltime
			np.Stats.Overall.HaveError = haveError
			batchesPerSec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
			updatespersec := batchesPerSec * float64(np.BatchSize)
			msg := fmt.Sprintf("normal (update):\n  Total number of documents updated: %d (line %d of script),\n  total batches per second: %f,\n  total docs per second: %f,\n  with errors: %v", np.Parallelism*np.LoadPerThread*np.BatchSize, np.Stats.StartLine, batchesPerSec, updatespersec, haveError)
			statsmsg := np.Stats.Overall.StatsToStrings()
			PrintTSs(msg, statsmsg)
			return nil
		})
	if err != nil {
		return fmt.Errorf("randomUpdate: can not update randomly in parallel: %v", err)
	}
	return nil
}

func readRandomlyInParallel(np *NormalProg) error {
	err := RunParallel(np.Parallelism, np.StartDelay, "randomRead", func(id int64) error {
		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("randomRead: Can not make client: %v", err)
		}
		db, err := cl.Database(context.Background(), np.Database)
		if err != nil {
			return fmt.Errorf("randomRead: Can not get database: %s", np.Database)
		}

		coll, err := db.Collection(nil, np.Collection)
		if err != nil {
			PrintTS(fmt.Sprintf("randomRead: could not open `%s` collection: %v\n", np.Collection, err))
			return err
		}

		ctx := context.Background()
		colSize, err := coll.Count(ctx)
		if err != nil {
			PrintTS(fmt.Sprintf("randomRead: could not count num of docs for `%s` collection: %v\n", np.Collection, err))
			return err
		}

		// It is crucial that every go routine has its own random source, otherwise
		// we create a lot of contention.
		source := rand.New(rand.NewSource(int64(id) + rand.Int63()))

		times := make([]time.Duration, 0, np.LoadPerThread)
		cyclestart := time.Now()
		for i := int64(1); i <= np.LoadPerThread; i++ {
			randIntId := source.Int63n(colSize)

			var doc datagen.Doc
			doc.ShaKey(randIntId, int(np.DocConfig.KeySize))

			var doc2 datagen.Doc
			start := time.Now()

			var err error
			if np.UseAql {
				query := "RETURN DOCUMENT(@myDocumentID)"
				bindVars := map[string]interface{}{
					"myDocumentID": coll.Name() + "/" + doc.Key,
				}
				_, err = db.Query(ctx, query, bindVars)
			} else {
				_, err = coll.ReadDocument(ctx, doc.Key, &doc2)
			}

			if err != nil {
				return fmt.Errorf("Can not read document with _key %s %v\n", doc.Key, err)
			}
			metrics.DocumentsRead.Inc()
			metrics.BatchesRead.Inc()
			times = append(times, time.Now().Sub(start))
		}
		totaltime := time.Now().Sub(cyclestart)
		readspersec := float64(np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
		stats := NormalStatsOneThread{}
		stats.TotalTime = totaltime
		stats.NumberOps = np.LoadPerThread
		stats.OpsPerSecond = readspersec
		stats.FillInStats(times)
		PrintStatistics(&stats, fmt.Sprintf("normal (randomRead):\n  Times for reading %d documents (line %d of script).\n  docs per second in this go routine: %f", np.LoadPerThread, np.Stats.StartLine, readspersec))

		// Report back:
		np.Stats.Mutex.Lock()
		np.Stats.Threads = append(np.Stats.Threads, stats)
		np.Stats.Mutex.Unlock()
		return nil
	}, func(totaltime time.Duration, haveError bool) error {
		// Here, we aggregate the data from all threads and report for the
		// whole command:
		np.Stats.Overall = AggregateStats(np.Stats.Threads, totaltime)
		np.Stats.Overall.TotalTime = totaltime
		np.Stats.Overall.HaveError = haveError
		readspersec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
		msg := fmt.Sprintf("normal (randomRead):\n  Total number of documents read: %d (line %d of script),\n  total docs per second: %f,\n  with errors: %v", np.Parallelism*np.LoadPerThread, np.Stats.StartLine, readspersec, haveError)
		statsmsg := np.Stats.Overall.StatsToStrings()
		PrintTSs(msg, statsmsg)
		return nil
	},
	)
	if err != nil {
		return fmt.Errorf("randomRead: can not read randomly in parallel: %v", err)
	}
	return nil
}

// writeSomeBatchesParallel does some batch imports in parallel
func writeSomeBatchesParallel(np *NormalProg, number int64) error {
	err := RunParallel(np.Parallelism, np.StartDelay, "writeSomeBatches", func(id int64) error {
		nrBatches := number / np.Parallelism

		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("Can not make client: %v", err)
		}
		db, err := cl.Database(context.Background(), np.Database)
		if err != nil {
			return fmt.Errorf("Can not get database: %s", np.Database)
		}

		insertCollection, err := db.Collection(nil, np.Collection)
		if err != nil {
			PrintTS(fmt.Sprintf("writeSomeBatches: could not open `%s` collection: %v\n", np.Collection, err))
			return err
		}

		docs := make([]datagen.Doc, 0, np.BatchSize)
		times := make([]time.Duration, 0, np.BatchSize)
		cyclestart := time.Now()
		last100start := cyclestart

		// It is crucial that every go routine has its own random source, otherwise
		// we create a lot of contention.
		source := rand.New(rand.NewSource(int64(id) + rand.Int63()))

		for i := int64(1); i <= nrBatches; i++ {
			start := time.Now()
			for j := int64(1); j <= np.BatchSize; j++ {
				var doc datagen.Doc
				doc.ShaKey((id*nrBatches+i-1)*np.BatchSize+j-1, int(np.DocConfig.KeySize))
				doc.FillData(&np.DocConfig, source)
				if np.AddFromTo {
					if np.Smart {
						smartF := datagen.MakeRandomString(2, source)
						smartT := datagen.MakeRandomString(2, source)
						doc.From = np.VertexCollName + "/" + smartF + ":" + datagen.MakeRandomString(8, source)
						doc.To = np.VertexCollName + "/" + smartT + ":" + datagen.MakeRandomString(8, source)
						doc.Key = smartF + ":" + doc.Key + ":" + smartT
					} else {
						doc.From = np.VertexCollName + "/" + datagen.MakeRandomString(8, source)
						doc.To = np.VertexCollName + "/" + datagen.MakeRandomString(8, source)
					}
				}
				docs = append(docs, doc)
			}
			ctx, cancel := context.WithTimeout(driver.WithOverwriteMode(context.Background(), driver.OverwriteModeIgnore), time.Duration(np.Timeout)*time.Second)

			var err error
			if np.UseAql && np.AddFromTo {
				// Reason: Right now all generated docs land in the same document arary `docs`. If we want to allow
				// creation via AQL here as well, we need to split the docs array by their collection names and/or
				// modify the query in the `np.UseAql` case.
				return fmt.Errorf("currently it is not supported to set useAql and addFromTo to `true` at the same time")
			}
			if np.UseAql {
				query := "FOR d IN @docs INSERT d INTO " + insertCollection.Name()
				bindVars := map[string]interface{}{
					"docs": docs,
				}
				_, err = db.Query(ctx, query, bindVars)
			} else {
				_, _, err = insertCollection.CreateDocuments(ctx, docs)
			}

			cancel()
			if err != nil {
				PrintTS(fmt.Sprintf("writeSomeBatches: could not write batch: %v, id: %d", err, id))
				if np.Retries == 0 {
					return err
				}
				var i int64 = 1
				for i <= np.Retries {
					if config.Verbose {
						PrintTS(fmt.Sprintf("normal: %s Need retry for id %d: %d of %d.\n", time.Now(), id, i, np.Retries))
					}
					ctx, cancel = context.WithTimeout(driver.WithOverwriteMode(context.Background(), driver.OverwriteModeIgnore), time.Duration(np.Timeout)*time.Second)
					if np.UseAql {
						query := "FOR d IN @docs INSERT d INTO " + insertCollection.Name()
						bindVars := map[string]interface{}{
							"docs": docs,
						}
						_, err = db.Query(ctx, query, bindVars)
					} else {
						_, _, err = insertCollection.CreateDocuments(ctx, docs)
					}

					cancel()
					if err == nil {
						if config.Verbose {
							PrintTS(fmt.Sprintf("writeSomeBatches: retry %d of %d was successful, id: %d", i, np.Retries, id))
						}
						break
					}
					PrintTS(fmt.Sprintf("writeSomeBatches: could not write batch: %v, id: %d, retry %d of %d", err, id, i, np.Retries))
					i += 1
				}
				if err != nil {
					return err
				}
			}
			metrics.DocumentsInserted.Add(float64(np.BatchSize))
			metrics.BatchesInserted.Inc()
			docs = docs[0:0]
			times = append(times, time.Now().Sub(start))
			if i%100 == 0 {
				dur := float64(time.Now().Sub(last100start)) / float64(time.Second)
				last100start = time.Now()

				// Intermediate report:
				if config.Verbose {
					PrintTS(fmt.Sprintf("normal: %s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur))
				}
			}
		}

		totaltime := time.Now().Sub(cyclestart)
		nrDocs := np.BatchSize * nrBatches
		docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))
		stats := NormalStatsOneThread{
			TotalTime:    totaltime,
			NumberOps:    nrDocs,
			OpsPerSecond: docspersec,
		}
		stats.FillInStats(times)
		PrintStatistics(&stats, fmt.Sprintf("normal (insert):\n  Times for inserting %d batches (line %d of script).\n  docs per second in this go routine: %f", nrBatches, np.Stats.StartLine, docspersec))

		// Report back:
		np.Stats.Mutex.Lock()
		np.Stats.Threads = append(np.Stats.Threads, stats)
		np.Stats.Mutex.Unlock()

		return nil
	}, func(totaltime time.Duration, haveError bool) error {
		// Here, we aggregate the data from all threads and report for the
		// whole command:
		np.Stats.Overall = AggregateStats(np.Stats.Threads, totaltime)
		np.Stats.Overall.TotalTime = totaltime
		np.Stats.Overall.HaveError = haveError
		batchesPerSec := float64(number) / (float64(totaltime) / float64(time.Second))
		docspersec := float64(number*np.BatchSize) / (float64(totaltime) / float64(time.Second))

		msg := fmt.Sprintf("normal (insert):\n  Total number of documents written: %d,\n  total batches per second: %f,\n  total docs per second: %f,\n  with errors: %v", number*np.BatchSize, batchesPerSec, docspersec, haveError)
		statsmsg := np.Stats.Overall.StatsToStrings()
		PrintTSs(msg, statsmsg)

		return nil
	})
	if err != nil {
		return fmt.Errorf("can not write some batches in parallel: %v", err)
	}
	return nil
}

// Execute executes a program of type NormalProg, depending on which
// subcommand is chosen in the arguments.
func (np *NormalProg) Execute() error {
	// This actually executes the NormalProg:
	np.Stats.StartTime = time.Now()
	var cl driver.Client
	var err error
	if config.Jwt != "" {
		cl, err = client.NewClient(config.Endpoints, driver.RawAuthentication(config.Jwt), config.Protocol)
	} else {
		cl, err = client.NewClient(config.Endpoints, driver.BasicAuthentication(config.Username, config.Password), config.Protocol)
	}
	if err != nil {
		np.Stats.EndTime = time.Now()
		np.Stats.RunTime = np.Stats.EndTime.Sub(np.Stats.StartTime)
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	switch np.SubCommand {
	case "create":
		err = np.Create(cl)
	case "drop":
		err = np.DoDrop(cl)
	case "truncate":
		err = np.Truncate(cl)
	case "insert":
		err = np.Insert()
	case "randomRead":
		err = np.RandomRead()
	case "randomUpdate":
		err = np.RandomUpdate()
	case "randomReplace":
		err = np.RandomReplace()
	case "createIdx":
		err = np.CreateIdx()
	case "dropIdx":
		err = np.DropIdx()
	case "queryOnIdx":
		err = np.RunQueryOnIdx()
	case "dropDatabase":
		err = np.DropDatabase(cl)
	default:
		err = nil
	}
	np.Stats.EndTime = time.Now()
	np.Stats.RunTime = np.Stats.EndTime.Sub(np.Stats.StartTime)
	return err
}
