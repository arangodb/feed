package operations

import (
	"context"
	"fmt"
	"github.com/arangodb/feed/pkg/client"
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/database"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/metrics"
	"github.com/arangodb/go-driver"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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

	// Parameter for statistics (possibly other outputs) format
	OutFormat string

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

func (s *NormalStatsOneThread) StatsToStrings() []string {
	return []string{
		fmt.Sprintf("  NumberOps : %d\n", s.NumberOps),
		fmt.Sprintf("  OpsPerSec : %f\n", s.OpsPerSecond),
		fmt.Sprintf("  Median    : %v\n", s.Median),
		fmt.Sprintf("  90%%ile   : %v\n", s.Percentile90),
		fmt.Sprintf("  99%%ile   : %v\n", s.Percentile99),
		fmt.Sprintf("  Average   : %v\n", s.Average),
		fmt.Sprintf("  Minimum   : %v\n", s.Minimum),
		fmt.Sprintf("  Maximum   : %v\n", s.Maximum),
	}
}

func (n *NormalProg) StatsOutput() []string {
	res := []string{
		fmt.Sprintf("normal (%s): Have run for %v (lines %d..%d of script)\n",
			n.SubCommand, n.Stats.EndTime.Sub(n.Stats.StartTime), n.Stats.StartLine,
			n.Stats.EndLine),
		fmt.Sprintf("  Start time: %v\n", n.Stats.StartTime),
		fmt.Sprintf("  End time  : %v\n", n.Stats.EndTime),
	}
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
		DocConfig: datagen.DocumentConfig{
			SizePerDoc:   GetInt64Value(m, "documentSize", 128),
			Size:         GetInt64Value(m, "size", 16*1024*1024*1024),
			WithGeo:      GetBoolValue(m, "withGeo", false),
			WithWords:    GetInt64Value(m, "withWords", 0),
			KeySize:      GetInt64Value(m, "keySize", 32),
			NumberFields: GetInt64Value(m, "numberFields", 1),
		},

		Parallelism:   GetInt64Value(m, "parallelism", 16),
		StartDelay:    GetInt64Value(m, "startDelay", 5),
		BatchSize:     GetInt64Value(m, "batchSize", 1000),
		LoadPerThread: GetInt64Value(m, "loadPerThread", 50),
		QueryLimit:    GetInt64Value(m, "queryLimit", 1),
		IdxName:       GetStringValue(m, "idxName", ""),
		OutFormat:     GetStringValue(m, "outFormat", ""),
	}
}

func ValidateOutFormat(outFormat string) (bool, error) {
	if strings.HasPrefix(outFormat, "\"") {
		outFormat = outFormat[1 : len(outFormat)-1]
	}
	outFormat = strings.ToLower(outFormat)
	if len(outFormat) != 0 && outFormat != "json" {
		return false, fmt.Errorf("output format should be either empty string or JSON")
	} else {
		return (outFormat == "json"), nil
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
	metrics.CollectionsCreated.Inc()
	return nil
}

func (np *NormalProg) DoDrop(cl driver.Client) error {
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

	fmt.Printf("normal: Database %s found and collection %s successfully dropped.\n", np.Database, np.Collection)
	metrics.CollectionsDropped.Inc()
	return nil
}

func (np *NormalProg) DropDatabase(cl driver.Client) error {
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

	config.OutputMutex.Lock()
	fmt.Printf("normal: Database %s dropped successfully.\n", np.Database)
	config.OutputMutex.Unlock()
	metrics.DatabasesDropped.Inc()
	return nil
}

func (np *NormalProg) Truncate(cl driver.Client) error {
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

	fmt.Printf("normal: Database %s found and collection %s successfully truncated.\n", np.Database, np.Collection)
	metrics.CollectionsTruncated.Inc()
	return nil
}

func (np *NormalProg) Insert(cl driver.Client) error {
	if np.DocConfig.KeySize < 1 || np.DocConfig.KeySize > 64 {
		np.DocConfig.KeySize = 64
	}

	// Number of batches to put into the collection:
	number := (np.DocConfig.Size / np.DocConfig.SizePerDoc) / np.BatchSize

	Print("\n")
	PrintTS(fmt.Sprintf("normal: Will write %d batches of %d docs across %d goroutines...\n", number, np.BatchSize, np.Parallelism))

	if err := writeSomeBatchesParallel(np, number); err != nil {
		return fmt.Errorf("can not do some batch imports")
	}

	return nil
}

func (np *NormalProg) RunQueryOnIdx(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform query on idx.\n\n")
	config.OutputMutex.Unlock()

	if err := runQueryOnIdxInParallel(np); err != nil {
		return fmt.Errorf("can not run query on idx")
	}

	return nil
}

func (np *NormalProg) CreateIdx(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform index creation.\n\n")
	config.OutputMutex.Unlock()

	if err := createIdx(np); err != nil {
		return fmt.Errorf("can not create index")
	}
	metrics.IndexesCreated.Inc()

	return nil
}

func (np *NormalProg) DropIdx(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform index drop.\n\n")
	config.OutputMutex.Unlock()

	if err := dropIdx(np); err != nil {
		return fmt.Errorf("can not create drop")
	}
	metrics.IndexesDropped.Inc()

	return nil
}

func (np *NormalProg) RandomReplace(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform random replaces.\n\n")
	config.OutputMutex.Unlock()

	if err := replaceRandomlyInParallel(np); err != nil {
		return fmt.Errorf("can not replace randomly")
	}

	return nil
}

func (np *NormalProg) RandomUpdate(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform random updates.\n\n")
	config.OutputMutex.Unlock()

	if err := updateRandomlyInParallel(np); err != nil {
		return fmt.Errorf("can not update randomly")
	}

	return nil
}

func (np *NormalProg) RandomRead(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform random reads.\n\n")
	config.OutputMutex.Unlock()

	if err := readRandomlyInParallel(np); err != nil {
		return fmt.Errorf("can not read randomly")
	}

	return nil
}

func runQueryOnIdxInParallel(np *NormalProg) error {
	isJSON, err := ValidateOutFormat(np.OutFormat)
	if err != nil {
		return fmt.Errorf("\nqueryOnIdx: can not obtain output format, %v", err)
	}
	err = RunParallel(np.Parallelism, np.StartDelay, "queryOnIdx", func(id int64) error {
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
			config.OutputMutex.Lock()
			fmt.Printf("queryOnIdx: could not open `%s` collection: %v\n", np.Collection, err)
			config.OutputMutex.Unlock()
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

		// It is crucial that every go routine has its own random source, otherwise
		// we create a lot of contention.
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

		if isJSON {
			// JSON format
			err = WriteStatisticsForTimes(times,
				fmt.Sprintf(`{"normal": {"numQueriesRun": %d, "queriesRunPerSec": %f}}`, np.LoadPerThread, queriesonidxpersec), true)
		} else {
			err = WriteStatisticsForTimes(times,
				fmt.Sprintf("\nnormal: Times for running %d queries on indexes.\n  queries per second in this go routine: %f\n\n", np.LoadPerThread, queriesonidxpersec), false)
		}
		if err != nil {
			return fmt.Errorf("\nqueryOnIdx: can not write statistics in JSON format, %v", err)
		}
		return nil
	},
		func(totaltime time.Duration, haveError bool) error {
			idxqueriespersec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
			var msg string
			if isJSON {
				msg = fmt.Sprintf(`{"normal": {"totalNumQueryOnIdxExecs": %d, "totalQueriesPerSec": %f, "hadErrors": %v}}`, np.Parallelism*np.LoadPerThread, idxqueriespersec, haveError)
				msg, err = PrettyPrintToJSON(msg)
				if err != nil {
					return fmt.Errorf("can not write statistics in JSON format: %v", err)
				}
			} else {
				msg = fmt.Sprintf("\nnormal: Total number of query on index executions: %d,\n total queries per second: %f, \n hadErrors: %v\n\n", np.Parallelism*np.LoadPerThread, idxqueriespersec, haveError)
			}
			config.OutputMutex.Lock()
			fmt.Printf(msg)
			config.OutputMutex.Unlock()
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("queryOnIdx: can not update randomly in parallel: %v", err)
	}
	return nil
}

func createIdx(np *NormalProg) error {
	isJSON, err := ValidateOutFormat(np.OutFormat)
	if err != nil {
		return fmt.Errorf("\nidxCreation: can not obtain output format, %v", err)
	}
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
		config.OutputMutex.Lock()
		fmt.Printf("idxCreation: could not open `%s` collection: %v\n", np.Collection, err)
		config.OutputMutex.Unlock()
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
	/*
		not shuffle fields for now

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idxFields), func(i, j int) { idxFields[i], idxFields[j] = idxFields[j], idxFields[i] })

		not use this attribute for now
			if np.DocConfig.WithWords > 0 {
				idxFields = append(idxFields, "Words")
			}
	*/

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
		config.OutputMutex.Lock()
		fmt.Printf("normal: idxCreation done\n")
		config.OutputMutex.Unlock()
	}

	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	idxspersec := 1 / (float64(totaltime) / float64(time.Second))
	if isJSON {
		// JSON format
		msg := fmt.Sprintf(`{"normal": {"totalTime": %v, "totalIdxsPerSecond": %f, "hadErrors": %v}}`, totaltimeend.Sub(totaltimestart), idxspersec, haveError)
		msg, err = PrettyPrintToJSON(msg)
		if err != nil {
			return fmt.Errorf("\nidxCreation: can not write statistics in JSON format, %v", err)
		}
		fmt.Printf(msg)
	} else {
		fmt.Printf("\nnormal: total time: %v,\n total idxs per second: %f \n\n", totaltimeend.Sub(totaltimestart), idxspersec)
	}

	if !haveError {
		return nil
	}
	fmt.Printf("Error in idxCreation %v \n", err)
	return fmt.Errorf("Error in idxCreation.")
}

func dropIdx(np *NormalProg) error {
	isJSON, err := ValidateOutFormat(np.OutFormat)
	if err != nil {
		return fmt.Errorf("\nidxDrop: can not obtain output format, %v", err)
	}
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
		config.OutputMutex.Lock()
		fmt.Printf("idxDrop: could not open `%s` collection: %v\n", np.Collection, err)
		config.OutputMutex.Unlock()
		return err
	}

	var idxName string
	if strings.HasPrefix(np.IdxName, "\"") {
		np.IdxName = np.IdxName[1 : len(np.IdxName)-1]
	}
	if len(np.IdxName) > 0 {
		idxName = np.IdxName
	} else {
		config.OutputMutex.Lock()
		fmt.Printf("idxDrop: no index name given to drop in collection %s: %v\n",
			np.Collection, err)
		config.OutputMutex.Unlock()
		return fmt.Errorf("idxDrop: no index name given")
	}

	ctx := context.Background()
	indexes, err := coll.Indexes(ctx)
	if err != nil {
		fmt.Printf("idxDrop: cannot list indexes of collection %s: %v\n", np.Collection, err)
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
			fmt.Printf("idxDrop: Did not find index with name %s in collection %s in database %s",
				idxName, np.Collection, np.Database)
			err = fmt.Errorf("Did not find index with name %s in collection %s in database %s",
				idxName, np.Collection, np.Database)
		}
	}

	if config.Verbose && !haveError {
		config.OutputMutex.Lock()
		fmt.Printf("normal: idxDrop done\n")
		config.OutputMutex.Unlock()
	}

	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	idxspersec := 1 / (float64(totaltime) / float64(time.Second))
	if isJSON {
		// JSON format
		msg := fmt.Sprintf(`{"normal": {"totalTime": %v, "totalIdxsPerSecond": %f, "hadErrors": %v}}`, totaltimeend.Sub(totaltimestart), idxspersec, haveError)
		msg, err = PrettyPrintToJSON(msg)
		if err != nil {
			return fmt.Errorf("\nidxDrop: can not write statistics in JSON format, %v", err)
		}
		fmt.Printf(msg)
	} else {
		fmt.Printf("\nnormal: total time: %v,\n total idxs per second: %f \n\n", totaltimeend.Sub(totaltimestart), idxspersec)
	}

	if !haveError {
		return nil
	}
	fmt.Printf("Error in idxCreation %v \n", err)
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
			_, errSlice, err := coll.ReplaceDocuments(ctx, keys, docs)
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
		PrintStatistics(&stats, fmt.Sprintf("normal (replace):\n  Times for replacing %d batches.\n  docs per second in this go routine: %f", np.LoadPerThread, replacespersec))

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
		msg := fmt.Sprintf("normal (replace):\n  Total number of documents replaced: %d,\n  total batches per second: %f,\n  total docs per second: %f,\n  with errors: %v", np.Parallelism*np.LoadPerThread*np.BatchSize, batchesPerSec, replacespersec, haveError)
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
	isJSON, err := ValidateOutFormat(np.OutFormat)
	if err != nil {
		return fmt.Errorf("\nrandomUpdate: can not obtain output format, %v", err)
	}
	var mtx sync.Mutex

	err = RunParallel(np.Parallelism, np.StartDelay, "randomUpdate", func(id int64) error {
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
			config.OutputMutex.Lock()
			fmt.Printf("randomUpdate: could not open `%s` collection: %v\n", np.Collection, err)
			config.OutputMutex.Unlock()
			return err
		}

		ctx := context.Background()
		colSize, err := coll.Count(ctx)
		if err != nil {
			fmt.Printf("randomUpdate: could not count num of docs for `%s` collection: %v\n", np.Collection, err)
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

		if isJSON {
			// JSON format
			err = WriteStatisticsForTimes(times,
				fmt.Sprintf(`{"normal": {"numDocsUpdated": %d, "docsUpdatedPerSec": %f}}`, np.LoadPerThread*batchSizeLimit, updatespersec), true)
		} else {
			err = WriteStatisticsForTimes(times,
				fmt.Sprintf("\nnormal: Times for updating %d docs randomly.\n  docs per second in this go routine: %f", np.LoadPerThread*batchSizeLimit, updatespersec), false)
		}
		if err != nil {
			return fmt.Errorf("\nrandomUpdate: can not write statistics in JSON format, %v", err)
		}
		mtx.Lock()
		writeConflictStats.numReplaceWriteConflicts += writeConflicts
		mtx.Unlock()
		return nil
	},

		func(totaltime time.Duration, haveError bool) error {
			updatespersec := float64(np.Parallelism*np.LoadPerThread*np.BatchSize) / (float64(totaltime) / float64(time.Second))
			var msg string
			if isJSON {
				msg = fmt.Sprintf(`{"normal": {"totalNumUpdates": %d, "totalUpdatesPerSec": %f, "totalWriteConflicts": %d, "hadErrors": %v}}`, np.Parallelism*np.LoadPerThread*np.BatchSize, updatespersec, writeConflictStats.numUpdateWriteConflicts, haveError)
				msg, err = PrettyPrintToJSON(msg)
				if err != nil {
					return fmt.Errorf("can not write statistics in JSON format: %v", err)
				}
			} else {
				msg = fmt.Sprintf("\nnormal: Total number of updates: %d,\n total updates per second: %f,\n total write conflicts: %d,\n hadErrors: %v \n\n", np.Parallelism*np.LoadPerThread*np.BatchSize, updatespersec, writeConflictStats.numUpdateWriteConflicts, haveError)
			}
			config.OutputMutex.Lock()
			fmt.Printf(msg)
			config.OutputMutex.Unlock()
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("randomUpdate: can not update randomly in parallel: %v", err)
	}
	return nil
}

func readRandomlyInParallel(np *NormalProg) error {
	isJSON, err := ValidateOutFormat(np.OutFormat)
	if err != nil {
		return fmt.Errorf("\nrandomRead: can not obtain output format, %v", err)
	}

	err = RunParallel(np.Parallelism, np.StartDelay, "randomRead", func(id int64) error {
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
			config.OutputMutex.Lock()
			fmt.Printf("randomRead: could not open `%s` collection: %v\n", np.Collection, err)
			config.OutputMutex.Unlock()
			return err
		}

		ctx := context.Background()
		colSize, err := coll.Count(ctx)
		if err != nil {
			fmt.Printf("randomRead: could not count num of docs for `%s` collection: %v\n", np.Collection, err)
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
			_, err := coll.ReadDocument(ctx, doc.Key, &doc2)
			if err != nil {
				return fmt.Errorf("Can not read document with _key %s %v\n", doc.Key, err)
			}
			metrics.DocumentsRead.Inc()
			metrics.BatchesRead.Inc()
			times = append(times, time.Now().Sub(start))
		}
		totaltime := time.Now().Sub(cyclestart)
		readspersec := float64(np.LoadPerThread) / (float64(totaltime) / float64(time.Second))

		if isJSON {
			// JSON format
			err = WriteStatisticsForTimes(times,
				fmt.Sprintf(`{"normal": {"numDocsRead": %d, "docsReadPerSec": %f}}`, np.LoadPerThread, readspersec), true)
		} else {
			err = WriteStatisticsForTimes(times,
				fmt.Sprintf("\nnormal: Times for reading %d docs randomly.\n  docs per second in this go routine: %f", np.LoadPerThread, readspersec), false)
		}
		if err != nil {
			return fmt.Errorf("\nrandomRead: can not write statistics in JSON format, %v", err)
		}
		return nil
	}, func(totaltime time.Duration, haveError bool) error {
		readspersec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
		var msg string
		if isJSON {
			msg = fmt.Sprintf(`{"normal": {"totalNumReads": %d, "totalReadsPerSec": %f, "hadErrors": %v}}`, np.Parallelism*np.LoadPerThread, readspersec, haveError)
			msg, err = PrettyPrintToJSON(msg)
			if err != nil {
				return fmt.Errorf("can not write statistics in JSON format: %v", err)
			}
		} else {
			msg = fmt.Sprintf("normal: Total number of reads: %d,\n total reads per second: %f, hadErrors: %v", np.Parallelism*np.LoadPerThread, readspersec, haveError)
		}
		config.OutputMutex.Lock()
		fmt.Printf(msg)
		config.OutputMutex.Unlock()
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

		edges, err := db.Collection(nil, np.Collection)
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
				docs = append(docs, doc)
			}
			ctx, cancel := context.WithTimeout(driver.WithOverwriteMode(context.Background(), driver.OverwriteModeIgnore), time.Hour)
			_, _, err := edges.CreateDocuments(ctx, docs)
			cancel()
			if err != nil {
				PrintTS(fmt.Sprintf("writeSomeBatches: could not write batch: %v", err))
				return err
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
		PrintStatistics(&stats, fmt.Sprintf("normal (insert):\n  Times for inserting %d batches.\n  docs per second in this go routine: %f", nrBatches, docspersec))

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
		err = np.Insert(cl)
	case "randomRead":
		err = np.RandomRead(cl)
	case "randomUpdate":
		err = np.RandomUpdate(cl)
	case "randomReplace":
		err = np.RandomReplace(cl)
	case "createIdx":
		err = np.CreateIdx(cl)
	case "dropIdx":
		err = np.DropIdx(cl)
	case "queryOnIdx":
		err = np.RunQueryOnIdx(cl)
	case "dropDatabase":
		err = np.DropDatabase(cl)
	default:
		err = nil
	}
	np.Stats.EndTime = time.Now()
	np.Stats.RunTime = np.Stats.EndTime.Sub(np.Stats.StartTime)
	return err
}
