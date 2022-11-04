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
	"strconv"
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

	// Parameters for random reads:
	LoadPerThread int64
}

type WriteConflictStats struct {
	numUpdateWriteConflicts  int64
	numReplaceWriteConflicts int64
}

var writeConflictStats WriteConflictStats

func NewNormalProg(args []string) (feedlang.Program, error) {
	// This function parses the command line args and fills the values in
	// the struct.
	// Defaults:
	subCmd, m := ParseArguments(args)
	var np *NormalProg = &NormalProg{
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
	}
	//maybe put these subcommands in a set-like structure
	if np.SubCommand != "create" &&
		np.SubCommand != "insert" && np.SubCommand != "randomRead" && np.SubCommand != "randomUpdate" && np.SubCommand != "randomReplace" && np.SubCommand != "randomIdxCreate" {
		return nil, fmt.Errorf("Unknown subcommand %s", np.SubCommand)
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

	// Number of batches to put into the collection:
	number := (np.DocConfig.Size / np.DocConfig.SizePerDoc) / np.BatchSize

	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will write %d batches of %d docs across %d goroutines...\n\n", number, np.BatchSize, np.Parallelism)
	config.OutputMutex.Unlock()

	if err := writeSomeBatchesParallel(np, number); err != nil {
		return fmt.Errorf("can not do some batch imports")
	}

	return nil
}

func (np *NormalProg) RandomIdxCreate(cl driver.Client) error {
	config.OutputMutex.Lock()
	fmt.Printf("\nnormal: Will perform random index creation.\n\n")
	config.OutputMutex.Unlock()

	if err := createIdxsInParallel(np); err != nil {
		return fmt.Errorf("can not create indexes randomly")
	}

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

func createIdxsPerThread(np *NormalProg, mtx *sync.Mutex) error {
	// Let's use our own private client and connection here:
	var cl driver.Client
	var err error
	var endpoints []string = make([]string, 0, len(config.Endpoints))
	for _, e := range config.Endpoints {
		endpoints = append(endpoints, e)
	}
	rand.Shuffle(len(endpoints), func(i int, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
	endpoints = endpoints[0:1] // Restrict to the first
	config.OutputMutex.Lock()
	fmt.Printf("Endpoints: %v\n", endpoints)
	config.OutputMutex.Unlock()
	if config.Jwt != "" {
		cl, err = client.NewClient(endpoints, driver.RawAuthentication(config.Jwt))
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(config.Username, config.Password))
	}
	if err != nil {
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	col, err := db.Collection(nil, np.Collection)
	if err != nil {
		fmt.Printf("randomReplace: could not open `%s` collection: %v\n", np.Collection, err)
		return err
	}

	ctx := context.Background()

	times := make([]time.Duration, 0, np.LoadPerThread)
	cyclestart := time.Now()

	idxTypes := make([]string, 0, np.LoadPerThread)

	idxTypes = append(idxTypes, "persistent")
	if np.DocConfig.WithGeo {
		idxTypes = append(idxTypes, "geo")
	}

	idxFields := make([]string, 0, 17) // Paylaod from 1 to F + Words = 17

	for i := int64(1); i <= np.DocConfig.NumberFields; i++ {
		idxFields = append(idxFields, "Payload"+fmt.Sprintf("%x", i))
	}

	if np.DocConfig.WithWords > 0 {
		idxFields = append(idxFields, "Words")
	}

	for i := int64(1); i <= np.LoadPerThread; i++ {
		randIdxType := rand.Intn(len(idxTypes))
		var err error
		idxName := "idx" + strconv.Itoa(int(rand.Uint32()))
		start := time.Now()
		if idxTypes[randIdxType] == "persistent" {
			var options driver.EnsurePersistentIndexOptions
			options.Name = idxName
			_, _, err = col.EnsurePersistentIndex(ctx, idxFields, &options)
		} else if idxTypes[randIdxType] == "geo" {
			var options driver.EnsureGeoIndexOptions
			options.Name = idxName
			//the method didn't accept a simple array, so has to do the following to add the field as an argument
			geoPayload := make([]string, 0, 1)
			geoPayload = append(geoPayload, "Geo")
			_, _, err = col.EnsureGeoIndex(ctx, geoPayload, &options)
		}
		if err != nil {
			return fmt.Errorf("Can not create idxs %v\n", err)
		}
		times = append(times, time.Now().Sub(start))
	}
	totaltime := time.Now().Sub(cyclestart)
	idxspersec := float64(np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
	WriteStatisticsForTimes(times, fmt.Sprintf("\nnormal: Times for creating %d idxs randomly.\n  idxs per second in this go routine: %f", np.LoadPerThread, idxspersec))
	return nil
}

func createIdxsInParallel(np *NormalProg) error {
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	var mtx sync.Mutex
	for i := 0; i <= int(np.Parallelism)-1; i++ {
		time.Sleep(time.Duration(np.StartDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int, mtx *sync.Mutex) {
			defer wg.Done()
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Starting go routine...\n")
				config.OutputMutex.Unlock()
			}

			err := createIdxsPerThread(np, mtx)
			if err != nil {
				fmt.Printf("randomIdxCreation error: %v\n", err)
				haveError = true
			}

			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Go routine %d done\n", i)
				config.OutputMutex.Unlock()
			}
		}(&wg, i, &mtx)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	idxspersec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nnormal: Total number of idx creations: %d,\n  total time: %v,\n total idxs per second: %f \n\n", np.Parallelism*np.LoadPerThread, totaltimeend.Sub(totaltimestart), idxspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in randomIdxCreation.\n")
	return fmt.Errorf("Error in randomIdxCreation.")
}

func replacePerThread(np *NormalProg, mtx *sync.Mutex) error {
	// Let's use our own private client and connection here:
	var cl driver.Client
	var err error
	var endpoints []string = make([]string, 0, len(config.Endpoints))
	for _, e := range config.Endpoints {
		endpoints = append(endpoints, e)
	}
	rand.Shuffle(len(endpoints), func(i int, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
	endpoints = endpoints[0:1] // Restrict to the first
	config.OutputMutex.Lock()
	fmt.Printf("Endpoints: %v\n", endpoints)
	config.OutputMutex.Unlock()
	if config.Jwt != "" {
		cl, err = client.NewClient(endpoints, driver.RawAuthentication(config.Jwt))
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(config.Username, config.Password))
	}
	if err != nil {
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	col, err := db.Collection(nil, np.Collection)
	if err != nil {
		fmt.Printf("randomReplace: could not open `%s` collection: %v\n", np.Collection, err)
		return err
	}

	ctx := context.Background()
	colSize, err := col.Count(ctx)
	if err != nil {
		fmt.Printf("randomReplace: could not count num of docs for `%s` collection: %v\n", np.Collection, err)
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
	for i := int64(1); i <= np.LoadPerThread; i++ {
		keys := make([]string, 0, batchSizeLimit)
		docs := make([]datagen.Doc, 0, batchSizeLimit)

		source := rand.New(rand.NewSource(int64(np.LoadPerThread) + rand.Int63()))

		for j := int64(1); j <= batchSizeLimit; j++ {
			var doc datagen.Doc
			randIntId = rand.Int63n(colSize) + 1
			doc.ShaKey(randIntId, int(np.DocConfig.KeySize))
			keys = append(keys, doc.Key)

			doc.FillData(&np.DocConfig, source)
			docs = append(docs, doc)
		}
		start := time.Now()
		_, errSlice, err := col.ReplaceDocuments(ctx, keys, docs)
		if err != nil {
			//if there's a write/write conflict, we ignore it, but count for statistics, err is not supposed to return a write conflict, only the ErrorSlice, but doesn't hurt performance much to test it
			if driver.IsPreconditionFailed(err) {
				writeConflicts++
			} else {
				mtx.Lock()
				writeConflictStats.numReplaceWriteConflicts += writeConflicts
				mtx.Unlock()
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

		times = append(times, time.Now().Sub(start))
	}
	totaltime := time.Now().Sub(cyclestart)
	replacespersec := float64(np.LoadPerThread) * float64(batchSizeLimit) / (float64(totaltime) / float64(time.Second))
	WriteStatisticsForTimes(times, fmt.Sprintf("\nnormal: Times for replacing %d docs randomly.\n  docs per second in this go routine: %f", np.LoadPerThread*batchSizeLimit, replacespersec))
	mtx.Lock()
	writeConflictStats.numReplaceWriteConflicts += writeConflicts
	mtx.Unlock()
	return nil
}

func replaceRandomlyInParallel(np *NormalProg) error {
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	var mtx sync.Mutex
	for i := 0; i <= int(np.Parallelism)-1; i++ {
		time.Sleep(time.Duration(np.StartDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int, mtx *sync.Mutex) {
			defer wg.Done()
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Starting go routine...\n")
				config.OutputMutex.Unlock()
			}

			err := replacePerThread(np, mtx)
			if err != nil {
				fmt.Printf("randomReplace error: %v\n", err)
				haveError = true
			}

			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Go routine %d done\n", i)
				config.OutputMutex.Unlock()
			}
		}(&wg, i, &mtx)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	replacespersec := float64(np.Parallelism*np.LoadPerThread*np.BatchSize) / (float64(totaltime) / float64(time.Second))

	fmt.Printf("\nnormal: Total number of replaces: %d,\n  total time: %v,\n total replaces per second: %f,\n total write conflicts: %d \n\n", np.Parallelism*np.LoadPerThread*np.BatchSize, totaltimeend.Sub(totaltimestart), replacespersec, writeConflictStats.numReplaceWriteConflicts)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in randomReplace.\n")
	return fmt.Errorf("Error in randomReplace.")
}

func updatePerThread(np *NormalProg, mtx *sync.Mutex) error {
	// Let's use our own private client and connection here:
	var cl driver.Client
	var err error
	var endpoints []string = make([]string, 0, len(config.Endpoints))
	for _, e := range config.Endpoints {
		endpoints = append(endpoints, e)
	}
	rand.Shuffle(len(endpoints), func(i int, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
	endpoints = endpoints[0:1] // Restrict to the first
	config.OutputMutex.Lock()
	fmt.Printf("Endpoints: %v\n", endpoints)
	config.OutputMutex.Unlock()
	if config.Jwt != "" {
		cl, err = client.NewClient(endpoints, driver.RawAuthentication(config.Jwt))
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(config.Username, config.Password))
	}
	if err != nil {
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	col, err := db.Collection(nil, np.Collection)
	if err != nil {
		fmt.Printf("randomUpdate: could not open `%s` collection: %v\n", np.Collection, err)
		return err
	}

	ctx := context.Background()
	colSize, err := col.Count(ctx)
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
	for i := int64(1); i <= np.LoadPerThread; i++ {
		keys := make([]string, 0, batchSizeLimit)
		docs := make([]datagen.Doc, 0, batchSizeLimit)

		source := rand.New(rand.NewSource(int64(np.LoadPerThread) + rand.Int63()))

		for j := int64(1); j <= batchSizeLimit; j++ {
			var doc datagen.Doc
			randIntId = rand.Int63n(colSize) + 1
			doc.ShaKey(randIntId, int(np.DocConfig.KeySize))
			keys = append(keys, doc.Key)
			doc.FillData(&np.DocConfig, source)
			docs = append(docs, doc)
		}

		start := time.Now()

		_, errSlice, err := col.UpdateDocuments(ctx, keys, docs)
		if err != nil {
			//if there's a write/write conflict, we ignore it, but count for statistics, err is not supposed to return a write conflict, only the ErrorSlice, but doesn't hurt performance much to test it
			if driver.IsPreconditionFailed(err) {
				writeConflicts++
			} else {
				mtx.Lock()
				writeConflictStats.numUpdateWriteConflicts += writeConflicts
				mtx.Unlock()
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
		times = append(times, time.Now().Sub(start))
	}
	totaltime := time.Now().Sub(cyclestart)
	updatespersec := float64(np.LoadPerThread) * float64(batchSizeLimit) / (float64(totaltime) / float64(time.Second))
	WriteStatisticsForTimes(times, fmt.Sprintf("\nnormal: Times for updating %d docs randomly.\n  docs per second in this go routine: %f", np.LoadPerThread*batchSizeLimit, updatespersec))
	mtx.Lock()
	writeConflictStats.numReplaceWriteConflicts += writeConflicts
	mtx.Unlock()
	return nil
}

func updateRandomlyInParallel(np *NormalProg) error {
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	var mtx sync.Mutex
	for i := 0; i <= int(np.Parallelism)-1; i++ {
		time.Sleep(time.Duration(np.StartDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int, mtx *sync.Mutex) {
			defer wg.Done()
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Starting go routine...\n")
				config.OutputMutex.Unlock()
			}

			err := updatePerThread(np, mtx)
			mtx.Lock()
			writeConflictStats.numUpdateWriteConflicts++
			mtx.Unlock()
			if err != nil {
				fmt.Printf("randomUpdate error: %v\n", err)
				haveError = true
			}

			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Go routine %d done\n", i)
				config.OutputMutex.Unlock()
			}
		}(&wg, i, &mtx)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	updatespersec := float64(np.Parallelism*np.LoadPerThread*np.BatchSize) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nnormal: Total number of updates: %d,\n  total time: %v,\n total updates per second: %f,\n total write conflicts: %d \n\n", np.Parallelism*np.LoadPerThread*np.BatchSize, totaltimeend.Sub(totaltimestart), updatespersec, writeConflictStats.numUpdateWriteConflicts)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in randomUpdate.\n")
	return fmt.Errorf("Error in randomUpdate.")
}

func readOnlyPerThread(np *NormalProg) error {
	// Let's use our own private client and connection here:
	var cl driver.Client
	var err error
	var endpoints []string = make([]string, 0, len(config.Endpoints))
	for _, e := range config.Endpoints {
		endpoints = append(endpoints, e)
	}
	rand.Shuffle(len(endpoints), func(i int, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
	endpoints = endpoints[0:1] // Restrict to the first
	config.OutputMutex.Lock()
	fmt.Printf("Endpoints: %v\n", endpoints)
	config.OutputMutex.Unlock()
	if config.Jwt != "" {
		cl, err = client.NewClient(endpoints, driver.RawAuthentication(config.Jwt))
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(config.Username, config.Password))
	}
	if err != nil {
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	col, err := db.Collection(nil, np.Collection)
	if err != nil {
		fmt.Printf("randomReads: could not open `%s` collection: %v\n", np.Collection, err)
		return err
	}

	ctx := context.Background()
	colSize, err := col.Count(ctx)
	if err != nil {
		fmt.Printf("randomReads: could not count num of docs for `%s` collection: %v\n", np.Collection, err)
		return err
	}

	times := make([]time.Duration, 0, np.LoadPerThread)
	cyclestart := time.Now()
	for i := int64(1); i <= np.LoadPerThread; i++ {
		randIntId := rand.Int63n(colSize) + 1

		var doc datagen.Doc
		doc.ShaKey(randIntId, int(np.DocConfig.KeySize))

		var doc2 datagen.Doc
		start := time.Now()
		_, err := col.ReadDocument(ctx, doc.Key, &doc2)
		if err != nil {
			return fmt.Errorf("Can not read document with _key %s %v\n", doc.Key, err)
		} else {
			times = append(times, time.Now().Sub(start))
		}
	}
	totaltime := time.Now().Sub(cyclestart)
	readspersec := float64(np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
	WriteStatisticsForTimes(times, fmt.Sprintf("\nnormal: Times for reading %d docs randomly.\n  docs per second in this go routine: %f", np.LoadPerThread, readspersec))
	return nil
}

func readRandomlyInParallel(np *NormalProg) error {
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	for i := 0; i <= int(np.Parallelism)-1; i++ {
		time.Sleep(time.Duration(np.StartDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Starting go routine...\n")
				config.OutputMutex.Unlock()
			}

			err := readOnlyPerThread(np)
			if err != nil {
				fmt.Printf("randomRead error: %v\n", err)
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
	readspersec := float64(np.Parallelism*np.LoadPerThread) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nnormal: Total number of reads: %d,\n  total time: %v,\n total reads per second: %f\n\n", np.Parallelism*np.LoadPerThread, totaltimeend.Sub(totaltimestart), readspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in randomRead.\n")
	return fmt.Errorf("Error in randomRead.")
}

// writeSomeBatchesParallel does some batch imports in parallel
func writeSomeBatchesParallel(np *NormalProg, number int64) error {
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	for i := 0; i <= int(np.Parallelism)-1; i++ {
		time.Sleep(time.Duration(np.StartDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: Starting go routine...\n")
				config.OutputMutex.Unlock()
			}
			err := writeSomeBatches(np, number/np.Parallelism, int64(i))
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
	docspersec := float64(number*np.BatchSize) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nnormal: Total number of documents written: %d,\n  total time: %v,\n  total batches per second: %f,\n  total docs per second: %f\n\n", number*np.BatchSize, totaltimeend.Sub(totaltimestart), batchesPerSec, docspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in writeSomeBatches.\n")
	return fmt.Errorf("Error in writeSomeBatches.")
}

// writeSomeBatches writes `nrBatches` batches with `batchSize` documents.
func writeSomeBatches(np *NormalProg, nrBatches int64, id int64) error {
	// Let's use our own private client and connection here:
	var cl driver.Client
	var err error
	var endpoints []string = make([]string, 0, len(config.Endpoints))
	for _, e := range config.Endpoints {
		endpoints = append(endpoints, e)
	}
	rand.Shuffle(len(endpoints), func(i int, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
	endpoints = endpoints[0:1] // Restrict to the first
	config.OutputMutex.Lock()
	fmt.Printf("Endpoints: %v\n", endpoints)
	config.OutputMutex.Unlock()
	if config.Jwt != "" {
		cl, err = client.NewClient(endpoints, driver.RawAuthentication(config.Jwt))
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(config.Username, config.Password))
	}
	if err != nil {
		return fmt.Errorf("Could not connect to database at %v: %v\n", config.Endpoints, err)
	}
	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	edges, err := db.Collection(nil, np.Collection)
	if err != nil {
		fmt.Printf("writeSomeBatches: could not open `%s` collection: %v\n", np.Collection, err)
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
			fmt.Printf("writeSomeBatches: could not write batch: %v\n", err)
			return err
		}
		docs = docs[0:0]
		times = append(times, time.Now().Sub(start))
		if i%100 == 0 {
			dur := float64(time.Now().Sub(last100start)) / float64(time.Second)
			last100start = time.Now()

			// Intermediate report:
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("normal: %s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur)
				config.OutputMutex.Unlock()
			}
		}
	}

	totaltime := time.Now().Sub(cyclestart)
	nrDocs := np.BatchSize * nrBatches
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
	case "randomRead":
		return np.RandomRead(cl)
	case "randomUpdate":
		return np.RandomUpdate(cl)
	case "randomReplace":
		return np.RandomReplace(cl)
	case "randomIdxCreate":
		return np.RandomIdxCreate(cl)
	}
	return nil
}
