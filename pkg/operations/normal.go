package operations

import (
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/feed/pkg/client"
	"github.com/neunhoef/feed/pkg/config"
	"github.com/neunhoef/feed/pkg/database"
	"github.com/neunhoef/feed/pkg/feedlang"

	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	wordList = []string{
		"Aldi Süd",
		"Aldi Nord",
		"Lidl",
		"Edeka",
		"Tengelmann",
		"Grosso",
		"allkauf",
		"neukauf",
		"Rewe",
		"Holdrio",
		"real",
		"Globus",
		"Norma",
		"Del Haize",
		"Spar",
		"Tesco",
		"Morrison",
	}
)

type DurationSlice []time.Duration

func (d DurationSlice) Len() int {
	return len(d)
}

func (d DurationSlice) Less(a, b int) bool {
	return d[a] < d[b]
}

func (d DurationSlice) Swap(a, b int) {
	var dummy time.Duration = d[a]
	d[a] = d[b]
	d[b] = dummy
}

type Point []float64

type Poly struct {
	Type        string  `json:"type"`
	Coordinates []Point `json:"coordinates"`
}

type Doc struct {
	Key     string `json:"_key"`
	Sha     string `json:"sha"`
	Payload string `json:"payload"`
	Geo     *Poly  `Json:"geo,omitempty"`
	Words   string `json:"words,omitempty"`
}

// makeRandomPolygon makes a random GeoJson polygon.
func makeRandomPolygon(source *rand.Rand) *Poly {
	ret := Poly{Type: "polygon", Coordinates: make([]Point, 0, 5)}
	for i := 1; i <= 4; i += 1 {
		ret.Coordinates = append(ret.Coordinates,
			Point{source.Float64()*300.0 - 90.0, source.Float64()*160.0 - 80.0})
	}
	return &ret
}

// makeRandomStringWithSpaces creates slice of bytes for the provided length.
// Each byte is in range from 33 to 123.
func makeRandomStringWithSpaces(length int, source *rand.Rand) string {
	b := make([]byte, length, length)

	wordlen := source.Int()%17 + 3
	for i := 0; i < length; i++ {
		wordlen -= 1
		if wordlen == 0 {
			wordlen = source.Int()%17 + 3
			b[i] = byte(32)
		} else {
			s := source.Int()%52 + 65
			if s >= 91 {
				s += 6
			}
			b[i] = byte(s)
		}
	}
	return string(b)
}

func makeRandomWords(nr int, source *rand.Rand) string {
	b := make([]byte, 0, 15*nr)
	for i := 1; i <= nr; i += 1 {
		if i > 1 {
			b = append(b, ' ')
		}
		b = append(b, []byte(wordList[source.Int()%len(wordList)])...)
	}
	return string(b)
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
	SizePerDoc   int64
	Size         int64
	Parallelism  int64
	StartDelay   int64
	BatchSize    int64
	WithGeo      bool
	WithWords    int64
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
		WithWords:         0,
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
			case "withWords":
				e := CheckInt64Parameter(&np.WithWords, "withWords",
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
	if np.KeySize < 1 || np.KeySize > 64 {
		np.KeySize = 64
	}

	db, err := cl.Database(context.Background(), np.Database)
	if err != nil {
		return fmt.Errorf("Can not get database: %s", np.Database)
	}

	number := int64(1000000000) // compute something sensible
	payloadSize := int64(100)

	if err := writeSomeBatchesParallel(np.Parallelism, number, np.StartDelay, payloadSize, np.BatchSize, np.Collection, np.WithGeo, int(np.WithWords), int(np.KeySize), db); err != nil {
		return fmt.Errorf("can not do some batch imports")
	}

	return nil
}

// writeSomeBatchesParallel does some batch imports in parallel
func writeSomeBatchesParallel(parallelism int64, number int64, startDelay int64, payloadSize int64, batchSize int64, collectionName string, withGeo bool, withWords int, keySize int, db driver.Database) error {
	var mutex sync.Mutex
	totaltimestart := time.Now()
	wg := sync.WaitGroup{}
	haveError := false
	for i := 0; i <= int(parallelism)-1; i++ {
		time.Sleep(time.Duration(startDelay) * time.Millisecond)
		i := i // bring into scope
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			fmt.Printf("Starting go routine...\n")
			err := writeSomeBatches(number, int64(i), payloadSize, batchSize, collectionName, withGeo, withWords, keySize, db, &mutex)
			if err != nil {
				fmt.Printf("writeSomeBatches error: %v\n", err)
				haveError = true
			}
			mutex.Lock()
			fmt.Printf("Go routine %d done\n", i)
			mutex.Unlock()
		}(&wg, i)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	batchesPerSec := float64(int64(parallelism)*number) / (float64(totaltime) / float64(time.Second))
	docspersec := float64(int64(parallelism)*number*batchSize) / (float64(totaltime) / float64(time.Second))
	fmt.Printf("\nTotal number of documents written: %d, total time: %v, total batches per second: %f, total docs per second: %f\n", int64(parallelism)*number*batchSize, totaltimeend.Sub(totaltimestart), batchesPerSec, docspersec)
	if !haveError {
		return nil
	}
	fmt.Printf("Error in writeSomeBatches.\n")
	return fmt.Errorf("Error in writeSomeBatches.")
}

// writeSomeBatches writes `nrBatches` batches with `batchSize` documents.
func writeSomeBatches(nrBatches int64, id int64, payloadSize int64, batchSize int64, collectionName string, withGeo bool, withWords int, keySize int, db driver.Database, mutex *sync.Mutex) error {
	edges, err := db.Collection(nil, collectionName)
	if err != nil {
		fmt.Printf("writeSomeBatches: could not open `%s` collection: %v\n", collectionName, err)
		return err
	}
	docs := make([]Doc, 0, batchSize)
	times := make([]time.Duration, 0, batchSize)
	cyclestart := time.Now()
	last100start := cyclestart
	source := rand.New(rand.NewSource(int64(id) + rand.Int63()))
	for i := int64(1); i <= nrBatches; i++ {
		start := time.Now()
		for j := int64(1); j <= batchSize; j++ {
			which := (id*nrBatches+i-1)*batchSize + j - 1
			x := fmt.Sprintf("%d", which)
			key := fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
			x = "SHA" + x
			sha := fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
			pay := makeRandomStringWithSpaces(int(payloadSize), source)
			var poly *Poly
			if withGeo {
				poly = makeRandomPolygon(source)
			}
			var words string
			if withWords > 0 {
				words = makeRandomWords(withWords, source)
			}
			docs = append(docs, Doc{
				Key: key[0:keySize], Sha: sha, Payload: pay, Geo: poly, Words: words})
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
			mutex.Lock()
			fmt.Printf("%s Have imported %d batches for id %d, last 100 took %f seconds.\n", time.Now(), int(i), id, dur)
			mutex.Unlock()
		}
	}
	sort.Sort(DurationSlice(times))
	var sum int64 = 0
	for _, t := range times {
		sum = sum + int64(t)
	}
	totaltime := time.Now().Sub(cyclestart)
	nrDocs := batchSize * nrBatches
	docspersec := float64(nrDocs) / (float64(totaltime) / float64(time.Second))
	mutex.Lock()
	fmt.Printf("Times for %d batches (per batch): %s (median), %s (90%%ile), %s (99%%ilie), %s (average), docs per second in this go routine: %f\n", nrBatches, times[int(float64(0.5)*float64(nrBatches))], times[int(float64(0.9)*float64(nrBatches))], times[int(float64(0.99)*float64(nrBatches))], time.Duration(sum/nrBatches), docspersec)
	mutex.Unlock()
	return nil
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
		return np.Create(cl)
	case "insert":
		return np.Insert(cl)
	}
	return nil
}
