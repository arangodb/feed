package operations

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	format "github.com/apache/arrow/go/v16/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/metrics"
	"github.com/arangodb/go-driver"
	"golang.org/x/xerrors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

type ReplayAqlStats struct {
	Threads []NormalStatsOneThread `json:"threads"`
	Overall NormalStatsOneThread   `json:"overall"`
	Mutex   sync.Mutex             `json:"-"`
	feedlang.ProgMeta
}

type ReplayAqlProg struct {
	// Input files:
	Input             string
	StartDelay        int64
	Parallelism       int64
	DelayByTimestamps bool

	// Statistics:
	Stats NormalStats
}

func (r *ReplayAqlProg) Lines() (int, int) {
	return r.Stats.StartLine, r.Stats.EndLine
}

func (r *ReplayAqlProg) SetSource(lines []string) {
	r.Stats.Source = lines
}

func (r *ReplayAqlProg) StatsOutput() []string {
	res := config.MakeStatsOutput(r.Stats.Source, []string{
		fmt.Sprintf("replayAQL: Have run for %v (lines %d..%d of script)\n",
			r.Stats.EndTime.Sub(r.Stats.StartTime), r.Stats.StartLine,
			r.Stats.EndLine),
		fmt.Sprintf("  Start time: %v\n", r.Stats.StartTime),
		fmt.Sprintf("  End time  : %v\n", r.Stats.EndTime),
	})
	res = append(res, r.Stats.Overall.StatsToStrings()...)
	return res
}

func (r *ReplayAqlProg) StatsJSON() interface{} {
	return &r.Stats
}

func parseReplayArgs(subCmd string, m map[string]string) *ReplayAqlProg {
	return &ReplayAqlProg{
		Input:             GetStringValue(m, "input", "queries.list"),
		StartDelay:        GetInt64Value(m, "startDelay", 5),
		Parallelism:       GetInt64Value(m, "parallelism", 16),
		DelayByTimestamps: GetBoolValue(m, "delayByTimestamp", false),
	}
}

func NewReplayAqlProg(args []string, line int) (feedlang.Program, error) {
	// This function parses the command line args and fills the values in
	// the struct.
	// Defaults:
	subCmd, m := ParseArguments(args)
	rp := parseReplayArgs(subCmd, m)
	rp.Stats.StartLine = line
	rp.Stats.EndLine = line
	rp.Stats.Type = "replayAQL"
	return rp, nil
}

func (rp *ReplayAqlProg) Replay() error {
	Print("\n")
	PrintTS(fmt.Sprintf("replay: Will replay queries.\n\n"))

	if err := runReplayAqlInParallel(rp); err != nil {
		return fmt.Errorf("can not replay queries: %v", err)
	}

	return nil
}

type QueryJson struct {
	QueryString string                 `json:"query"`
	BindVars    map[string]interface{} `json:"bindVars"`
	Streaming   bool                   `json:"stream"`
}

type SingleQuery struct {
	TimeStamp string    `json:"t"`
	Database  string    `json:"db"`
	Query     QueryJson `json:"q"`
}

const (
	footerSize uint32 = 8
)

var (
	magicBytes                  = []byte("PAR1")
	magicEBytes                 = []byte("PARE")
	errInconsistentFileMetadata = xerrors.New("parquet: file is smaller than indicated metadata size")
)

func GetFileContentType(fname string) (string, error) {
	app := "file"
	cmd := exec.Command(app, fname)
	stdout, err := cmd.Output()
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}
	return string(stdout), nil
}

type Reader struct {
	r            parquet.ReaderAtSeeker
	props        *parquet.ReaderProperties
	metadata     *metadata.FileMetaData
	footerOffset int64

	bufferPool sync.Pool
}

type ReadOption func(*Reader)

// parseMetaData handles parsing the metadata from the opened file.
func (f *Reader) parseMetaData() error {
	if f.footerOffset <= int64(footerSize) {
		return fmt.Errorf("parquet: file too small (size=%d)", f.footerOffset)
	}

	buf := make([]byte, footerSize)
	// backup 8 bytes to read the footer size (first four bytes) and the magic bytes (last 4 bytes)
	n, err := f.r.ReadAt(buf, f.footerOffset-int64(footerSize))
	if err != nil && err != io.EOF {
		return fmt.Errorf("parquet: could not read footer: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("parquet: could not read %d bytes from end of file", len(buf))
	}

	size := int64(binary.LittleEndian.Uint32(buf[:4]))
	if size < 0 || size+int64(footerSize) > f.footerOffset {
		return errInconsistentFileMetadata
	}

	fileDecryptProps := f.props.FileDecryptProps

	switch {
	case bytes.Equal(buf[4:], magicBytes): // non-encrypted metadata
		buf = make([]byte, size)
		if _, err := f.r.ReadAt(buf, f.footerOffset-int64(footerSize)-size); err != nil {
			return fmt.Errorf("parquet: could not read footer: %w", err)
		}

		f.metadata, err = metadata.NewFileMetaData(buf, nil)
		if err != nil {
			return fmt.Errorf("parquet: could not read footer: %w", err)
		}

	case bytes.Equal(buf[4:], magicEBytes): // encrypted metadata
		buf = make([]byte, size)
		if _, err := f.r.ReadAt(buf, f.footerOffset-int64(footerSize)-size); err != nil {
			return fmt.Errorf("parquet: could not read footer: %w", err)
		}

		if fileDecryptProps == nil {
			return xerrors.New("could not read encrypted metadata, no decryption found in reader's properties")
		}

		fileCryptoMetadata, err := metadata.NewFileCryptoMetaData(buf)
		if err != nil {
			return err
		}

		f.metadata, err = metadata.NewFileMetaData(buf[fileCryptoMetadata.Len():], nil)
		if err != nil {
			return fmt.Errorf("parquet: could not read footer: %w", err)
		}
	default:
		return fmt.Errorf("parquet: magic bytes not found in footer. Either the file is corrupted or this isn't a parquet file")
	}

	return nil
}

func NewParquetReader(r parquet.ReaderAtSeeker, opts ...ReadOption) (*Reader, error) {
	var err error
	f := &Reader{r: r}
	for _, o := range opts {
		o(f)
	}

	if f.footerOffset <= 0 {
		f.footerOffset, err = r.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("parquet: could not retrieve footer offset: %w", err)
		}
	}

	if f.props == nil {
		f.props = parquet.NewReaderProperties(memory.NewGoAllocator())
	}

	f.bufferPool = sync.Pool{
		New: func() interface{} {
			buf := memory.NewResizableBuffer(f.props.Allocator())
			runtime.SetFinalizer(buf, func(obj *memory.Buffer) {
				obj.Release()
			})
			return buf
		},
	}

	if f.metadata == nil {
		return f, f.parseMetaData()
	}

	return f, nil
}

// NumRows returns the total number of rows in this parquet file.
func (f *Reader) NumRows() int64 {
	return f.metadata.GetNumRows()
}

// NumRowGroups returns the total number of row groups in this file.
func (f *Reader) NumRowGroups() int {
	return len(f.metadata.GetRowGroups())
}

// FileMetaData is a proxy around the underlying thrift FileMetaData object
// to make it easier to use and interact with.
type FileMetaData struct {
	*format.FileMetaData
	Schema        *schema.Schema
	FileDecryptor encryption.FileDecryptor

	// app version of the writer for this file
	version *AppVersion
	// size of the raw bytes of the metadata in the file which were
	// decoded by thrift, Size() getter returns the value.
	metadataLen int
}

// RowGroupReader is the primary interface for reading a single row group
type RowGroupReader struct {
	r            parquet.ReaderAtSeeker
	sourceSz     int64
	fileMetadata *metadata.FileMetaData
	rgMetadata   *metadata.RowGroupMetaData
	props        *parquet.ReaderProperties

	bufferPool *sync.Pool
}

// WriterVersion returns the constructed application version from the
// created by string
func (f *FileMetaData) WriterVersion() *AppVersion {
	if f.version == nil {
		f.version = NewAppVersion(f.GetCreatedBy())
	}
	return f.version
}

// RowGroup returns a reader for the desired (0-based) row group
func (f *Reader) RowGroup(i int) *RowGroupReader {
	rg := f.metadata.RowGroups[i]

	return &RowGroupReader{
		fileMetadata:  f.metadata,
		rgMetadata:    metadata.NewRowGroupMetaData(rg, f.metadata.Schema, f.WriterVersion(), f.fileDecryptor),
		props:         f.props,
		r:             f.r,
		sourceSz:      f.footerOffset,
		fileDecryptor: f.fileDecryptor,
		bufferPool:    &f.bufferPool,
	}
}

func runReplayAqlInParallel(rp *ReplayAqlProg) error {
	// First let's have a go routine which only reads the input and
	// stuffs it into a channel of objects.
	queries := make(chan *SingleQuery, 100)
	var firstTime time.Time
	var errFromReader error
	go func() {
		file, err := os.Open(rp.Input)
		if err != nil {
			// Handle error
			errFromReader = fmt.Errorf("replayAQL: Could not open input file %s, error: %v!\n", rp.Input, err)
			return
		}
		defer file.Close()

		contentType, err := GetFileContentType(rp.Input)
		if err != nil {
			panic(err)
		}

		if strings.Contains(contentType, "Parquet") {
			rdr, err := NewParquetReader(file)
			if err != nil {
				panic(err)
			}

			for r := 0; r < rdr.NumRowGroups(); r++ {
				fmt.Println("--- Row Group:", r, " ---")
				rgr := rdr.RowGroup(r)
			}
			rgr := rdr.RowGroup(r)
			rowGroupMeta := rgr.MetaData()

		} else {
			scanner := bufio.NewScanner(file)
			first := true
			for scanner.Scan() {
				var sq SingleQuery
				err = json.Unmarshal(scanner.Bytes(), &sq)
				if err != nil {
					// Handle error
					PrintTS(fmt.Sprintf("replayAQL: Could not parse json: %s, error: %v, skipping it...\n", scanner.Text(), err))
					return
				} else {
					if first {
						var err error
						firstTime, err = time.Parse(time.RFC3339, sq.TimeStamp)
						PrintTS(fmt.Sprintf("Guck: %v\n", firstTime))
						if err != nil {
							PrintTS(fmt.Sprintf("replayAQL: Could not parse first time stamp %s, error: %v\n", sq.TimeStamp, err))
						} else {
							first = false
						}
					}
					queries <- &sq
				}
			}
			close(queries)
		}
	}()

	overallStartTime := time.Now()

	err := RunParallel(rp.Parallelism, rp.StartDelay, "replayAQL", func(id int64) error {
		// Let's use our own private client and connection here:
		cl, err := config.MakeClient()
		if err != nil {
			return fmt.Errorf("replayAQL: Can not make client: %v", err)
		}

		// Per thread database cache:
		dbCache := make(map[string]driver.Database)

		// Statistics:
		var count int64 = 0
		var errorCount int64 = 0
		times := make([]time.Duration, 0, 1000000)
		cyclestart := time.Now()

		for q := range queries {
			// First find database:
			var db driver.Database
			db, found := dbCache[q.Database]
			if !found {
				database, err := cl.Database(context.Background(), q.Database)
				if err != nil {
					PrintTS(fmt.Sprintf("Can not get database: %s for query %v", q.Database, *q))
					errorCount += 1
					continue
				}
				dbCache[q.Database] = database
				db = database
			}

			if rp.DelayByTimestamps {
				theTime, err := time.Parse(time.RFC3339, q.TimeStamp)
				if err == nil {
					timeSinceFirst := theTime.Sub(firstTime)
					timeSinceStart := time.Now().Sub(overallStartTime)
					if timeSinceFirst > timeSinceStart {
						waitingTime := timeSinceFirst - timeSinceStart
						// Need to wait a bit:
						if waitingTime > time.Second && config.Verbose {
							PrintTS(fmt.Sprintf("Thread %d: sleeping for %v to fire next query when it is due...", id, waitingTime))
						}
						time.Sleep(waitingTime)
					}
				}

			}

			if config.Verbose {
				PrintTS(fmt.Sprintf("Thread %d: executing query %v ...", id, *q))
			}

			start := time.Now()

			ctx := context.Background()
			if q.Query.Streaming {
				ctx = driver.WithQueryStream(ctx, true)
			}
			cursor, err := db.Query(ctx, q.Query.QueryString, q.Query.BindVars)
			if err != nil {
				PrintTS(fmt.Sprintf("Can not execute query: %v, error: %v\n", *q, err))
				metrics.QueriesReplayedErrors.Inc()
				errorCount += 1
			} else {
				// Get result of query:
				for cursor.HasMore() {
					var obj map[string]interface{}
					meta, err := cursor.ReadDocument(ctx, &obj)
					if err != nil {
						PrintTS(fmt.Sprintf("Error when reading document: %v\n", err))
						errorCount += 1
						metrics.QueriesReplayedErrors.Inc()
						break
					} else {
						if config.Verbose {
							PrintTS(fmt.Sprintf("Document read: %v, meta: %v", obj, meta))
						}
					}
				}
				cursor.Close()
			}
			count += 1
			metrics.QueriesReplayed.Inc()
			times = append(times, time.Now().Sub(start))

		}
		totaltime := time.Now().Sub(cyclestart)
		queriespersec := float64(count) / (float64(totaltime) / float64(time.Second))
		stats := NormalStatsOneThread{}
		stats.TotalTime = totaltime
		stats.NumberOps = count
		stats.OpsPerSecond = queriespersec
		stats.FillInStats(times)
		Print("")
		PrintStatistics(&stats, fmt.Sprintf("replayAQL:\n  Times for replaying %d queries.\n  queries per second in this go routine: %f, errors: %d", count, queriespersec, errorCount))

		// Report back:
		rp.Stats.Mutex.Lock()
		rp.Stats.Threads = append(rp.Stats.Threads, stats)
		rp.Stats.Mutex.Unlock()
		return nil
	},
		func(totaltime time.Duration, haveError bool) error {
			// Here, we aggregate the data from all threads and report for the
			// whole command:
			rp.Stats.Overall = AggregateStats(rp.Stats.Threads, totaltime)
			rp.Stats.Overall.TotalTime = totaltime
			rp.Stats.Overall.HaveError = haveError
			queriespersec := float64(rp.Stats.Overall.NumberOps) / (float64(totaltime) / float64(time.Second))
			msg := fmt.Sprintf("replayAQL:\n  Total number of queries sent: %d,\n  total queries per second: %f,\n  with errors: %v", rp.Stats.Overall.NumberOps, queriespersec, haveError)
			statsmsg := rp.Stats.Overall.StatsToStrings()
			PrintTSs(msg, statsmsg)
			return nil
		},
	)
	if errFromReader != nil {
		return errFromReader
	}
	return err
}

// Execute executes a program of type NormalProg, depending on which
// subcommand is chosen in the arguments.
func (rp *ReplayAqlProg) Execute() error {
	// This actually executes the NormalProg:
	rp.Stats.StartTime = time.Now()
	err := rp.Replay()
	rp.Stats.EndTime = time.Now()
	rp.Stats.RunTime = rp.Stats.EndTime.Sub(rp.Stats.StartTime)
	return err
}
