package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/go-driver"

	err2 "github.com/pkg/errors"
)

// IsNameSystemReserved checks if name of arangod resource is forbidden.
func IsNameSystemReserved(name string) bool {
	if len(name) > 0 && name[0] == '_' {
		return true
	}

	return false
}

// CreateOrGetDatabase returns handle to a database. If database does not exist then it is created.
func CreateOrGetDatabase(ctx context.Context, client driver.Client, DBName string,
	options *driver.CreateDatabaseOptions) (driver.Database, error) {
	if IsNameSystemReserved(DBName) {
		return client.Database(ctx, DBName)
	}

	handle, err := client.CreateDatabase(ctx, DBName, options)
	if err != nil {
		if driver.IsConflict(err) {
			return client.Database(ctx, DBName)
		}
		return nil, err
	}

	return handle, nil
}

// CreateOrGetCollection returns handle to a collection. If collection does not exist then it is created.
func CreateOrGetCollection(ctx context.Context, DBHandle driver.Database, colName string,
	options *driver.CreateCollectionOptions) (driver.Collection, error) {

	colHandle, err := DBHandle.CreateCollection(ctx, colName, options)

	if err == nil {
		return colHandle, nil
	}

	if driver.IsConflict(err) {
		// collection already exists
		return DBHandle.Collection(ctx, colName)
	}

	return nil, err
}

// CreateOrGetDatabaseCollection returns handle to a collection. Creates database and collection if needed.
func CreateOrGetDatabaseCollection(ctx context.Context, client driver.Client, DBName, colName string,
	options *driver.CreateCollectionOptions) (driver.Collection, error) {

	DBHandle, err := CreateOrGetDatabase(context.Background(), client, DBName, nil)
	if err != nil {
		return nil, err2.Wrap(err, "can not create/get database")
	}

	colHandle, err := CreateOrGetCollection(context.Background(), DBHandle, colName, options)
	if err == nil {
		return colHandle, nil
	}

	if driver.IsConflict(err) {
		// collection already exists
		return DBHandle.Collection(ctx, colName)
	}

	return nil, err
}

func RunParallel(parallelism int64, startDelay int64, jobName string,
	action func(id int64) error,
	finalReport func(totalTime time.Duration, haveError bool) error) error {
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
				fmt.Printf("%s: Starting go routine...\n", jobName)
				config.OutputMutex.Unlock()
			}
			err := action(int64(i))
			if err != nil {
				fmt.Printf("%s error: %v\n", jobName, err)
				haveError = true
			}
			if config.Verbose {
				config.OutputMutex.Lock()
				fmt.Printf("%s: Go routine %d done\n", jobName, i)
				config.OutputMutex.Unlock()
			}
		}(&wg, i)
	}

	wg.Wait()
	totaltimeend := time.Now()
	totaltime := totaltimeend.Sub(totaltimestart)
	if finalReport != nil {
		err := finalReport(totaltime, haveError)
		if err != nil {
			return fmt.Errorf("Error in job %s.", jobName)
		}
	}
	if !haveError {
		return nil
	}
	return fmt.Errorf("Error in job %s.", jobName)
}
