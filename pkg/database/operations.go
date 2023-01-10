package database

import (
	"context"

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
