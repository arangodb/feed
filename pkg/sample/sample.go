package sample

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/feed/pkg/client"
	"github.com/neunhoef/feed/pkg/database"
	"os"
)

// Doit: Example code for database/collection creation and driver usage
func Doit() {
	cl, err := client.NewClient([]string{"http://localhost:8529"}, driver.BasicAuthentication("root", ""))
	if err != nil {
		fmt.Printf("Could not connect to database at localhost: %v\n", err)
		os.Exit(1)
	}
	db, err := database.CreateOrGetDatabase(nil, cl, "xyz",
		&driver.CreateDatabaseOptions{})
	if err != nil {
		fmt.Printf("Could not create/open database xyz: %v\n", err)
		os.Exit(2)
	}
	_, err = database.CreateOrGetCollection(nil, db, "coll",
		&driver.CreateCollectionOptions{})
	if err != nil {
		fmt.Printf("Could not create/open collection coll: %v\n", err)
		os.Exit(3)
	}
}
