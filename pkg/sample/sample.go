package sample

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/neunhoef/feed/pkg/client"
	"github.com/neunhoef/feed/pkg/database"
	"os"
)

// Doit: Example code for database/collection creation and driver usage
func Doit(endpoints []string, jwt string, username string, password string) {
	var cl driver.Client
	var err error
	if jwt != "" {
	  cl, err = client.NewClient(endpoints, driver.RawAuthentication(jwt))
	} else {
		cl, err = client.NewClient(endpoints, driver.BasicAuthentication(username , password))
	}
	if err != nil {
		fmt.Printf("Could not connect to database at %v: %v\n", endpoints, err)
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
