package operations

import (
	"github.com/neunhoef/feed/pkg/feedlang"
)

type NormalProg struct {
	// General parameters:
  Database string
	Collection string
	SubCommand string

	// Parameters for creation:
	NumberOfShards int64
	ReplicationFactor int64

	// Parameters for batch import:
	SizePerDoc int64
	Size int64
}

func NewNormalProg(args []string) (NormalProg, error) {
	// This function parses the command line args and fills the values in
	// the struct.

}

func (p *NormalProg) Execute() error {
  // This actually executes the NormalProg:

}

