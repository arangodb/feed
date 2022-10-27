package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/feedlang"

	"fmt"
	"strconv"
)

type TestProg struct {
	n int64
}

func (dp *TestProg) Execute() error {
	config.OutputMutex.Lock()
	c := datagen.NewCyclicGraph(4)
	v := c.VertexChannel()
	for V := range v {
		fmt.Printf("Got Vertex: %v\n", V)
	}
	e := c.EdgeChannel()
	for E := range e {
		fmt.Printf("Got Edge: %v\n", E)
	}
	config.OutputMutex.Unlock()
	return nil
}

func TestMaker(args []string) (feedlang.Program, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Expecting one integer argument!")
	}
	i, err := strconv.ParseInt(args[0], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("Could not parse integer %s: %v\n", args[0], err)
	}
	return &TestProg{n: i}, nil
}

func init() {
	if feedlang.Atoms == nil {
		feedlang.Atoms = make(map[string]feedlang.Maker, 100)
	}
	feedlang.Atoms["cyclic"] = TestMaker
}
