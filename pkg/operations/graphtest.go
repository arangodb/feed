package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/graphgen"

	"fmt"

	// "os"
	"strconv"
)

func PrintGraph(gg graphgen.GraphGenerator) {
	config.OutputMutex.Lock()
	defer config.OutputMutex.Unlock()

	vertexChannel := gg.VertexChannel()
	for vertex := range vertexChannel {
		fmt.Printf("Got Vertex: %v\n", vertex)
	}
	fmt.Printf("\n")
	edgeChannel := gg.EdgeChannel()
	for edge := range edgeChannel {
		fmt.Printf("Got Edge: %v\n", edge)
	}

}

type cycleGraphParameters graphgen.CycleGraphParameters

func (cp *cycleGraphParameters) Execute() error {
	cycleGenerator, err := (&graphgen.CycleGraphParameters{Length: cp.Length,
		GeneralParams: graphgen.GeneralParameters{Prefix: "", EdgePrefix: cp.GeneralParams.EdgePrefix, StartIndexVertices: 0, StartIndexEdges: 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		return err
	}
	PrintGraph(cycleGenerator)
	return nil
}

type pathGraphParameters graphgen.PathParameters

func (pp *pathGraphParameters) Execute() error {
	path, err := (&graphgen.PathParameters{
		Length: pp.Length, Directed: pp.Directed, GeneralParams: graphgen.GeneralParameters{
			Prefix:             pp.GeneralParams.Prefix,
			EdgePrefix:         pp.GeneralParams.EdgePrefix,
			StartIndexVertices: pp.GeneralParams.StartIndexVertices,
			StartIndexEdges:    pp.GeneralParams.StartIndexEdges}}).MakeGraphGenerator(true, true)
	if err != nil {
		return err
	}
	graphgen.PrintGraph(&path)
	return nil
}

type ProgramParameters struct {
	Name string   // the name of the program
	Args []string // the parameters for the program
}

func ProduceGraphGenerator(pp ProgramParameters) (feedlang.Program, error) {
	switch pp.Name {
	case "cycle":
		{
			if len(pp.Args) == 0 {
				return nil, fmt.Errorf("Expecting one integer argument!")
			}
			i, err := strconv.ParseUint(pp.Args[0], 10, 0)
			if err != nil {
				return nil, fmt.Errorf("Could not parse integer %s: %v\n", pp.Args, err)
			}
			return &cycleGraphParameters{Length: i}, nil
		}
	case "path":
		{
			if len(pp.Args) < 2 {
				return nil, fmt.Errorf("Expecting one integer and one boolean argument!")
			}
			length, err := strconv.ParseUint(pp.Args[0], 10, 0)
			if err != nil {
				return nil, fmt.Errorf("Could not parse integer %s: %v\n", pp.Args[0], err)
			}
			directed, err := strconv.ParseBool(pp.Args[1])
			if err != nil {
				return nil, fmt.Errorf("Could not parse boolean %s: %v\n", pp.Args[1], err)
			}
			return &pathGraphParameters{Length: length, Directed: directed}, nil
		}
	default:
		return nil, fmt.Errorf("Unknown graph type: %s\n", pp.Name)
	}

}

// func init() {
// 	if feedlang.Atoms == nil {
// 		feedlang.Atoms = make(map[string]feedlang.Maker, 100)
// 	}
// 	feedlang.Atoms["cyclic"] = TestMaker
// }
