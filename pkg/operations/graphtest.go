package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/feedlang"
	"github.com/arangodb/feed/pkg/graphgen"

	"fmt"
	"strconv"
	"time"
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

type cycleGraphParameters struct {
	graphgen.CycleGraphParameters
	feedlang.ProgMeta
}

func (cgp *cycleGraphParameters) Lines() (int, int) {
	return cgp.StartLine, cgp.EndLine
}

func (cgp *cycleGraphParameters) SetSource(lines []string) {
	cgp.Source = lines
}

func (cgp *cycleGraphParameters) StatsOutput() []string {
	return []string{
		fmt.Sprintf("graph (cycle): Have run for %v (lines %d..%d of script)\n",
			cgp.EndTime.Sub(cgp.StartTime), cgp.StartLine, cgp.EndLine),
		fmt.Sprintf("      Start time: %v\n", cgp.StartTime),
		fmt.Sprintf("      End time  : %v\n", cgp.EndTime),
	}
}

func (cgp *cycleGraphParameters) StatsJSON() interface{} {
	cgp.Type = "CycleGraphParameters"
	return cgp
}

func (cp *cycleGraphParameters) Execute() error {
	cp.StartTime = time.Now()
	cycleGenerator, err := (&graphgen.CycleGraphParameters{Length: cp.Length,
		GeneralParams: graphgen.GeneralParameters{Prefix: "", EdgePrefix: cp.GeneralParams.EdgePrefix, StartIndexVertices: 0, StartIndexEdges: 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		cp.EndTime = time.Now()
		cp.RunTime = cp.EndTime.Sub(cp.StartTime)
		return err
	}
	PrintGraph(cycleGenerator)
	cp.EndTime = time.Now()
	cp.RunTime = cp.EndTime.Sub(cp.StartTime)
	return nil
}

type pathGraphParameters struct {
	graphgen.PathParameters
	feedlang.ProgMeta
}

func (pgp *pathGraphParameters) Lines() (int, int) {
	return pgp.StartLine, pgp.EndLine
}

func (pgp *pathGraphParameters) SetSource(lines []string) {
	pgp.Source = lines
}

func (pgp *pathGraphParameters) StatsOutput() []string {
	return []string{
		fmt.Sprintf("graph (path): Have run for %v (lines %d..%d of script)\n",
			pgp.EndTime.Sub(pgp.StartTime), pgp.StartLine, pgp.EndLine),
		fmt.Sprintf("      Start time: %v\n", pgp.StartTime),
		fmt.Sprintf("      End time  : %v\n", pgp.EndTime),
	}
}

func (pgp *pathGraphParameters) StatsJSON() interface{} {
	pgp.Type = "PathGraphParameters"
	return pgp
}

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

func ProduceGraphGenerator(pp ProgramParameters, line int) (feedlang.Program, error) {
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
			return &cycleGraphParameters{
				CycleGraphParameters: graphgen.CycleGraphParameters{Length: i},
				ProgMeta:             feedlang.ProgMeta{StartLine: line, EndLine: line}}, nil
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
			return &pathGraphParameters{
				PathParameters: graphgen.PathParameters{
					Length:   length,
					Directed: directed,
				},
				ProgMeta: feedlang.ProgMeta{
					StartLine: line,
					EndLine:   line,
				}}, nil
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
