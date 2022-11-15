package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type CycleGraphParameters struct {
	Length        uint64
	GeneralParams GeneralParameters
}

func vertexIndexToLabel(index uint64) string {
	return strconv.FormatUint(index, 10)
}

func makeCycleEdge(prefix *string, edgeLabel *string, localFromIndex uint64,
	localToIndex uint64, startIndexVertices uint64, startIndexEdges uint64,
	e chan *datagen.Doc) {
	edgeIndex := localFromIndex + startIndexEdges
	fromIndex := localFromIndex + startIndexVertices
	toIndex := localToIndex + startIndexVertices
	fromLabel := vertexIndexToLabel(fromIndex)
	toLabel := vertexIndexToLabel(toIndex)
	makeEdge(prefix, edgeIndex, edgeLabel, fromIndex, fromIndex,
		&fromLabel, &toLabel, e)
}

func (c *CycleGraphParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	if c.GeneralParams.Prefix != "" {
		c.GeneralParams.Prefix += "_"
	}

	if makeVertices {
		go func() { // Sender for vertices
			// Has access to c because it is a closure
			var i uint64
			for i = 0; uint64(i) < c.Length; i += 1 {
				index := i + c.GeneralParams.StartIndexVertices
				label := vertexIndexToLabel(i)
				makeVertex(&c.GeneralParams.Prefix, index, &label, V)
			}
			close(V)
		}()
	} else {
		close(V)
	}

	if makeEdges {
		go func() { // Sender for edges
			// Has access to c because it is a closure
			var i uint64
			for i = 0; uint64(i) < c.Length-1; i += 1 {
				edgeLabel := strconv.FormatUint(i, 10)
				makeCycleEdge(&c.GeneralParams.Prefix, &edgeLabel, i, i+1,
					c.GeneralParams.StartIndexVertices,
					c.GeneralParams.StartIndexEdges, E)
			}
			edgeLabel := strconv.FormatUint(c.Length-1, 10)
			makeCycleEdge(&c.GeneralParams.Prefix, &edgeLabel, c.Length-1, 0,
				c.GeneralParams.StartIndexVertices,
				c.GeneralParams.StartIndexEdges, E)

			close(E)
		}()
	} else {
		close(E)
	}

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: c.Length, numberEdges: c.Length}, nil
}

var _ Generatable = &CycleGraphParameters{}
