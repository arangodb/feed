package graphgen

import (
	"fmt"
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type CollatzParameters struct {
	Size          uint64
	GeneralParams GeneralParameters
}

// implements: GraphGenerator

func (c *CollatzParameters) NumberVertices() uint64 {
	return c.Size
}

func (c *CollatzParameters) NumberEdges() uint64 {
	return c.Size
}

func (c *CollatzParameters) VertexChannel() chan *datagen.Doc {
	V := make(chan *datagen.Doc, 4096)
	go func() {
		var i uint64
		for i = 0; uint64(i) < c.Size; i += 1 {
			index := i + c.GeneralParams.StartIndexVertices
			label := vertexIndexToLabel(i)
			makeVertex(&c.GeneralParams.Prefix, index, &label, V)
		}
		close(V)
	}()
	return V
}

func (c *CollatzParameters) EdgeChannel() chan *datagen.Doc {
	E := make(chan *datagen.Doc, 4096)
	go func() {
		var i uint64
		for i = 0; uint64(i) < c.Size; i += 1 {
			var j uint64
			if i&1 == 0 {
				j = i / 2
			} else {
				j = 3*i + 1
			}
			if j < c.Size {
				// Make an edge from i to j
				edgeLabel := strconv.FormatUint(i, 10)
				makeCycleEdge(&c.GeneralParams.Prefix, &c.GeneralParams.EdgePrefix,
					&edgeLabel, i, j, c.GeneralParams.StartIndexVertices,
					c.GeneralParams.StartIndexEdges, E)
				fmt.Printf("Making an edge from %d to %d.\n", i, j)
			}
		}
		close(E)
	}()
	return E
}
