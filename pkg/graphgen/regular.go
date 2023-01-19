package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type RegularParameters struct {
	Size          uint64
	Degree        uint64
	KeySize       int64
	GeneralParams GeneralParameters
}

// implements: GraphGenerator

func (c *RegularParameters) NumberVertices() uint64 {
	return c.Size
}

func (c *RegularParameters) NumberEdges() uint64 {
	return c.Size * c.Degree
}

func (c *RegularParameters) VertexChannel() chan *datagen.Doc {
	V := make(chan *datagen.Doc, 4096)
	go func() {
		var i uint64
		for i = 0; uint64(i) < c.Size; i += 1 {
			index := i + c.GeneralParams.StartIndexVertices
			label := vertexIndexToLabel(i)
			makeVertex(&c.GeneralParams.Prefix, index, &label, c.KeySize, V)
		}
		close(V)
	}()
	return V
}

var (
	maxDegree uint64   = 10
	primes    []uint64 = []uint64{101, 257, 23, 41, 43, 89, 173, 269, 113, 349}
)

func makeRegularEdge(prefix *string, edgePrefix *string, edgeLabel *string,
	edgeIndex uint64, localFromIndex uint64, localToIndex uint64,
	startIndexVertices uint64, startIndexEdges uint64,
	keySize int64, e chan *datagen.Doc) {
	edgeIndex = localFromIndex + edgeIndex
	fromIndex := localFromIndex + startIndexVertices
	toIndex := localToIndex + startIndexVertices
	fromLabel := vertexIndexToLabel(fromIndex)
	toLabel := vertexIndexToLabel(toIndex)
	makeEdge(prefix, edgePrefix, edgeIndex, edgeLabel, fromIndex, toIndex,
		&fromLabel, &toLabel, keySize, e)
}

func (c *RegularParameters) EdgeChannel() chan *datagen.Doc {
	E := make(chan *datagen.Doc, 4096)
	go func() {
		var i uint64
		d := c.Degree
		if d > maxDegree {
			d = maxDegree
		}
		for i = 0; uint64(i) < c.Size; i += 1 {
			var k uint64
			for k = 0; k < d; k += 1 {
				j := (i*primes[k] + 17) % c.Size
				// Make an edge from i to j
				edgeLabel := strconv.FormatUint(i, 10) + "_" + strconv.FormatUint(primes[k], 10)
				edgeIndex := i*d + k
				makeRegularEdge(&c.GeneralParams.Prefix, &c.GeneralParams.EdgePrefix,
					&edgeLabel, edgeIndex, i, j, c.GeneralParams.StartIndexVertices,
					c.GeneralParams.StartIndexEdges, c.KeySize, E)
			}
		}
		close(E)
	}()
	return E
}
