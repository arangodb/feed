package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type EdgelessGraph struct {
	Size   uint64 // number of vertices
	Prefix string
}

func (g EdgelessGraph) MakeGraphGenerator() (GraphGenerator, error) {

	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	var prefix string = ""
	if g.Prefix != "" {
		prefix = g.Prefix + "_"
	}

	close(E)

	go func() {
		var i uint64
		for i = 0; i < g.Size; i += 1 {
			var d datagen.Doc
			d.Label = prefix + strconv.Itoa(int(i))
			V <- &d
		}
		close(V)

	}()

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: g.Size, numberEdges: 0}, nil
}
