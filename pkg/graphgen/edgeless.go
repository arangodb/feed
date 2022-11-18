package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type EdgelessGraphParameters struct {
	Size          uint64 // number of vertices
	GeneralParams GeneralParameters
}

func (g *EdgelessGraphParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	if g.GeneralParams.Prefix != "" {
		g.GeneralParams.Prefix += "_"
	}

	close(E)

	if makeVertices {
		go func() {
			var i uint64
			for i = 0; i < g.Size; i += 1 {
				label := strconv.FormatUint(i, 10)
				makeVertex(&g.GeneralParams.Prefix,
					g.GeneralParams.StartIndexVertices+i, &label, V)
			}
			close(V)

		}()
	} else {
		close(V)
	}

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: g.Size, numberEdges: 0}, nil
}

var _ Generatable = &EdgelessGraphParameters{}
