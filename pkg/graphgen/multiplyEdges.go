package graphgen

import (
	"github.com/arangodb/feed/pkg/datagen"
)

type MultiplyEdgesParameters struct {
	Operand       Generatable
	Factor        uint64
	GeneralParams GeneralParameters
}

func (m MultiplyEdgesParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	gg, err := m.Operand.MakeGraphGenerator(makeVertices, makeEdges)
	if err != nil {
		return nil, err
	}

	if makeVertices {
		go func() {
			for v := range gg.VertexChannel() {
				V <- v
			}
			close(V)
		}()
	} else {
		close(V)
	}

	if makeEdges {
		go func() {
			for e := range gg.EdgeChannel() {
				for i := 0; i < int(m.Factor); i++ {
					E <- e
				}
			}
			close(E)
		}()
	} else {
		close(E)
	}

	return &GraphGeneratorData{V: gg.VertexChannel(), E: E,
		numberVertices: gg.NumberVertices(),
		numberEdges:    gg.NumberEdges() * m.Factor}, nil
}

var _ Generatable = &MultiplyEdgesParameters{}
