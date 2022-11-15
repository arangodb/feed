package graphgen

import (
	"github.com/arangodb/feed/pkg/datagen"
)

type MultiplyEdgesParameters struct {
	Operand       Generatable
	Factor        uint64
	GeneralParams GeneralParameters
}

func (m MultiplyEdgesParameters) MakeGraphGenerator() (GraphGenerator, error) {
	E := make(chan *datagen.Doc, BatchSize())

	gg, err := m.Operand.MakeGraphGenerator()
	if err != nil {
		return nil, err
	}
	go func() {
		for e := range gg.EdgeChannel() {
			for i := 0; i < int(m.Factor); i++ {
				E <- e
			}
		}
		close(E)
	}()
	return &GraphGeneratorData{V: gg.VertexChannel(), E: E,
		numberVertices: gg.NumberVertices(),
		numberEdges:    gg.NumberEdges() * m.Factor}, nil
}

var _ Generatable = &MultiplyEdgesParameters{}
