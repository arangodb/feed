package graphgen

import (
	"github.com/arangodb/feed/pkg/datagen"
)

type MultiplyEdges struct {
	Operand Generatable
	Factor  uint64
	Prefix  string
}

func (m MultiplyEdges) MakeGraphGenerator() (GraphGenerator, error) {
	E := make(chan *datagen.Doc, batchSize())

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
