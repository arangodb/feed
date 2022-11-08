package graphgen

import (
	"github.com/arangodb/feed/pkg/datagen"
)

type MultiplyEdges struct {
	Operand Generatable
	Factor  uint64
	Prefix  string
}

func (m MultiplyEdges) MakeGraphGenerator() GraphGeneratorData {
	E := make(chan *datagen.Doc, batchSize())

	proxy := m.Operand.MakeGraphGenerator()
	go func() {
		for e := range proxy.EdgeChannel() {
			for i := 0; i < int(m.Factor); i++ {
				E <- e
			}
		}
		close(E)
	}()
	return GraphGeneratorData{V: proxy.VertexChannel(), E: E,
		numberVertices: proxy.NumberVertices(),
		numberEdges:    proxy.NumberEdges() * m.Factor}
}
