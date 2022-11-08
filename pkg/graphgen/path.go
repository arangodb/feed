package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type PathParameters struct {
	Length   uint64 // number of edges
	Directed bool
	Prefix   string
}

func (p *PathParameters) MakeGraphGenerator() GraphGenerator {

	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	go func() {
		var i uint64
		for i = 0; i <= p.Length; i += 1 { // one more vertices than Length
			var d datagen.Doc
			if p.Prefix == "" {
				d.Label = strconv.Itoa(int(i))
			} else {
				d.Label = p.Prefix + "_" + strconv.Itoa(int(i))
			}
			V <- &d
		}
		close(V)
	}()

	go func() {
		var i uint64
		for i = 0; i < p.Length; i += 1 {
			makeEdge(p.Prefix, i, i, i+1, &E)
			if !p.Directed {
				makeEdge(p.Prefix, i, i+1, i, &E)
			}
		}
		close(E)
	}()
	return &GraphGeneratorData{V: V, E: E,
		numberVertices: p.Length + 1, numberEdges: p.Length}
}
