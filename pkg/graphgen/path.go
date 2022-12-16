package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type PathParameters struct {
	Length        uint64 // number of edges
	Directed      bool
	GeneralParams GeneralParameters
}

func (p *PathParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	if p.GeneralParams.Prefix != "" {
		p.GeneralParams.Prefix += "_"
	}

	if makeVertices {
		go func() {
			var i uint64
			for i = 0; i <= p.Length; i += 1 { // one more vertices than Length
				label := strconv.FormatUint(i, 10)
				makeVertex(&p.GeneralParams.Prefix,
					p.GeneralParams.StartIndexVertices+i, &label, V)
			}
			close(V)
		}()
	} else {
		close(V)
	}

	if makeEdges {
		go func() {
			var i uint64
			edgeIndex := p.GeneralParams.StartIndexEdges
			for i = 0; i < p.Length; i += 1 {
				edgeLabel := strconv.FormatUint(i, 10)
				globalFromIndex := p.GeneralParams.StartIndexVertices + i
				globalToIndex := p.GeneralParams.StartIndexVertices + i + 1
				fromLabel := strconv.FormatUint(i, 10)
				toLabel := strconv.FormatUint(i+1, 10)

				makeEdge(&p.GeneralParams.Prefix, &p.GeneralParams.EdgePrefix,
					edgeIndex, &edgeLabel, globalFromIndex, globalToIndex, &fromLabel,
					&toLabel, E)
				edgeIndex++
				if !p.Directed {
					edgeLabel = strconv.FormatUint(edgeIndex, 10)
					makeEdge(&p.GeneralParams.Prefix, &p.GeneralParams.EdgePrefix,
						edgeIndex, &edgeLabel, globalToIndex, globalFromIndex, &toLabel,
						&fromLabel, E)
					edgeIndex++
				}
			}
			close(E)
		}()
	} else {
		close(E)
	}

	var numEdges uint64
	if p.Directed {
		numEdges = p.Length
	} else {
		numEdges = 2 * p.Length
	}

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: p.Length + 1, numberEdges: numEdges}, nil
}

var _ Generatable = &PathParameters{}
