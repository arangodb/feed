package graphgen

import (
	"fmt"
	// "reflect"

	"github.com/arangodb/feed/pkg/datagen"
)

type LexicographicalProductParameters struct {
	Left   Generatable
	Right  Generatable
	Prefix string
}

type edgeLabels struct {
	from string
	to   string
}

// We assume that Right is small and wait until we obtain the whole graph Right.
func (u LexicographicalProductParameters) MakeGraphGenerator() GraphGenerator {
	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	p1 := u.Left.MakeGraphGenerator()
	p2 := u.Right.MakeGraphGenerator()

	var edgesRight []edgeLabels
	// read all edges from the second graph
	for e := range p2.EdgeChannel() {
		pair := edgeLabels{e.FromLabel, e.ToLabel}
		edgesRight = append(edgesRight, pair)
	}

	var labelsVerticesRight []string
	// read all vertices from the second graph
	for v := range p2.VertexChannel() {
		labelsVerticesRight = append(labelsVerticesRight, v.Label)
	}

	// make vertices
	VLabels := make(chan *string, batchSize()) // copy vertex labels for future
	go func() {
		for v := range p1.VertexChannel() {
			VLabels <- &(v.Label)
			// produce all vertices with first element v
			for _, rightLabel := range labelsVerticesRight {
				productVertex := v
				productVertex.Label = fmt.Sprintf("(%s,%s)",
					productVertex.Label, rightLabel)
				V <- productVertex
			}
		}
		close(VLabels)
		close(V)
	}()

	// make edges
	go func() {
		var countEdges uint64
		// edges between super-vertices (v and w replaced by copies of p2)
		for e := range p1.EdgeChannel() {
			for _, labelVLeft := range labelsVerticesRight {
				for _, labelVRight := range labelsVerticesRight {
					newFromLabel := fmt.Sprintf("(%s,%s)", e.FromLabel, labelVLeft)
					newToLabel := fmt.Sprintf("(%s,%s)", e.ToLabel, labelVRight)

					makeEdgeString(u.Prefix, countEdges, &newFromLabel,
						&newToLabel, &E)
					countEdges++
				}
			}
		}

		// edges within super-vertices (v replaced by a copy of p2)
		for vLabel := range VLabels {
			for _, eLabels := range edgesRight {
				newFromLabel := fmt.Sprintf("(%s,%s)", *vLabel, eLabels.from)
				newToLabel := fmt.Sprintf("(%s,%s)", *vLabel, eLabels.to)

				makeEdgeString(u.Prefix, countEdges, &newFromLabel,
					&newToLabel, &E)
				countEdges++
			}
		}
		close(E)
	}()

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: p1.NumberVertices() * p2.NumberVertices(),
		numberEdges: p1.NumberEdges()*p2.NumberVertices()*p2.NumberVertices() +
			p1.NumberVertices()*p2.NumberEdges()}
}
