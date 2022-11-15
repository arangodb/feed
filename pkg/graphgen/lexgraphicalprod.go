package graphgen

import (
	"fmt"
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type LexicographicalProductParameters struct {
	Left          Generatable
	Right         Generatable
	GeneralParams GeneralParameters
}

type vertexInfo struct {
	index uint64
	label string
}

type edgeLabels struct {
	fromIndex uint64
	toIndex   uint64
	fromLabel string
	toLabel   string
}

// We assume that Right is small and wait until we obtain the whole graph Right.
func (lp *LexicographicalProductParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	if lp.GeneralParams.Prefix != "" {
		lp.GeneralParams.Prefix += "_"
	}

	p1, errLeft := lp.Left.MakeGraphGenerator(makeVertices, makeEdges)
	if errLeft != nil {
		return nil, errLeft
	}
	p2, errRight := lp.Right.MakeGraphGenerator(true, makeEdges)
	if errRight != nil {
		return nil, errRight
	}

	var verticesRight []vertexInfo
	// read all vertices from the second graph
	for v := range p2.VertexChannel() {
		verticesRight = append(verticesRight, vertexInfo{index: v.Index, label: v.Label})
	}

	// make vertices

	VInfos := make(chan vertexInfo, BatchSize()) // copy vertex labels for future
	if makeVertices {
		var vertexIndex uint64 = lp.GeneralParams.StartIndexVertices
		go func() {
			for v := range p1.VertexChannel() {
				if makeEdges {
					VInfos <- vertexInfo{index: v.Index, label: v.Label}
				}
				// produce all vertices with first element v
				for _, rightVertex := range verticesRight {
					label := fmt.Sprintf("(%s,%s),", v.Label, rightVertex.label)
					makeVertex(&lp.GeneralParams.Prefix, vertexIndex, &label, V)
					vertexIndex++
				}
			}

			close(VInfos)
			close(V)
		}()
	} else {
		close(V)
	}

	// make edges
	if makeEdges {
		var edgesRight []edgeLabels
		// read all edges from the second graph
		for e := range p2.EdgeChannel() {
			eInfo := edgeLabels{e.FromIndex, e.ToIndex, e.FromLabel, e.ToLabel}
			edgesRight = append(edgesRight, eInfo)
		}

		go func() {
			var countEdges uint64 = lp.GeneralParams.StartIndexEdges
			// edges between super-vertices (v and w replaced by copies of p2)
			for e := range p1.EdgeChannel() {
				for _, vLeft := range verticesRight { // (vLeft and verticesRight) is correct
					for _, vRight := range verticesRight {
						newFromIndex := (e.FromIndex-lp.GeneralParams.StartIndexVertices)*p2.NumberVertices() +
							(vLeft.index - lp.GeneralParams.StartIndexVertices - p1.NumberVertices()) + lp.GeneralParams.StartIndexVertices
						newFromLabel := fmt.Sprintf("(%s,%s)", e.FromLabel, vLeft.label)
						newToIndex := (e.ToIndex-lp.GeneralParams.StartIndexVertices)*p2.NumberVertices() +
							(vRight.index - lp.GeneralParams.StartIndexVertices - p1.NumberVertices()) + lp.GeneralParams.StartIndexVertices
						newToLabel := fmt.Sprintf("(%s,%s)", e.ToLabel, vRight.label)
						edgeLabel := strconv.FormatUint(countEdges, 10)
						makeEdge(&lp.GeneralParams.Prefix, countEdges, &edgeLabel,
							newFromIndex, newToIndex, &newFromLabel, &newToLabel, E)
						countEdges++
					}
				}
			}

			// edges within super-vertices (v replaced by a copy of p2)
			for v := range VInfos {
				for _, eLabels := range edgesRight {
					newFromIndex := v.index + eLabels.fromIndex
					newFromLabel := fmt.Sprintf("(%s,%s)", v.label, eLabels.fromLabel)
					newToIndex := v.index + eLabels.toIndex
					newToLabel := fmt.Sprintf("(%s,%s)", v.label, eLabels.toLabel)
					edgeLabel := strconv.FormatUint(countEdges, 10)
					makeEdge(&lp.GeneralParams.Prefix, countEdges, &edgeLabel,
						newFromIndex, newToIndex, &newFromLabel, &newToLabel, E)
					countEdges++
				}
			}
			close(E)
		}()
	} else {
		close(E)
	}

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: p1.NumberVertices() * p2.NumberVertices(),
		numberEdges: p1.NumberEdges()*p2.NumberVertices()*p2.NumberVertices() +
			p1.NumberVertices()*p2.NumberEdges()}, nil
}

var _ Generatable = &LexicographicalProductParameters{}
