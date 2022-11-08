package graphgen

import (
	"math"
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type CompleteNaryTreeParameters struct {
	BranchingDegree uint64
	Depth           uint64 // length (number of edges) of a path from the root to a leaf
	DirectionType   string // "downwards" (from the root) , "upwards" (to the root), "bidirected"
	Prefix          string
}

func labelToString(label *[]uint64) string {
	labelStr := ""
	for _, n := range *label {
		labelStr += strconv.FormatUint(n, 10) + "-"
	}
	if labelStr != "" {
		// remove the last -"
		labelStr = labelStr[:len(labelStr)-1]
	}
	return labelStr
}

func putVertextoChannel(vPtr chan *datagen.Doc, label *[]uint64, prefix string) {
	var vertex datagen.Doc
	labelStr := labelToString(label)

	if prefix != "" {
		prefix += "_"
	}
	vertex.Label = prefix + labelStr
	v := vPtr
	v <- &vertex
}

func popLabel(labelPtr *[]uint64) uint64 {
	result := (*labelPtr)[len(*labelPtr)-1]
	*labelPtr = (*labelPtr)[:len(*labelPtr)-1]
	return result
}

func (t *CompleteNaryTreeParameters) MakeGraphGenerator() GraphGenerator {

	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	go func() {
		// the root
		var vertex datagen.Doc
		if len(t.Prefix) == 0 {
			vertex.Label = "eps"
		} else {
			vertex.Label = t.Prefix + "_eps"
		}
		V <- &vertex

		var edgeIndex uint64 = 0
		var first uint64
		for first = 0; first < t.BranchingDegree; first++ {
			var label []uint64 = make([]uint64, 0)
			label = append(label, first)
			putVertextoChannel(V, &label, t.Prefix)
			// edge (eps, first)
			fromLabel := "eps"
			toLabel := labelToString(&label)
			makeEdgeString(t.Prefix, edgeIndex, &fromLabel, &toLabel, &E)
			edgeIndex++
			var current uint64 = 0
			for len(label) > int(0) {
				// invariant: current is the next value to put on label
				//            label put to channel
				if (len(label) == int(t.Depth)) ||
					(current == t.BranchingDegree /* one too big*/) {
					current = popLabel(&label)
					current++
					continue
				}

				fromLabel := labelToString(&label)
				label = append(label, current)
				toLabel := labelToString(&label)
				putVertextoChannel(V, &label, t.Prefix)
				makeEdgeString(t.Prefix, edgeIndex, &fromLabel, &toLabel, &E)
				edgeIndex++
				current = 0
			}
		}
		close(V)
		close(E)
	}()

	numVertices := uint64(math.Pow(float64(t.BranchingDegree),
		float64(t.Depth+1)) - 1)
	numEdges := numVertices - 1
	return &GraphGeneratorData{V: V, E: E,
		numberVertices: numVertices,
		numberEdges:    numEdges}
}
