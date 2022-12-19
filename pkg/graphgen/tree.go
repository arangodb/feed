package graphgen

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

const (
	DirectionDown string = "downwards"
	DirectionUp   string = "upwards"
	DirectionBi   string = "bidirected"
)

func Pow(a uint64, b uint64) uint64 {
	var x uint64 = 1
	var aa uint64 = a
	for b != 0 {
		if b&1 != 0 {
			x *= aa
		}
		b = b >> 1
		aa = aa * aa
	}
	return x
}

type Tree struct {
	BranchingDegree uint64
	Depth           uint64
	DirectionType   string
	GeneralParams   GeneralParameters
}

func (t *Tree) NumVertices() uint64 {
	return Pow(t.BranchingDegree, t.Depth+1) / (t.BranchingDegree - 1)
}

func labelFromInt(i uint64, b uint64) string {
	if i == 0 {
		return "eps"
	}
	return labelFromInt((i-1)/b, b) + "-" + strconv.FormatInt(int64((i-1)%b), 10)
}

func (t *Tree) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	if t.BranchingDegree < 2 {
		return nil, errors.New(fmt.Sprintf("Wrong argument to tree MakeGraphGenerator: %d; BranchingDegree should be at least 2.",
			t.BranchingDegree))
	}
	if t.Depth == 0 {
		return nil, errors.New("Wrong argument to tree MakeGraphGenerator: Depth cannot be 0.")
	}

	numVertices := t.NumVertices()
	numEdges := numVertices - 1
	if t.DirectionType == "bidirected" {
		numEdges = numEdges * 2
	}

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	if makeVertices {
		go func() {
			var vertexIndex uint64 = t.GeneralParams.StartIndexVertices
			var i uint64
			for i = 0; i < numVertices; i += 1 {
				label := labelFromInt(i, t.BranchingDegree)
				makeVertex(&t.GeneralParams.Prefix, vertexIndex, &label, V)
				vertexIndex += 1
			}
			close(V)
		}()
	}

	b := t.BranchingDegree
	if makeEdges {
		go func() {
			var vertexIndex uint64 = t.GeneralParams.StartIndexVertices
			var edgeIndex uint64 = t.GeneralParams.StartIndexEdges
			var i uint64
			labelEdge := "Karl"
			for i = 0; i < numVertices-Pow(b, t.Depth); i += 1 {
				labelFrom := labelFromInt(i, b)
				var j uint64
				for j = 1; j <= b; j += 1 {
					labelTo := labelFromInt(i*b+j, b)
					addEdge(&t.DirectionType, &t.GeneralParams.Prefix,
						&t.GeneralParams.EdgePrefix, &edgeIndex, &labelEdge,
						vertexIndex, vertexIndex*b+j, &labelFrom, &labelTo, E)
					edgeIndex += 1
				}
				vertexIndex += 1
			}
			close(E)
		}()
	}

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: numVertices,
		numberEdges:    numEdges}, nil
}

type CompleteNaryTreeParameters struct {
	BranchingDegree uint64
	Depth           uint64 // length (number of edges) of a path from the root to a leaf
	DirectionType   string // "downwards" (from the root) , "upwards" (to the root), "bidirected"
	GeneralParams   GeneralParameters
}

func (t *CompleteNaryTreeParameters) NumVertices() uint64 {
	return Pow(t.BranchingDegree, t.Depth+1) / (t.BranchingDegree - 1)
}

func labelToString(stack *[]stackElem) string {
	labelStr := ""
	for _, elem := range *stack {
		labelStr += strconv.FormatUint(elem.labelInt, 10) + "-"
	}
	if labelStr != "" {
		// remove the last -"
		labelStr = labelStr[:len(labelStr)-1]
	}
	return labelStr
}

func popLabel(stackPtr *[]stackElem) stackElem {
	result := (*stackPtr)[len(*stackPtr)-1]
	*stackPtr = (*stackPtr)[:len(*stackPtr)-1]
	return result
}

func addEdge(directionType *string, prefix *string, edgePrefix *string,
	edgeIndex *uint64, edgeLabel *string, globalFromIndex uint64,
	globalToIndex uint64, fromLabel *string, toLabel *string,
	e chan *datagen.Doc) {

	switch *directionType {
	case DirectionDown:
		makeEdge(prefix, edgePrefix, *edgeIndex, edgeLabel, globalFromIndex, globalToIndex,
			fromLabel, toLabel, e)
	case DirectionUp:
		makeEdge(prefix, edgePrefix, *edgeIndex, edgeLabel, globalToIndex, globalFromIndex,
			toLabel, fromLabel, e)
	case DirectionBi:
		{
			makeEdge(prefix, edgePrefix, *edgeIndex, edgeLabel, globalFromIndex,
				globalToIndex, fromLabel, toLabel, e)
			*edgeIndex++
			makeEdge(prefix, edgePrefix, *edgeIndex, edgeLabel, globalToIndex,
				globalFromIndex, toLabel, fromLabel, e)
		}
	}

}

type stackElem struct {
	labelInt uint64
	indexInt uint64
}

func (t *CompleteNaryTreeParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	if t.BranchingDegree < 2 {
		return nil, errors.New(fmt.Sprintf("Wrong argument to tree MakeGraphGenerator: %d; BranchingDegree should be at least 2.",
			t.BranchingDegree))
	}
	if t.Depth == 0 {
		return nil, errors.New("Wrong argument to tree MakeGraphGenerator: Depth cannot be 0.")
	}

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	go func() {
		// the root
		var rootPrefix string
		if len(t.GeneralParams.Prefix) == 0 {
			rootPrefix = "eps"
		} else {
			rootPrefix = t.GeneralParams.Prefix + "_eps"
		}
		rootLabel := ""
		var vertexIndex uint64 = t.GeneralParams.StartIndexVertices
		if makeVertices {
			makeVertex(&rootPrefix, vertexIndex, &rootLabel, V)
		}
		vertexIndex++

		if t.GeneralParams.Prefix != "" {
			t.GeneralParams.Prefix += "_"
		}

		var edgeIndex uint64 = t.GeneralParams.StartIndexEdges
		var first uint64
		for first = 0; first < t.BranchingDegree; first++ {

			var stack []stackElem = make([]stackElem, 0)

			// add first to vertex channel
			stack = append(stack, stackElem{first, vertexIndex})
			labelStr := labelToString(&stack)
			if makeVertices {
				makeVertex(&t.GeneralParams.Prefix, vertexIndex, &labelStr, V)
			}

			// add edge (eps, first) to edge channel
			if makeEdges {
				fromLabel := "eps"
				toLabel := labelToString(&stack)
				addEdge(&t.DirectionType, &t.GeneralParams.Prefix,
					&t.GeneralParams.EdgePrefix, &edgeIndex,
					&labelStr, t.GeneralParams.StartIndexVertices, vertexIndex,
					&fromLabel, &toLabel, E)
			}
			vertexIndex++
			edgeIndex++

			// dfs on the tree adding vertices and edges
			var currentLabelElem uint64 = 0
			var currentIndexElem uint64
			for len(stack) > int(0) {
				// invariant: current is the next value to put on stack
				//            stack.labelInt put to channel
				if (len(stack) == int(t.Depth)) ||
					(currentLabelElem == t.BranchingDegree /* one too big*/) {
					current := popLabel(&stack)
					currentLabelElem = current.labelInt
					currentLabelElem++
					currentIndexElem = current.indexInt
					continue
				}

				stack = append(stack, stackElem{labelInt: currentLabelElem,
					indexInt: currentIndexElem})
				fromLabel := labelToString(&stack)
				fromIndex := stack[len(stack)-1].indexInt
				toLabel := labelToString(&stack)
				if makeVertices {
					makeVertex(&t.GeneralParams.Prefix, vertexIndex, &toLabel, V)
				}
				if makeEdges {
					toIndex := currentIndexElem
					addEdge(&t.DirectionType, &t.GeneralParams.Prefix,
						&t.GeneralParams.EdgePrefix, &edgeIndex,
						&toLabel, fromIndex, toIndex, &fromLabel, &toLabel, E)
				}
				edgeIndex++
				vertexIndex++
				currentLabelElem = 0
			}
		}
		close(V)
		close(E)
	}()

	numVertices := t.NumVertices()
	numEdges := numVertices - 1
	if t.DirectionType == "bidirected" {
		numEdges = numEdges * 2
	}
	return &GraphGeneratorData{V: V, E: E,
		numberVertices: numVertices,
		numberEdges:    numEdges}, nil
}

var _ Generatable = &CompleteNaryTreeParameters{}
