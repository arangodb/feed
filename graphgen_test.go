package main

import (
	"io/ioutil"
	"log"

	"math"

	"strings"

	// "reflect"
	"testing"

	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/graphgen"
	// "github.com/arangodb/feed/pkg/operations"
)

func numberElementsInChannel(c chan *datagen.Doc) (uint64, chan *datagen.Doc) {
	v := make(chan *datagen.Doc, 1000)
	var numElements uint64 = 0
	for element := range c {
		numElements++
		v <- element
	}
	close(v)
	return numElements, v
}

func TestCycleGeneration(t *testing.T) {
	var length int = 5
	lengthParameter := uint64(length)
	cycleGenerator := (&graphgen.CycleGraphParameters{lengthParameter}).MakeGraphGenerator()

	numVertices, _ := numberElementsInChannel(cycleGenerator.VertexChannel())
	if numVertices != lengthParameter {
		t.Fatalf("Wrong number of vertices. Expected: %#v, obtained: %#v",
			length, numVertices)
	}

	numEdges, _ := numberElementsInChannel(cycleGenerator.EdgeChannel())
	if numEdges != lengthParameter {
		t.Fatalf("Wrong number of edges. Expected: %#v, obtained: %#v",
			length, numEdges)
	}
}

func TestDirectedPathGeneration(t *testing.T) {
	var length int = 5
	var directed bool = true
	lengthParameter := uint64(length)
	pathGenerator := (&graphgen.PathParameters{lengthParameter, directed, ""}).MakeGraphGenerator()

	numVertices, _ := numberElementsInChannel(pathGenerator.VertexChannel())
	if numVertices != lengthParameter+1 {
		t.Fatalf("Wrong number of vertices. Expected: %#v, obtained: %#v",
			length+1, numVertices)
	}

	numEdges, _ := numberElementsInChannel(pathGenerator.EdgeChannel())
	if numEdges != lengthParameter {
		t.Fatalf("Wrong number of edges. Expected: %#v, obtained: %#v",
			length, numEdges)
	}
}

func TestUndirectedPathGeneration(t *testing.T) {
	var length int = 5
	var directed bool = false
	lengthParameter := uint64(length)
	pathGenerator := (&graphgen.PathParameters{lengthParameter, directed, ""}).MakeGraphGenerator()

	numVertices, _ := numberElementsInChannel(pathGenerator.VertexChannel())

	if numVertices != lengthParameter+1 {
		t.Fatalf("Wrong number of vertices. Expected: %#v, obtained: %#v",
			length+1, numVertices)
	}

	numEdges, _ := numberElementsInChannel(pathGenerator.EdgeChannel())
	if numEdges != 2*lengthParameter {
		t.Fatalf("Wrong number of edges. Expected: %#v, obtained: %#v",
			length, numEdges)
	}
}

func TestPrintUnionPathPathGeneration(t *testing.T) {
	var length int = 5
	var directed bool = true
	lengthParameter := uint64(length)
	unionGenerator := graphgen.UnionParameters{
		&graphgen.PathParameters{lengthParameter, directed, "a"},
		&graphgen.PathParameters{lengthParameter, directed, "b"},
		""}.MakeGraphGenerator()

	// operations.PrintGraph(pathGenerator)
	numVertices, _ := numberElementsInChannel(unionGenerator.VertexChannel())
	expectedNumberVertices := 2 * (lengthParameter + 1)
	if numVertices != expectedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %#v, obtained: %#v",
			expectedNumberVertices, numVertices)
	}

}

func TestExpectedNumberVerticesPath(t *testing.T) {
	var length uint64 = 3
	pathGenerator := (&graphgen.PathParameters{length, true, ""}).MakeGraphGenerator()
	expectedNumberVertices := pathGenerator.NumberVertices()
	if expectedNumberVertices != length+1 {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, length+1)
	}
}

func TestExpectedNumberVerticesTree(t *testing.T) {
	var branchingDegree uint64 = 3
	var depth uint64 = 3
	var directionType string = "downwards"
	var prefix string = ""
	treeGenerator := (&graphgen.CompleteNaryTreeParameters{branchingDegree,
		depth, directionType, prefix}).MakeGraphGenerator()
	obtainedNumberVertices := treeGenerator.NumberVertices()
	expectedNumberVertices := uint64(math.Pow(float64(branchingDegree), float64(depth+1)) - 1)
	if expectedNumberVertices != obtainedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, obtainedNumberVertices)
	}
}

func TestExpectedNumberVerticesUnionPathPath(t *testing.T) {
	var length uint64 = 5
	var directed bool = true
	unionGenerator := graphgen.UnionParameters{
		&graphgen.PathParameters{length, directed, "a"},
		&graphgen.PathParameters{length, directed, "b"},
		""}.MakeGraphGenerator()

	obtainedNumberVertices := unionGenerator.NumberVertices()
	expectedNumberVertices := 2 * (length + 1)
	if expectedNumberVertices != obtainedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, obtainedNumberVertices)
	}
}

func TestTreeHasNo__(t *testing.T) {
	var branchingDegree uint64 = 3
	var depth uint64 = 3
	var directionType string = "downwards"
	var prefix string = ""
	treeGenerator := (&graphgen.CompleteNaryTreeParameters{branchingDegree,
		depth, directionType, prefix}).MakeGraphGenerator()

	count := 0
	for v := range treeGenerator.VertexChannel() {
		label := v.Label
		if strings.Contains(label, "__") {
			t.Fatalf("The label of the vertex number %d contains \"__\", label:%v, ",
				count, label)
		}
	}
}

func TestPathHasNo__(t *testing.T) {
	var length uint64 = 1
	var directed bool = true
	var prefix string = ""
	pathGenerator := (&graphgen.PathParameters{length, directed, prefix}).MakeGraphGenerator()

	count := 0
	for v := range pathGenerator.VertexChannel() {
		label := v.Label
		if strings.Contains(label, "__") {
			t.Fatalf("The label of the vertex number %d contains \"__\", label:%v, ",
				count, label)
		}
	}
}

func TestUnionHasNo__(t *testing.T) {
	var length uint64 = 5
	var directed bool = true
	unionGenerator := graphgen.UnionParameters{
		&graphgen.PathParameters{length, directed, "a"},
		&graphgen.PathParameters{length, directed, "b"},
		""}.MakeGraphGenerator()

	count := 0
	for v := range unionGenerator.VertexChannel() {
		label := v.Label
		if strings.Contains(label, "__") {
			t.Fatalf("The label of the vertex number %d contains \"__\", label:%v, ",
				count, label)
		}
	}
}

func TestLexProductHasNo__(t *testing.T) {
	var length uint64 = 5
	directed := true
	lpGenerator := graphgen.LexicographicalProductParameters{
		&graphgen.PathParameters{length, directed, "a"},
		&graphgen.PathParameters{length, directed, "b"},
		""}.MakeGraphGenerator()

	count := 0
	for v := range lpGenerator.VertexChannel() {
		label := v.Label
		if strings.Contains(label, "__") {
			t.Fatalf("The label of the vertex number %d contains \"__\", label:%v, ",
				count, label)
		}
	}
}

func TestExpectedNumberVerticesLexProdPathPath(t *testing.T) {
	var length uint64 = 2
	var directed bool = true
	lpGenerator := graphgen.LexicographicalProductParameters{
		&graphgen.PathParameters{length, directed, "a"},
		&graphgen.PathParameters{length, directed, "b"},
		""}.MakeGraphGenerator()

	expectedNumberVertices := (length + 1) * (length + 1)

	actualNumberVertices, _ := numberElementsInChannel(lpGenerator.VertexChannel())
	if expectedNumberVertices != actualNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, actualNumberVertices)
	}

	obtainedNumberVertices := lpGenerator.NumberVertices()
	if expectedNumberVertices != obtainedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, obtainedNumberVertices)
	}
}

func TestPrintReadJSONTree(t *testing.T) {
	const filename = "tree.json"
	buf, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Panicf("Could not read from file %s, error: %v", filename, err)
	}
	gg := graphgen.JSON2Graph(buf)
	actualNumVertices, _ := numberElementsInChannel(gg.VertexChannel())
	obtainedNumberVertices := gg.NumberVertices()
	expectedNumberVertices := uint64(15) // branchingDegree: 2, depth: 3

	actualNumEdges, _ := numberElementsInChannel(gg.EdgeChannel())
	obtainedNumEdges := gg.NumberEdges()
	expectedNumEdges := uint64(14) // branchingDegree: 2, depth: 3

	if actualNumVertices != expectedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, actual: %v",
			expectedNumberVertices, actualNumVertices)
	}
	if obtainedNumberVertices != expectedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, obtainedNumberVertices)
	}

	if actualNumEdges != expectedNumEdges {
		t.Fatalf("Wrong number of edges. Expected: %v, actual: %v",
			expectedNumEdges, actualNumEdges)
	}
	if obtainedNumEdges != expectedNumEdges {
		t.Fatalf("Wrong number of edges. Expected: %v, obtained: %v",
			expectedNumEdges, obtainedNumEdges)
	}

}

func TestPrintReadJSONLexProdUnionTreePathTree(t *testing.T) {
	const filename = "lexProdUnionTreePathTree.json"
	// {
	// "lexProduct": [
	// 	{"union": [
	// 		{"tree": {"branchingDegree": 2, "depth": 1, "directionType": "downwards"}},
	// 		{"path": {"length": 3, "directed": true}}
	// 	]},
	// 	{"tree": {"branchingDegree": 2, "depth": 1, "directionType": "upwards"}}
	// ]

	// }

	buf, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Panicf("Could not read from file %s, error: %v", filename, err)
	}
	gg := graphgen.JSON2Graph(buf)

	actualNumVertices, _ := numberElementsInChannel(gg.VertexChannel())
	obtainedNumberVertices := gg.NumberVertices()
	expectedNumberVertices := uint64(21) // branchingDegree: 2, depth: 3

	actualNumEdges, _ := numberElementsInChannel(gg.EdgeChannel())
	obtainedNumEdges := gg.NumberEdges()
	expectedNumEdges := uint64(59) // branchingDegree: 2, depth: 3

	if actualNumVertices != expectedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, actual: %v",
			expectedNumberVertices, actualNumVertices)
	}
	if obtainedNumberVertices != expectedNumberVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumberVertices, obtainedNumberVertices)
	}

	if actualNumEdges != expectedNumEdges {
		t.Fatalf("Wrong number of edges. Expected: %v, actual: %v",
			expectedNumEdges, actualNumEdges)
	}
	if obtainedNumEdges != expectedNumEdges {
		t.Fatalf("Wrong number of edges. Expected: %v, obtained: %v",
			expectedNumEdges, obtainedNumEdges)
	}
}
