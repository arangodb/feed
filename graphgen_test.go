package main

import (
	"io/ioutil"
	"log"
	"strings"

	"testing"

	"github.com/arangodb/feed/pkg/datagen"
	"github.com/arangodb/feed/pkg/graphgen"
)

func numberElementsInChannel(c chan *datagen.Doc) (uint64, chan *datagen.Doc) {
	v := make(chan *datagen.Doc, graphgen.BatchSize())
	var numElements uint64 = 0
	for element := range c {
		numElements++
		v <- element
	}
	close(v)
	return numElements, v
}

func testExpectedNumberVerticesEdges(t *testing.T, gg graphgen.GraphGenerator,
	expectedNumVertices uint64, expectedNumEdges uint64) {

	actualNumVertices, _ := numberElementsInChannel(gg.VertexChannel())
	obtainedNumVertices := gg.NumberVertices()

	actualNumEdges, _ := numberElementsInChannel(gg.EdgeChannel())
	obtainedNumEdges := gg.NumberEdges()

	if actualNumVertices != expectedNumVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, actual: %v",
			expectedNumVertices, actualNumVertices)
	}
	if obtainedNumVertices != expectedNumVertices {
		t.Fatalf("Wrong number of vertices. Expected: %v, obtained: %v",
			expectedNumVertices, obtainedNumVertices)
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

func TestCycleGeneration(t *testing.T) {
	var length uint64 = 5
	cycleGenerator, err := (&graphgen.CycleGraphParameters{length, 64,
		graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.CycleGraphParameters: ", err)
	}

	testExpectedNumberVerticesEdges(t, cycleGenerator, length, length)
}

func TestDirectedPathGeneration(t *testing.T) {
	var length uint64 = 5
	var directed bool = true
	pathGenerator, err := (&graphgen.PathParameters{
		length, directed, 64, graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.PathParameters: ", err)
	}

	testExpectedNumberVerticesEdges(t, pathGenerator, length+1, length)
}

func TestUndirectedPathGeneration(t *testing.T) {
	var length uint64 = 5
	var directed bool = false
	pathGenerator, err := (&graphgen.PathParameters{
		length, directed, 64, graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.PathParameters: ", err)
	}

	testExpectedNumberVerticesEdges(t, pathGenerator, length+1, 2*length)
}

func TestUnionPathPathGeneration(t *testing.T) {
	var length uint64 = 5
	var directed bool = true
	unionGenerator, err := (&graphgen.UnionParameters{
		&graphgen.PathParameters{length, directed, 64, graphgen.GeneralParameters{
			"a", "", 0, 0}},
		&graphgen.PathParameters{length, directed, 64, graphgen.GeneralParameters{
			"b", "", length + 1, length}},
		graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.UnionParameters or in graphgen.PathParameters: ", err)
	}

	expectedNumberVertices := 2 * (length + 1)
	expectedNumberEdges := 2 * length
	testExpectedNumberVerticesEdges(t, unionGenerator, expectedNumberVertices,
		expectedNumberEdges)
}

func TestDirectedTreeGeneration(t *testing.T) {
	var branchingDegree uint64 = 3
	var depth uint64 = 2
	var directionType string = graphgen.DirectionDown
	var prefix string = ""
	treeGenerator, err := (&graphgen.Tree{
		BranchingDegree: branchingDegree,
		Depth:           depth,
		DirectionType:   directionType,
		GeneralParams: graphgen.GeneralParameters{
			Prefix:             prefix,
			EdgePrefix:         "E/",
			StartIndexVertices: 0,
			StartIndexEdges:    0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Errorf("Error in graphgen.Tree: %v", err)
	}

	expectedNumberVertices :=
		(graphgen.Pow(branchingDegree, depth+1) - 1) / (branchingDegree - 1)

	testExpectedNumberVerticesEdges(t, treeGenerator, expectedNumberVertices,
		expectedNumberVertices-1)
}

func TestUndirectedTreeGeneration(t *testing.T) {
	var branchingDegree uint64 = 3
	var depth uint64 = 2
	var directionType string = graphgen.DirectionBi
	var prefix string = ""
	treeGenerator, err := (&graphgen.Tree{
		BranchingDegree: branchingDegree,
		Depth:           depth,
		DirectionType:   directionType,
		GeneralParams: graphgen.GeneralParameters{
			Prefix:             prefix,
			EdgePrefix:         "E/",
			StartIndexVertices: 0,
			StartIndexEdges:    0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Errorf("Error in graphgen.Tree: %v", err)
	}

	expectedNumberVertices :=
		(graphgen.Pow(branchingDegree, depth+1) - 1) / (branchingDegree - 1)

	testExpectedNumberVerticesEdges(t, treeGenerator, expectedNumberVertices,
		2*(expectedNumberVertices-1))
}

func TestTreeHasNo__(t *testing.T) {
	var branchingDegree uint64 = 3
	var depth uint64 = 3
	var directionType string = "downwards"
	var prefix string = ""
	treeGenerator, err := (&graphgen.CompleteNaryTreeParameters{branchingDegree,
		depth, directionType, 64,
		graphgen.GeneralParameters{prefix, "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.CompleteNaryTreeParameters: ", err)
	}

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
	pathGenerator, err := (&graphgen.PathParameters{
		length, directed, 64,
		graphgen.GeneralParameters{prefix, "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.PathParameters: ", err)
	}

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
	unionGenerator, err := (&graphgen.UnionParameters{
		&graphgen.PathParameters{length, directed, 64,
			graphgen.GeneralParameters{"a", "", 0, 0}},
		&graphgen.PathParameters{length, directed, 64,
			graphgen.GeneralParameters{"b", "", length + 1, length}},
		graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.UnionParameters: ", err)
	}

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
	lpGenerator, err := (&graphgen.LexicographicalProductParameters{
		&graphgen.PathParameters{length, directed, 64,
			graphgen.GeneralParameters{"a", "", 0, 0}},
		&graphgen.PathParameters{length, directed, 64,
			graphgen.GeneralParameters{"b", "", length + 1, length}},
		graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.LexicographicalProductParameters or in graphgen.PathParameters: ", err)
	}

	count := 0
	for v := range lpGenerator.VertexChannel() {
		label := v.Label
		if strings.Contains(label, "__") {
			t.Fatalf("The label of the vertex number %d contains \"__\", label:%v, ",
				count, label)
		}
	}
}

func TestLexProdPathPathGenerator(t *testing.T) {
	var length uint64 = 2
	var directed bool = true
	lpGenerator, err := (&graphgen.LexicographicalProductParameters{
		&graphgen.PathParameters{length, directed, 64,
			graphgen.GeneralParameters{"a", "", 0, 0}},
		&graphgen.PathParameters{length, directed, 64,
			graphgen.GeneralParameters{"b", "", length + 1, length}},
		graphgen.GeneralParameters{"", "", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.LexicographicalProductParameters or in graphgen.PathParameters: ", err)
	}

	expectedNumberVertices := (length + 1) * (length + 1)
	expectedNumberEdges := (length+1)*length /*within super-vertices*/ +
		length*(length+1)*(length+1) /*between super-vertices*/

	testExpectedNumberVerticesEdges(t, lpGenerator, expectedNumberVertices,
		expectedNumberEdges)

}

func xTestReadJSONTree(t *testing.T) {
	const filename = "tree.json"
	/*
		{
			"tree": {"branchingDegree": 2, "depth": 3, "directionType": "downwards"}
		}
	*/
	buf, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Panicf("Could not read from file %s, error: %v", filename, err)
	}
	gg, err := graphgen.JSON2Graph(buf, "", true, true)
	if err != nil {
		t.Error("Error in JSON2Graph: ", err)
	}

	expectedNumberVertices := uint64(15) // branchingDegree: 2, depth: 3
	expectedNumberEdges := uint64(14)    // branchingDegree: 2, depth: 3
	testExpectedNumberVerticesEdges(t, gg, expectedNumberVertices,
		expectedNumberEdges)

}

func xTestReadJSONLexProdUnionTreePathTree(t *testing.T) {
	const filename = "lexProdUnionTreePathTree.json"
	/*
		{"lexProduct": [
			{"union": [
				{"tree": {"branchingDegree": 2, "depth": 1, "directionType": "downwards"}},
				{"path": {"length": 3, "directed": true}}
			]},
			{"tree": {"branchingDegree": 2, "depth": 1, "directionType": "upwards"}}
		]

		}
	*/

	buf, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Panicf("Could not read from file %s, error: %v", filename, err)
	}
	gg, errJSON2Graph := graphgen.JSON2Graph(buf, "", true, true)
	if errJSON2Graph != nil {
		t.Error("Error in JSON2Graph: ", err)
	}

	expectedNumberVertices := uint64(21) // branchingDegree: 2, depth: 3
	expectedNumberEdges := uint64(59)    // branchingDegree: 2, depth: 3

	testExpectedNumberVerticesEdges(t, gg, expectedNumberVertices,
		expectedNumberEdges)
}

func xTestReadJSONCycle(t *testing.T) {
	const filename = "cycle.json"
	/*
		{
			"cycle": {"length": 5}
		}
	*/

	buf, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Panicf("Could not read from file %s, error: %v", filename, err)
	}
	gg, errJSON2Graph := graphgen.JSON2Graph(buf, "", true, true)
	if errJSON2Graph != nil {
		t.Error("Error in JSON2Graph: ", err)
	}

	expectedNumberVertices := uint64(5) // branchingDegree: 2, depth: 3
	expectedNumberEdges := uint64(5)    // branchingDegree: 2, depth: 3

	testExpectedNumberVerticesEdges(t, gg, expectedNumberVertices,
		expectedNumberEdges)
}
