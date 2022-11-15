package main

import (
	"testing"

	"github.com/fatih/color"

	"github.com/arangodb/feed/pkg/graphgen"
)

func TestPrintPath(t *testing.T) {
	const length uint64 = 3
	const directed bool = true
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix string = "pr"
	gg, err := (&graphgen.PathParameters{
		length, directed, graphgen.GeneralParameters{prefix, startV,
			startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.PathParameters: ", err)
	}
	color.Cyan("Directed path, length %d, startIndexVertices %d, startIndexEdges %d",
		length, startV, startE)
	graphgen.PrintGraph(&gg)
}

func TestPrintUndirectedPath(t *testing.T) {
	var length uint64 = 2
	var directed bool = false
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix string = "pr"
	gg, err := (&graphgen.PathParameters{
		length, directed, graphgen.GeneralParameters{prefix, startV,
			startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.PathParameters: ", err)
	}
	color.Cyan("Undirected path, length %d, startIndexVertices %d, startIndexEdges %d",
		length, startV, startE)
	graphgen.PrintGraph(&gg)
}

func TestPrintDownTree(t *testing.T) {
	const branchingDegree uint64 = 3
	const depth uint64 = 2
	const directionType string = "downwards"
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix string = "pr"
	gg, err := (&graphgen.CompleteNaryTreeParameters{
		branchingDegree, depth, directionType,
		graphgen.GeneralParameters{prefix, startV, startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.CompleteNaryTreeParameters: ", err)
	}
	color.Cyan("Downward tree, branch %d, depth %d, directionType %s, startIndexVertices %d, startIndexEdges %d",
		branchingDegree, depth, directionType, startV, startE)
	graphgen.PrintGraph(&gg)
}

func TestPrintDirectedCycle(t *testing.T) {
	const length uint64 = 3
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix string = "pr"
	gg, err := (&graphgen.CycleGraphParameters{
		length, graphgen.GeneralParameters{prefix, startV,
			startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.CycleGraphParameters: ", err)
	}
	color.Cyan("Directed cycle, length %d, startIndexVertices %d, startIndexEdges %d",
		length, startV, startE)
	graphgen.PrintGraph(&gg)
}

func TestPrintEdgeless(t *testing.T) {
	const size uint64 = 5
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix string = "pr"
	gg, err := (&graphgen.EdgelessGraphParameters{
		size, graphgen.GeneralParameters{prefix, startV,
			startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.EdgelessGraphParameters: ", err)
	}
	color.Cyan("Edgeless graph, size %d, startIndexVertices %d, startIndexEdges %d",
		size, startV, startE)
	graphgen.PrintGraph(&gg)
}

func TestPrintUnionPathTree(t *testing.T) {
	const length uint64 = 2
	const directed bool = true
	const branchingDegree uint64 = 2
	const depth uint64 = 1
	const directionType string = "upwards"
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix string = "pr"
	gg, err := (&graphgen.UnionParameters{
		&graphgen.PathParameters{length, directed, graphgen.GeneralParameters{
			"a", startV, startE}},
		&graphgen.CompleteNaryTreeParameters{branchingDegree, depth,
			directionType, graphgen.GeneralParameters{
				"b", startV + length + 1, startE + length}},
		graphgen.GeneralParameters{prefix, startV, startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.UnionParameters: ", err)
	}
	const formatString string = "Union of a path " +
		"(length %d, directed %t) and a " +
		"tree (branching %d, depth %d, type %s),\nprefix %s, " +
		"startIndexVertices %d, startIndexEdges %d"
	color.Cyan(formatString, length, directed, branchingDegree, depth,
		directionType, prefix, startV, startE)
	graphgen.PrintGraph(&gg)
}

func TestPrintLexprodPathTree(t *testing.T) {
	const length uint64 = 2
	const directed bool = true
	const branchingDegree uint64 = 2
	const depth uint64 = 1
	const directionType string = "upwards"
	const startV uint64 = 1
	const startE uint64 = 2
	const prefix = "pr"
	gg, err := (&graphgen.LexicographicalProductParameters{
		&graphgen.PathParameters{length, directed, graphgen.GeneralParameters{
			"a", startV, startE}},
		&graphgen.CompleteNaryTreeParameters{branchingDegree, depth,
			directionType, graphgen.GeneralParameters{
				"b", startV + length + 1, startE + length}},
		graphgen.GeneralParameters{prefix, startV, startE}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.UnionParameters: ", err)
	}
	const formatString string = "Lexicographical product of a path " +
		"(length %d, directed %t) and\na " +
		"tree (branching %d, depth %d, type %s),\nprefix %s, " +
		"startIndexVertices %d, startIndexEdges %d"
	color.Cyan(formatString, length, directed, branchingDegree, depth,
		directionType, prefix, startV, startE)
	graphgen.PrintGraph(&gg)
}
