package graphgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
)

const (
	unionString   = "union"
	lexProdString = "lexProduct"
	treeString    = "tree"
	pathString    = "path"
	cycleString   = "cycle"
)

func checkSubtreeInput(jsonNodeName *string, subtree *interface{}) error {
	switch *jsonNodeName {
	case unionString, lexProdString:
		{
			switch (*subtree).(type) {
			case []interface{}:
				{
				}
			default:
				{
					return errors.New(fmt.Sprintf("Error: Value of \"%s\" is not \"[]interface{}\" but %v",
						*jsonNodeName, reflect.TypeOf(*subtree)))
				}
			}
			numChildren := len((*subtree).([]interface{}))
			if numChildren != 2 { // we assume that union also takes exactly 2 parameters!
				return errors.New(fmt.Sprintf("Cannot parse the value of \"%s\": "+
					"it should be a slice of length 2, "+
					"however, it has length %d", *jsonNodeName, numChildren))
			}
			return nil
		}

	case pathString, treeString, cycleString:
		{
			switch (*subtree).(type) {
			case map[string]interface{}:
				{
				}
			default:
				{
					return errors.New(fmt.Sprintf(
						"Error: value of \"%s\" is not \"map[string]interface{}\" but %v",
						*jsonNodeName, reflect.TypeOf(*subtree)))
				}
			}
			return nil
		}
	default:
		{
			return errors.New(fmt.Sprintf("Unknown node in json: %s", *jsonNodeName))
		}
	}

}

type parseJSONtoGraphResult struct {
	gg          *Generatable
	numVertices uint64
	numEdges    uint64
	err         error
}

func errorResult(err *error) parseJSONtoGraphResult {
	return parseJSONtoGraphResult{nil, 0, 0, *err}
}

func parseJSONtoGraph(f map[string]any,
	prefix string,
	numVertices uint64,
	numEdges uint64) parseJSONtoGraphResult {

	if prefix != "" {
		prefix += "_"
	}
	for jsonNodeName, subtree := range f {
		err := checkSubtreeInput(&jsonNodeName, &subtree)
		if err != nil {
			return errorResult(&err)
		}
		switch jsonNodeName {
		case unionString:
			{
				left := subtree.([]interface{})[0]
				leftChild := left.(map[string]any)
				leftResult := parseJSONtoGraph(leftChild, prefix+"a",
					numVertices, numEdges)
				if leftResult.err != nil {
					err := errors.New(fmt.Sprintf("Could not construct graph from %v, error: %v",
						leftChild, leftResult.err))
					return errorResult(&err)
				}

				right := subtree.([]interface{})[1]
				rightChild := right.(map[string]any)
				rightResult := parseJSONtoGraph(rightChild, prefix+"b",
					numVertices+leftResult.numVertices, numEdges+leftResult.numEdges)
				if rightResult.err != nil {
					err := errors.New(fmt.Sprintf("Could not construct graph from %v, error: %v",
						rightChild, rightResult.err))
					return errorResult(&err)
				}

				var u Generatable = &UnionParameters{*leftResult.gg,
					*rightResult.gg, GeneralParameters{prefix, numVertices, numEdges}}
				return parseJSONtoGraphResult{
					&u,
					numVertices + leftResult.numVertices + rightResult.numVertices,
					numEdges + leftResult.numEdges + rightResult.numEdges,
					nil}
			}
		case lexProdString:
			{
				left := subtree.([]interface{})[0]
				leftChild := left.(map[string]any)
				leftResult := parseJSONtoGraph(leftChild, prefix+"a", numVertices, numEdges)
				if leftResult.err != nil {
					err :=
						errors.New(fmt.Sprintf("Could not construct graph from %v, error: %v",
							leftChild, leftResult.err))
					return errorResult(&err)
				}

				right := subtree.([]interface{})[1]
				rightChild := right.(map[string]any)
				rightResult := parseJSONtoGraph(rightChild, prefix+"b",
					numVertices+leftResult.numVertices,
					numEdges+leftResult.numEdges)
				if rightResult.err != nil {
					err :=
						errors.New(fmt.Sprintf("Could not construct graph from %v, error: %v",
							rightChild, rightResult.err))
					return errorResult(&err)
				}
				var lp Generatable = &LexicographicalProductParameters{
					*leftResult.gg, *rightResult.gg,
					GeneralParameters{prefix, numVertices, numEdges}}
				return parseJSONtoGraphResult{
					&lp,
					numVertices + leftResult.numVertices + rightResult.numVertices,
					numEdges + leftResult.numEdges + rightResult.numEdges,
					nil}
			}

		case pathString:
			{
				length := uint64(subtree.(map[string]interface{})["length"].(float64)) // unmarshalling can only give float, never int
				directed := subtree.(map[string]interface{})["directed"].(bool)
				var path Generatable = &PathParameters{length, directed,
					GeneralParameters{prefix, numVertices, numEdges}}
				return parseJSONtoGraphResult{&path,
					numVertices + length + 1, numEdges + length, nil}
			}
		case treeString:
			{
				branchingDegree := uint64(subtree.(map[string]interface{})["branchingDegree"].(float64))
				depth := uint64(subtree.(map[string]interface{})["depth"].(float64))
				directionType := subtree.(map[string]interface{})["directionType"].(string)
				if directionType != "downwards" &&
					directionType != "upwards" && directionType != "bidirected" {
					err := errors.New(fmt.Sprintf(
						"Error: value of \"tree.directionType\" shold "+
							"be one of \"downwards\", \"upwards\", \"bidirected\", "+
							", but it is %v", directionType))
					return errorResult(&err)
				}
				var tree Generatable = &CompleteNaryTreeParameters{
					branchingDegree, depth, directionType,
					GeneralParameters{prefix, numVertices, numEdges}}
				numTreeVertices := tree.(*CompleteNaryTreeParameters).NumVertices()
				return parseJSONtoGraphResult{&tree,
					numVertices + numTreeVertices,
					numEdges + numTreeVertices - 1,
					nil}

			}
		case cycleString:
			{
				length := uint64(subtree.(map[string]interface{})["length"].(float64))
				var cycle Generatable = &CycleGraphParameters{length,
					GeneralParameters{prefix, numVertices, numEdges}}
				return parseJSONtoGraphResult{&cycle, numVertices + length, numEdges + length, nil}
			}
		default:
			err := errors.New(fmt.Sprintf("Cannot parse %v", jsonNodeName))
			return errorResult(&err)
		}

	}
	err := errors.New("We should not end up here. Probably forgot to return from the previous switch.")
	return errorResult(&err)
}

func JSON2Graph(jsonGraph []byte, makeVertices bool, makeEdges bool) (GraphGenerator, error) {
	var f map[string]any
	err := json.Unmarshal(jsonGraph, &f)
	if err != nil {
		log.Fatalf("Could not parse input graph: %v", jsonGraph)
	}
	result := parseJSONtoGraph(f, "", 0, 0)
	if result.err != nil {
		log.Printf("Could not produce a graph generator from the given JSON, error: %v", err)
	}
	gg, err := (*result.gg).MakeGraphGenerator(makeVertices, makeEdges)
	if err != nil {
		return nil, err
	}
	return gg, nil
}
