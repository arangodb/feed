package graphgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
)

func unionString() string   { return "union" }
func lexProdString() string { return "lexProduct" }
func treeString() string    { return "tree" }
func pathString() string    { return "path" }
func cycleString() string   { return "cycle" }

func contains(string *string, strings []string) bool {
	return true
}

func checkSubtreeInput(jsonNodeName *string, subtree *interface{}) error {
	switch *jsonNodeName {
	case unionString(), lexProdString():
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

	case pathString(), treeString(), cycleString():
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

//func getLeftRight(subtree *interface)

func parseJSONtoGraph(f map[string]any, prefix string, numVertices uint64) (*Generatable, uint64, error) {
	if prefix != "" {
		prefix += "_"
	}
	for jsonNodeName, subtree := range f {
		err := checkSubtreeInput(&jsonNodeName, &subtree)
		if err != nil {
			return nil, 0, err
		}
		switch jsonNodeName {
		case "union":
			{
				left := subtree.([]interface{})[0]
				leftChild := left.(map[string]any)
				ggLeft, numVerticesLeft, err := parseJSONtoGraph(leftChild, prefix+"a", numVertices)
				if err != nil {
					return nil, 0,
						errors.New(fmt.Sprintf("Could not construct graph from %v",
							leftChild))
				}

				right := subtree.([]interface{})[1]
				rightChild := right.(map[string]any)
				ggRight, numVerticesRight, err := parseJSONtoGraph(rightChild, prefix+"b", numVertices+numVerticesLeft)
				if err != nil {
					return nil, 0,
						errors.New(fmt.Sprintf("Could not construct graph from %v",
							rightChild))
				}

				var u Generatable = UnionParameters{*ggLeft, *ggRight, prefix, numVertices}
				return &u, numVertices + numVerticesLeft + numVerticesRight, nil
			}
		case "lexProduct":
			{
				left := subtree.([]interface{})[0]
				leftChild := left.(map[string]any)
				ggLeft, numVerticesLeft, err := parseJSONtoGraph(leftChild, prefix+"a", numVertices)
				if err != nil {
					return nil, 0,
						errors.New(fmt.Sprintf("Could not construct graph from %v",
							leftChild))
				}

				right := subtree.([]interface{})[1]
				rightChild := right.(map[string]any)
				ggRight, numVerticesRight, err := parseJSONtoGraph(rightChild, prefix+"b", numVertices+numVerticesLeft)
				if err != nil {
					return nil, 0,
						errors.New(fmt.Sprintf("Could not construct graph from %v",
							rightChild))
				}
				var lp Generatable = LexicographicalProductParameters{*ggLeft, *ggRight, prefix, numVertices}
				return &lp, numVertices + numVerticesLeft + numVerticesRight, nil
			}

		case "path":
			{
				length := uint64(subtree.(map[string]interface{})["length"].(float64)) // unmarshalling can only give float, never int
				directed := subtree.(map[string]interface{})["directed"].(bool)
				var path Generatable = &PathParameters{length, directed, prefix, numVertices}
				return &path, length + 1, nil
			}
		case "tree":
			{
				branchingDegree := uint64(subtree.(map[string]interface{})["branchingDegree"].(float64))
				depth := uint64(subtree.(map[string]interface{})["depth"].(float64))
				directionType := subtree.(map[string]interface{})["directionType"].(string)
				if directionType != "downwards" &&
					directionType != "upwards" && directionType != "bidirected" {
					return nil, 0, errors.New(fmt.Sprintf(
						"Error: value of \"tree.directionType\" shold "+
							"be one of \"downwards\", \"upwards\", \"bidirected\", "+
							", but it is %v", directionType))
				}
				var tree Generatable = &CompleteNaryTreeParameters{
					branchingDegree, depth, directionType, prefix, numVertices}
				return &tree, tree.(*CompleteNaryTreeParameters).NumVertices(), nil

			}
		case "cycle":
			{
				length := uint64(subtree.(map[string]interface{})["length"].(float64))
				var cycle Generatable = &CycleGraphParameters{length, prefix}
				return &cycle, length, nil
			}
		default:
			return nil, 0, errors.New(fmt.Sprintf("Cannot parse %v", jsonNodeName))
		}

	}
	return nil, 0, errors.New("We should not end up here. Probably forgot to return from the previous switch.")
}

func JSON2Graph(jsonGraph []byte) (GraphGenerator, error) {
	var f map[string]any
	err := json.Unmarshal(jsonGraph, &f)
	if err != nil {
		log.Fatalf("Could not parse input graph: %v", jsonGraph)
	}
	generatable, _, err := parseJSONtoGraph(f, "", 0)
	if err != nil {
		log.Printf("Could not produce a graph generator from the given JSON, error: %v", err)
	}
	gg, err := (*generatable).MakeGraphGenerator()
	if err != nil {
		return nil, err
	}
	return gg, nil
}
