package graphgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
)

func parseJSONtoGraph(f map[string]any, prefix string) (*Generatable, error) {
	for node, subtree := range f {
		switch node {
		case "union":
			{
				switch subtree.(type) {
				case []interface{}:
					{
						numChildren := len(subtree.([]interface{}))
						if numChildren != 2 {
							return nil,
								errors.New(fmt.Sprintf("Cannot parse the value of \"union\": "+
									"it should be a slice of length 2, "+
									"however, it has length %d", numChildren))
						}

						if prefix != "" {
							prefix += "_"
						}

						left := subtree.([]interface{})[0]
						leftChild := left.(map[string]any)
						proxyLeft, err := parseJSONtoGraph(leftChild, prefix+"a")
						if err != nil {
							return nil,
								errors.New(fmt.Sprintf("Could not construct graph from %v",
									leftChild))
						}

						right := subtree.([]interface{})[1]
						rightChild := right.(map[string]any)
						proxyRight, err := parseJSONtoGraph(rightChild, prefix+"b")
						if err != nil {
							return nil,
								errors.New(fmt.Sprintf("Could not construct graph from %v",
									rightChild))
						}

						var u Generatable = UnionParameters{*proxyLeft, *proxyRight, prefix}
						return &u, nil

					}
				default:
					return nil,
						errors.New(fmt.Sprintf("Error: Value of \"union\" is not \"[]interface{}\" but %v",
							reflect.TypeOf(subtree)))
				}
				// return nil, nil
			}
		case "lexProduct":
			{
				switch subtree.(type) {
				case []interface{}:
					{
						numChildren := len(subtree.([]interface{}))
						if numChildren != 2 {
							return nil,
								errors.New(fmt.Sprintf("Cannot parse the value of \"lexProduct\": "+
									"it should be a slice of length 2, "+
									"however, it has length %d", numChildren))
						}

						if prefix != "" {
							prefix += "_"
						}

						left := subtree.([]interface{})[0]
						leftChild := left.(map[string]any)
						proxyLeft, err := parseJSONtoGraph(leftChild, prefix+"a")
						if err != nil {
							return nil,
								errors.New(fmt.Sprintf("Could not construct graph from %v",
									leftChild))
						}

						right := subtree.([]interface{})[1]
						rightChild := right.(map[string]any)
						proxyRight, err := parseJSONtoGraph(rightChild, prefix+"b")
						if err != nil {
							return nil,
								errors.New(fmt.Sprintf("Could not construct graph from %v",
									rightChild))
						}
						var u Generatable = LexicographicalProductParameters{*proxyLeft, *proxyRight, prefix}
						return &u, nil

					}
				default:
					return nil,
						errors.New(fmt.Sprintf("Error: Value of \"union\" is not \"[]interface{}\" but %v",
							reflect.TypeOf(subtree)))
				}
				// return nil, nil
			}

		case "path":
			{
				switch subtree.(type) {
				case map[string]interface{}:
					length := uint64(subtree.(map[string]interface{})["length"].(float64)) // unmarshalling can only give float, never int
					directed := subtree.(map[string]interface{})["directed"].(bool)
					var path Generatable = &PathParameters{length, directed, prefix}
					return &path, nil
				default:
					return nil, errors.New(fmt.Sprintf("Error: value of \"path\" is not \"map[string]interface{}\" but %v", reflect.TypeOf(subtree)))
				}
			}
		case "tree":
			{
				switch subtree.(type) {
				case map[string]interface{}:
					branchingDegree := uint64(subtree.(map[string]interface{})["branchingDegree"].(float64))
					depth := uint64(subtree.(map[string]interface{})["depth"].(float64))
					directionType := subtree.(map[string]interface{})["directionType"].(string)
					if directionType != "downwards" &&
						directionType != "upwards" && directionType != "bidirected" {
						return nil, errors.New(fmt.Sprintf(
							"Error: value of \"tree.directionType\" shold "+
								"be one of \"downwards\", \"upwards\", \"bidirected\", "+
								", but it is %v", directionType))
					}
					var tree Generatable = &CompleteNaryTreeParameters{branchingDegree, depth, directionType, prefix}
					return &tree, nil
				default:
					return nil, errors.New(fmt.Sprintf("Error: value of \"tree\" is not \"map[string]interface{}\" but %v", reflect.TypeOf(subtree)))
				}
			}
		default:
			return nil, errors.New(fmt.Sprintf("Cannot parse %v", node))
		}

	}
	//
	return nil, errors.New("We should not end up here. Probably forgot to return from the previous switch.")
}

func JSON2Graph(jsonGraph []byte) (GraphGenerator, error) {
	var f map[string]any
	err := json.Unmarshal(jsonGraph, &f)
	if err != nil {
		log.Fatalf("Could not parse input graph: %v", jsonGraph)
	}
	generatable, err := parseJSONtoGraph(f, "")
	if err != nil {
		log.Printf("Could not produce a graph generator from the given JSON, error: %v", err)
	}
	gg, err := (*generatable).MakeGraphGenerator()
	if err != nil {
		return nil, err
	}
	return gg, nil
}
