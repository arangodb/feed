package graphgen

import (
	"fmt"

	"github.com/arangodb/feed/pkg/datagen"
)

type GraphGenerator interface {
	VertexChannel() chan *datagen.Doc
	EdgeChannel() chan *datagen.Doc
	NumberVertices() uint64
	NumberEdges() uint64
}

type GraphGeneratorData struct {
	V              chan *datagen.Doc
	E              chan *datagen.Doc
	numberVertices uint64
	numberEdges    uint64
}

type GeneralParameters struct {
	Prefix             string
	StartIndexVertices uint64
	StartIndexEdges    uint64
}

// ensure that GraphGeneratorData implements GraphGenerator
var _ GraphGenerator = (*GraphGeneratorData)(nil)

func (p *GraphGeneratorData) VertexChannel() chan *datagen.Doc {
	return p.V
}

func (p *GraphGeneratorData) EdgeChannel() chan *datagen.Doc {
	return p.E
}

func (p *GraphGeneratorData) NumberVertices() uint64 {
	return p.numberVertices
}

func (p *GraphGeneratorData) NumberEdges() uint64 {
	return p.numberEdges
}

type Generatable interface {
	MakeGraphGenerator(makeVertices bool, makeEdges bool) (GraphGenerator, error)
}

func BatchSize() int64 {
	return 1000
}

func PrintGraph(gg *GraphGenerator) {
	fmt.Println("Vertices:")
	for v := range (*gg).VertexChannel() {
		fmt.Printf("Index: %d, Label: %s\n", v.Index, v.Label)
	}
	fmt.Println("Edges:")
	for e := range (*gg).EdgeChannel() {
		fmt.Printf("Index: %d, Label: %s, FromIndex: %d, ToIndex: %d, FromLabel: %s, ToLabel: %s\n",
			e.Index, e.Label, e.FromIndex, e.ToIndex, e.FromLabel, e.ToLabel)
	}
}

// produce edge out of indexes and put it into the channel
func makeEdge(
	prefix *string,
	edgeIndex uint64,
	edgeLabel *string,
	globalFromIndex uint64,
	globalToIndex uint64,
	fromLabel *string,
	toLabel *string,
	edgeChannel chan *datagen.Doc) {

	var edge datagen.Doc
	edge.Key = datagen.KeyFromIndex(edgeIndex)
	edge.Index = edgeIndex
	edge.FromIndex = globalFromIndex
	edge.ToIndex = globalToIndex
	edge.Label = *prefix + *edgeLabel
	edge.From = datagen.KeyFromIndex(globalFromIndex)
	edge.To = datagen.KeyFromIndex(globalToIndex)
	edge.FromLabel = *prefix + *fromLabel
	edge.ToLabel = *prefix + *toLabel
	edgeChannel <- &edge
}

func makeVertex(
	prefix *string,
	globalIndex uint64,
	label *string,
	vertexChannel chan *datagen.Doc) {

	var vertex datagen.Doc
	vertex.Key = datagen.KeyFromIndex(globalIndex)
	vertex.Index = globalIndex
	vertex.Label = *prefix + *label
	vertexChannel <- &vertex
}

// produce edge out of strings and put it into the channel
// func makeEdgeString(prefix string, edgeIndex uint64, fromLabel *string,
// 	toLabel *string, edgeChannel *chan *datagen.Doc) {
// 	var edge datagen.Doc
// 	edgeIdxAsString := strconv.Itoa(int(edgeIndex))
// 	if prefix != "" {
// 		prefix += "_"
// 	}
// 	edge.Label = prefix + edgeIdxAsString
// 	edge.From = datagen.KeyFromLabel(*fromLabel)
// 	edge.To = datagen.KeyFromLabel(*toLabel)
// 	edge.FromLabel = prefix + *fromLabel
// 	edge.ToLabel = prefix + *toLabel
// 	*edgeChannel <- &edge
// }
