package graphgen

import (
	"fmt"
	"strconv"

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
	MakeGraphGenerator() GraphGenerator
}

type Graph struct {
	V              chan *datagen.Doc
	E              chan *datagen.Doc
	numberVertices uint64
	numberEdges    uint64
}

func (p *Graph) VertexChannel() chan *datagen.Doc {
	return p.V
}

func (p *Graph) EdgeChannel() chan *datagen.Doc {
	return p.E
}

func (p *Graph) NumberVertices() uint64 {
	return p.numberVertices
}

func (p *Graph) NumberEdges() uint64 {
	return p.numberEdges
}

// assure *Graph implements GraphGenerator
var _ GraphGenerator = (*Graph)(nil)

func batchSize() int64 {
	return 1000
}

func PrintGraph(gg *GraphGenerator) {
	fmt.Println("Vertices:")
	for v := range (*gg).VertexChannel() {
		fmt.Println(v)
	}
	fmt.Println("Edges:")
	for e := range (*gg).EdgeChannel() {
		fmt.Println(e)
	}
}

func PrintGraphProxy(gp *GraphGeneratorData) {
	fmt.Println("Vertices:")
	for v := range (*gp).VertexChannel() {
		fmt.Println(v)
	}
	fmt.Println("Edges:")
	for e := range (*gp).EdgeChannel() {
		fmt.Printf("(%v,%v)\n", e.FromLabel, e.ToLabel)
	}
}

// produce edge out of indexes and put it into the channel
func makeEdge(prefix string, edgeIndex uint64, fromIndex uint64, toIndex uint64,
	edgeChannel *chan *datagen.Doc) {
	var edge datagen.Doc
	edgeIdxAsString := strconv.Itoa(int(edgeIndex))
	edge.FromLabel = datagen.LabelFromIndex(prefix, fromIndex)
	edge.ToLabel = datagen.LabelFromIndex(prefix, toIndex)
	if prefix != "" {
		prefix += "_"
	}
	edge.Label = prefix + edgeIdxAsString
	edge.From = datagen.KeyFromIndex(int64(fromIndex))
	edge.To = datagen.KeyFromIndex(int64(toIndex))
	*edgeChannel <- &edge
}

// produce edge out of strings and put it into the channel
func makeEdgeString(prefix string, edgeIndex uint64, fromLabel *string,
	toLabel *string, edgeChannel *chan *datagen.Doc) {
	var edge datagen.Doc
	edgeIdxAsString := strconv.Itoa(int(edgeIndex))
	if prefix != "" {
		prefix += "_"
	}
	edge.Label = prefix + edgeIdxAsString
	edge.From = datagen.KeyFromLabel(*fromLabel)
	edge.To = datagen.KeyFromLabel(*toLabel)
	edge.FromLabel = prefix + *fromLabel
	edge.ToLabel = prefix + *toLabel
	*edgeChannel <- &edge
}
