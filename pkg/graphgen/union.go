package graphgen

import (
	"sync"

	"github.com/arangodb/feed/pkg/datagen"
)

type UnionParameters struct {
	Left   Generatable
	Right  Generatable
	Prefix string
}

func (u UnionParameters) MakeGraphGenerator() GraphGenerator {
	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	p1 := u.Left.MakeGraphGenerator()
	p2 := u.Right.MakeGraphGenerator()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for v := range p1.VertexChannel() {
			V <- v
		}
		wg.Done()
	}()

	go func() {
		for v := range p2.VertexChannel() {
			V <- v
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(V)
	}()

	var wge sync.WaitGroup
	wge.Add(2)

	go func() {
		for e := range p1.EdgeChannel() {
			E <- e
		}
		wge.Done()
	}()

	go func() {
		for e := range p2.EdgeChannel() {
			E <- e
		}
		wge.Done()
	}()

	go func() {
		wge.Wait()
		close(E)
	}()

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: p1.NumberVertices() + p2.NumberVertices(),
		numberEdges:    p1.NumberEdges() + p2.NumberEdges()}
}
