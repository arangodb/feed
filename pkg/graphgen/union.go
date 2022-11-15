package graphgen

import (
	"sync"

	"github.com/arangodb/feed/pkg/datagen"
)

type UnionParameters struct {
	Left          Generatable
	Right         Generatable
	GeneralParams GeneralParameters
}

func (u *UnionParameters) MakeGraphGenerator(
	makeVertices bool, makeEdges bool) (GraphGenerator, error) {

	V := make(chan *datagen.Doc, BatchSize())
	E := make(chan *datagen.Doc, BatchSize())

	p1, errLeft := u.Left.MakeGraphGenerator(makeVertices, makeEdges)
	if errLeft != nil {
		return nil, errLeft
	}
	p2, errRight := u.Right.MakeGraphGenerator(makeVertices, makeEdges)
	if errRight != nil {
		return nil, errRight
	}

	if makeVertices {
		var wgv sync.WaitGroup
		wgv.Add(2)

		go func() {
			for v := range p1.VertexChannel() {
				V <- v
			}
			wgv.Done()
		}()

		go func() {
			for v := range p2.VertexChannel() {
				V <- v
			}
			wgv.Done()
		}()

		go func() {
			wgv.Wait()
			close(V)
		}()

	} else {
		close(V)
	}

	if makeEdges {
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
	} else {
		close(E)
	}

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: p1.NumberVertices() + p2.NumberVertices(),
		numberEdges:    p1.NumberEdges() + p2.NumberEdges()}, nil
}

var _ Generatable = &UnionParameters{}
